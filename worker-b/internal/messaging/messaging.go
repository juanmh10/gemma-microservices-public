// Package messaging delivers bounded notifications and aggregate email.
package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"reflect"
	"regexp"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	contracts "github.com/juanmh10/gemma-microservices/schemas/messaging"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

const Version = "analysis.completed.v1"
const Recipient = "gym-sales-manager-demo"
const Channel = "test_sink"
const Template = "stakeholder-summary-v1"
const PublicationPrefix = "gs://your-gcp-project-id-results/publication"
const NotificationPrefix = "gs://your-gcp-project-id-results/notifications"
const Topic = "projects/your-gcp-project-id/topics/poc-gemma-analysis-completed"
const Subscription = "projects/your-gcp-project-id/subscriptions/poc-gemma-notify"
const MaxEventBytes = 8 << 10

var ErrInvalid = errors.New("invalid notification event")
var idPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Event struct {
	SchemaVersion    string `json:"schema_version"`
	EventID          string `json:"event_id"`
	AnalysisID       string `json:"analysis_id"`
	SourceRunID      string `json:"source_run_id"`
	CreatedAt        string `json:"created_at"`
	ProcessingStatus string `json:"processing_status"`
	QualityStatus    string `json:"quality_status"`
	RecipientID      string `json:"recipient_id"`
	Channel          string `json:"channel"`
	TemplateVersion  string `json:"template_version"`
	ReportURI        string `json:"report_uri"`
	ReportSHA256     string `json:"report_sha256"`
}

func EventID(id, hash string) string { return analysis.Digest([]byte(Version + ":" + id + ":" + hash)) }
func NewEvent(r analysis.Report, hash string) Event {
	return Event{Version, EventID(r.AnalysisID, hash), r.AnalysisID, r.Metrics.SourceRunID,
		r.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), "completed", r.Decision.Status,
		Recipient, Channel, Template, "gs://your-gcp-project-id-results/analysis/" + r.AnalysisID + "/report.json", hash}
}
func (e Event) Validate() error {
	if _, err := time.Parse(time.RFC3339Nano, e.CreatedAt); err != nil {
		return ErrInvalid
	}
	if !SchemaValid("event", analysis.JSON(e)) {
		return ErrInvalid
	}
	if e.SchemaVersion != Version || !idPattern.MatchString(e.AnalysisID) || !idPattern.MatchString(e.SourceRunID) || !hashPattern.MatchString(e.ReportSHA256) || e.EventID != EventID(e.AnalysisID, e.ReportSHA256) || e.ProcessingStatus != "completed" || e.RecipientID != Recipient || e.Channel != Channel || e.TemplateVersion != Template || e.ReportURI != "gs://your-gcp-project-id-results/analysis/"+e.AnalysisID+"/report.json" || (e.QualityStatus != "pass" && e.QualityStatus != "fail" && e.QualityStatus != "not_evaluated") {
		return ErrInvalid
	}
	return nil
}

const BatchNotificationVersion = "analysis.notification.v2"

func SchemaValid(name string, data []byte) bool {
	version := "v1"
	var envelope struct {
		SchemaVersion string `json:"schema_version"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return false
	}
	if name == "email-receipt" && envelope.SchemaVersion == EmailVersion {
		version = "v2"
	}
	if name == "notification" && envelope.SchemaVersion == BatchNotificationVersion {
		version = "v2"
	}
	raw, err := contracts.Files.ReadFile(name + "-" + version + ".json")
	if err != nil {
		return false
	}
	var s jsonschema.Schema
	if json.Unmarshal(raw, &s) != nil {
		return false
	}
	resolved, err := s.Resolve(nil)
	if err != nil {
		return false
	}
	var value any
	return json.Unmarshal(data, &value) == nil && resolved.Validate(value) == nil
}

type Loader func(context.Context, string) (analysis.Report, string, error)
type Publisher interface {
	Publish(context.Context, []byte) (string, error)
}
type Engine struct {
	Store                                 analysis.Store
	Load                                  Loader
	PublicationPrefix, NotificationPrefix string
	Email                                 *EmailDelivery
}

func PublicationBase(prefix string, e Event) string {
	return prefix + "/" + e.AnalysisID + "/" + e.EventID
}
func NotificationPath(prefix string, e Event) string {
	return prefix + "/" + e.AnalysisID + "/" + e.EventID + "/" + Recipient + "/" + Channel + "/" + Template + "/_DELIVERED.json"
}

// Immutable reconciles a create-only write whose response or acknowledgment was lost.
func Immutable(ctx context.Context, s analysis.Store, name string, v any) error {
	data := analysis.JSON(v)
	err := s.Create(ctx, name, data)
	if errors.Is(err, analysis.ErrExists) {
		old, e := s.Read(ctx, name, 32<<10)
		var a, b any
		if e != nil || json.Unmarshal(old, &a) != nil || json.Unmarshal(data, &b) != nil || !reflect.DeepEqual(a, b) {
			return ErrInvalid
		}
		return nil
	}
	return err
}

type Publication struct {
	SchemaVersion string `json:"schema_version"`
	Event         Event  `json:"event"`
	Status        string `json:"status"`
	MessageID     string `json:"message_id,omitempty"`
}

func (x *Engine) Publish(ctx context.Context, p Publisher, id string, replay bool) (result Event, retErr error) {
	ctx = telemetry.Bind(ctx, id, "")
	span := telemetry.Begin(ctx, "messaging", "publication")
	defer func() { span.End(retErr != nil) }()
	r, hash, err := x.Load(ctx, id)
	if err != nil {
		return Event{}, err
	}
	e := NewEvent(r, hash)
	ctx = telemetry.Bind(ctx, e.AnalysisID, e.EventID)
	if e.Validate() != nil {
		return e, ErrInvalid
	}
	base := PublicationBase(x.PublicationPrefix, e)
	raw, err := x.Store.Read(ctx, base+"/_PUBLISHED.json", 32<<10)
	if err == nil {
		var ack Publication
		if analysis.Decode(raw, &ack) != nil || ack.Event != e || ack.SchemaVersion != Version || ack.Status != "published" || ack.MessageID == "" {
			return e, ErrInvalid
		}
		if !replay {
			return e, nil
		}
	} else if !errors.Is(err, analysis.ErrMissing) {
		return e, err
	}
	intent := Publication{SchemaVersion: Version, Event: e, Status: "pending"}
	// A pre-existing pending intent requires explicit replay; never launch again implicitly.
	err = x.Store.Create(ctx, base+"/_INTENT.json", analysis.JSON(intent))
	if errors.Is(err, analysis.ErrExists) {
		if !replay {
			return e, errors.New("publication pending; explicit replay required")
		}
		if err = Immutable(ctx, x.Store, base+"/_INTENT.json", intent); err != nil {
			return e, err
		}
	} else if err != nil {
		return e, err
	}
	phase := telemetry.Begin(ctx, "messaging", "publish_rpc")
	message, err := p.Publish(ctx, analysis.JSON(e))
	phase.End(err != nil)
	if err != nil || message == "" {
		return e, errors.New("publication pending; reconcile with explicit replay")
	}
	ack := Publication{Version, e, "published", message}
	// Each transport acknowledgment is immutable. A canonical first acknowledgment
	// remains authoritative while explicit replays retain separate transport IDs.
	if err = Immutable(ctx, x.Store, base+"/acks/"+analysis.Digest([]byte(message))+".json", ack); err != nil {
		return e, err
	}
	err = x.Store.Create(ctx, base+"/_PUBLISHED.json", analysis.JSON(ack))
	if errors.Is(err, analysis.ErrExists) {
		raw, readErr := x.Store.Read(ctx, base+"/_PUBLISHED.json", 32<<10)
		var old Publication
		if readErr != nil || analysis.Decode(raw, &old) != nil || old.Event != e || old.Status != "published" || old.SchemaVersion != Version || old.MessageID == "" {
			return e, ErrInvalid
		}
		return e, nil
	}
	return e, err
}

type Notification struct {
	SchemaVersion   string   `json:"schema_version"`
	Event           Event    `json:"event"`
	Status          string   `json:"status"`
	Summary         string   `json:"summary"`
	QualityReasons  []string `json:"quality_reasons"`
	RecordsExpected int      `json:"records_expected"`
	RecordsValid    int      `json:"records_valid"`
	RecordsInvalid  int      `json:"records_invalid"`
	CorrectOutcomes *int     `json:"correct_outcomes"`
	CauseF1         *float64 `json:"cause_f1"`
	ReportPath      string   `json:"report_path"`
}

func Render(e Event, r analysis.Report) Notification {
	version := Version
	if r.SchemaVersion == analysis.BatchSchemaVersion {
		version = BatchNotificationVersion
	}
	return Notification{version, e, "delivered", fmt.Sprintf("Analysis %s completed: quality %s; %d/%d valid records.", e.AnalysisID, e.QualityStatus, r.Metrics.RecordsValid, r.Metrics.RecordsExpected), r.Decision.Reasons, r.Metrics.RecordsExpected, r.Metrics.RecordsValid, r.Metrics.RecordsInvalid, r.Metrics.CorrectOutcomes, r.Metrics.CauseF1, "/analyses/" + e.AnalysisID + "/report"}
}
func (x *Engine) Deliver(ctx context.Context, e Event) (retErr error) {
	ctx = telemetry.Bind(ctx, e.AnalysisID, e.EventID)
	span := telemetry.Begin(ctx, "messaging", "notification")
	defer func() { span.End(retErr != nil) }()
	if e.Validate() != nil {
		return ErrInvalid
	}
	r, hash, err := x.Load(ctx, e.AnalysisID)
	if err != nil {
		if errors.Is(err, analysis.ErrMissing) {
			return ErrInvalid
		}
		return err
	}
	if e != NewEvent(r, hash) {
		return ErrInvalid
	}
	if err := Immutable(ctx, x.Store, NotificationPath(x.NotificationPrefix, e), Render(e, r)); err != nil {
		return err
	}
	if x.Email != nil {
		return x.Email.Deliver(ctx, x.Store, x.NotificationPrefix, e, r)
	}
	return nil
}

// Status does not read BigQuery, follow caller URLs or invalidate analysis status.
func (x *Engine) Status(ctx context.Context, id, hash string) map[string]string {
	e := Event{AnalysisID: id, ReportSHA256: hash, EventID: EventID(id, hash)}
	out := map[string]string{"publication_status": "not_published", "notification_status": "pending"}
	base := PublicationBase(x.PublicationPrefix, e)
	raw, err := x.Store.Read(ctx, base+"/_PUBLISHED.json", 32<<10)
	if err == nil {
		var p Publication
		if analysis.Decode(raw, &p) != nil || p.Event.Validate() != nil || p.Event.AnalysisID != id || p.Event.ReportSHA256 != hash || p.Status != "published" || p.SchemaVersion != Version || p.MessageID == "" {
			out["publication_status"] = "unavailable"
		} else {
			out["publication_status"] = "published"
		}
	} else if !errors.Is(err, analysis.ErrMissing) {
		out["publication_status"] = "unavailable"
	} else {
		raw, err = x.Store.Read(ctx, base+"/_INTENT.json", 32<<10)
		if err == nil {
			var p Publication
			if analysis.Decode(raw, &p) != nil || p.Event.Validate() != nil || p.Event.AnalysisID != id || p.Event.ReportSHA256 != hash || p.Status != "pending" || p.SchemaVersion != Version {
				out["publication_status"] = "unavailable"
			} else {
				out["publication_status"] = "pending"
			}
		} else if !errors.Is(err, analysis.ErrMissing) {
			out["publication_status"] = "unavailable"
		}
	}
	raw, err = x.Store.Read(ctx, NotificationPath(x.NotificationPrefix, e), 32<<10)
	if err == nil {
		var n Notification
		if analysis.Decode(raw, &n) != nil || !SchemaValid("notification", raw) || n.Event.Validate() != nil || n.Event.AnalysisID != id || n.Event.ReportSHA256 != hash || n.Status != "delivered" || (n.SchemaVersion != Version && n.SchemaVersion != BatchNotificationVersion) {
			out["notification_status"] = "unavailable"
		} else {
			out["notification_status"] = "delivered"
		}
	} else if !errors.Is(err, analysis.ErrMissing) {
		out["notification_status"] = "unavailable"
	}
	if status := x.emailStatus(ctx, e); status != "" {
		out["email_status"] = status
	}
	return out
}
