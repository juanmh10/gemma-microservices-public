package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"io"
	"net/http"
	"net/mail"
	"regexp"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

const LegacyEmailVersion = "stakeholder-email-v1"
const EmailVersion = "stakeholder-email-v2"

var ErrEmailPermanent = errors.New("email provider rejected request")
var ErrEmailReview = errors.New("email requires manual reconciliation")

type EmailRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
}

type EmailSender interface {
	Send(context.Context, string, EmailRequest) (string, error)
}

// Resend never returns provider bodies, addresses, headers or transport errors.
// Pub/Sub owns retries; redirects and client-side retries are disabled.
type Resend struct {
	Key    string
	Client *http.Client
}

func NewResend(key string) (*Resend, error) {
	if strings.TrimSpace(key) == "" || strings.ContainsAny(key, "\r\n") {
		return nil, errors.New("invalid email credential configuration")
	}
	return &Resend{Key: key, Client: &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (s *Resend) Send(ctx context.Context, key string, payload EmailRequest) (providerID string, retErr error) {
	span := telemetry.Begin(ctx, "messaging", "email_rpc")
	defer func() { span.End(retErr != nil) }()
	data := analysis.JSON(payload)
	if len(data) > 20<<10 {
		return "", ErrEmailPermanent
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.resend.com/emails", bytes.NewReader(data))
	if err != nil {
		return "", errors.New("email request unavailable")
	}
	req.Header.Set("Authorization", "Bearer "+s.Key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := s.Client.Do(req)
	if err != nil {
		return "", errors.New("email transport unavailable")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<10)+1))
	if err != nil || len(raw) > 8<<10 {
		return "", errors.New("email response unavailable")
	}
	if resp.StatusCode == 409 {
		var conflict struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(raw, &conflict) == nil && conflict.Name == "concurrent_idempotent_requests" {
			return "", errors.New("email request in progress")
		}
		return "", ErrEmailReview
	}
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return "", errors.New("email provider temporarily unavailable")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", ErrEmailPermanent
	}
	var accepted struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &accepted) != nil || !providerIDPattern.MatchString(accepted.ID) {
		return "", errors.New("email acknowledgment unavailable")
	}
	return accepted.ID, nil
}

var providerIDPattern = regexp.MustCompile(`^[a-zA-Z0-9-]{1,128}$`)

type EmailReceipt struct {
	SchemaVersion string `json:"schema_version"`
	EventID       string `json:"event_id"`
	AnalysisID    string `json:"analysis_id"`
	PayloadSHA256 string `json:"payload_sha256"`
	StartedAt     string `json:"started_at"`
	Status        string `json:"status"`
	ProviderID    string `json:"provider_id,omitempty"`
}

func EmailBase(prefix string, e Event) string {
	return prefix + "/" + e.AnalysisID + "/" + e.EventID + "/" + Recipient + "/email/" + LegacyEmailVersion
}

type EmailDelivery struct {
	Sender   EmailSender
	From, To string
	Now      func() time.Time
}

func NewEmailDelivery(sender EmailSender, from, to, domain string) (*EmailDelivery, error) {
	for _, address := range []string{from, to} {
		parsed, err := mail.ParseAddress(address)
		if err != nil || parsed.Address != address || strings.ContainsAny(address, "\r\n") || len(address) > 254 {
			return nil, errors.New("invalid private email configuration")
		}
	}
	if domain == "" || !strings.EqualFold(from[strings.LastIndex(from, "@")+1:], domain) || sender == nil {
		return nil, errors.New("invalid private sender configuration")
	}
	return &EmailDelivery{Sender: sender, From: from, To: to, Now: time.Now}, nil
}

// The delivery ledger path stays stable across template versions, so older and
// newer binaries cannot claim independent sends for the same event.
func emailVersion(ctx context.Context, store analysis.Store, base string, e Event) (string, error) {
	version := ""
	for _, file := range []string{"_ACCEPTED.json", "_REJECTED.json", "_INTENT.json"} {
		raw, err := store.Read(ctx, base+"/"+file, 8<<10)
		if errors.Is(err, analysis.ErrMissing) {
			continue
		}
		if err != nil {
			return "", err
		}
		var receipt EmailReceipt
		if analysis.Decode(raw, &receipt) != nil || !SchemaValid("email-receipt", raw) || receipt.EventID != e.EventID || receipt.AnalysisID != e.AnalysisID {
			return "", ErrEmailReview
		}
		if version != "" && version != receipt.SchemaVersion {
			return "", ErrEmailReview
		}
		version = receipt.SchemaVersion
	}
	if version == "" {
		version = EmailVersion
	}
	return version, nil
}

func (d *EmailDelivery) payload(e Event, r analysis.Report, version string) EmailRequest {
	n := Render(e, r)
	text := fmt.Sprintf("%s\nInvalid records: %d.\n", n.Summary, n.RecordsInvalid)
	if n.CorrectOutcomes != nil {
		text += fmt.Sprintf("Correct outcomes: %d/%d.\n", *n.CorrectOutcomes, n.RecordsExpected)
	}
	if n.CauseF1 != nil {
		text += fmt.Sprintf("Cause micro F1: %.4f.\n", *n.CauseF1)
	}
	text += "Quality reasons: " + strings.Join(n.QualityReasons, ", ") + "\n"
	// Only aggregate metrics are sent. No conversation evidence or private API URL.
	if version != LegacyEmailVersion {
		text += "Reference: " + e.AnalysisID + ".\n"
	} else if r.SchemaVersion == analysis.BatchSchemaVersion {
		text += "Reference: " + e.AnalysisID + ". This POC uses a fresh synthetic batch.\n"
	} else {
		text += "Reference: " + e.AnalysisID + ". This POC uses a retained synthetic shard.\n"
	}
	subject := "Pipeline analysis: " + e.AnalysisID
	if version == LegacyEmailVersion {
		subject = "Pipeline POC: " + e.AnalysisID
	}
	return EmailRequest{d.From, []string{d.To}, subject, text}
}

func (d *EmailDelivery) Deliver(ctx context.Context, store analysis.Store, prefix string, e Event, r analysis.Report) (retErr error) {
	ctx = telemetry.Bind(ctx, e.AnalysisID, e.EventID)
	span := telemetry.Begin(ctx, "messaging", "email")
	defer func() { span.End(retErr != nil) }()
	base := EmailBase(prefix, e)
	version, err := emailVersion(ctx, store, base, e)
	if err != nil {
		return err
	}
	payload := d.payload(e, r, version)
	hash := analysis.Digest(analysis.JSON(payload))
	valid := func(v EmailReceipt, status string) bool {
		_, err := time.Parse(time.RFC3339Nano, v.StartedAt)
		return err == nil && SchemaValid("email-receipt", analysis.JSON(v)) && v.SchemaVersion == version && v.EventID == e.EventID && v.AnalysisID == e.AnalysisID && v.PayloadSHA256 == hash && v.Status == status && (status != "accepted" || providerIDPattern.MatchString(v.ProviderID))
	}
	for _, status := range []string{"accepted", "rejected"} {
		raw, err := store.Read(ctx, base+"/_"+strings.ToUpper(status)+".json", 8<<10)
		if err == nil {
			var receipt EmailReceipt
			if analysis.Decode(raw, &receipt) != nil || !valid(receipt, status) {
				return ErrEmailReview
			}
			if status == "rejected" {
				return ErrEmailPermanent
			}
			return nil
		}
		if !errors.Is(err, analysis.ErrMissing) {
			return err
		}
	}
	now := d.Now().UTC()
	intent := EmailReceipt{SchemaVersion: version, EventID: e.EventID, AnalysisID: e.AnalysisID, PayloadSHA256: hash, StartedAt: now.Format(time.RFC3339Nano), Status: "pending"}
	err = store.Create(ctx, base+"/_INTENT.json", analysis.JSON(intent))
	if errors.Is(err, analysis.ErrExists) {
		raw, readErr := store.Read(ctx, base+"/_INTENT.json", 8<<10)
		if readErr != nil {
			return readErr
		}
		if analysis.Decode(raw, &intent) != nil || !valid(intent, "pending") {
			return ErrEmailReview
		}
	} else if err != nil {
		return err
	}
	started, err := time.Parse(time.RFC3339Nano, intent.StartedAt)
	// Stay inside provider's 24-hour idempotency window even after clock variation.
	if err != nil || now.Before(started) || now.Sub(started) >= 23*time.Hour {
		return ErrEmailReview
	}
	keyPrefix := "pipeline-email/"
	if version == LegacyEmailVersion {
		keyPrefix = "poc-email/"
	}
	providerID, err := d.Sender.Send(ctx, keyPrefix+e.EventID+"/"+version, payload)
	if errors.Is(err, ErrEmailPermanent) {
		intent.Status = "rejected"
		if saveErr := Immutable(ctx, store, base+"/_REJECTED.json", intent); saveErr != nil {
			return saveErr
		}
		return ErrEmailPermanent
	}
	if err != nil {
		return err
	}
	if !providerIDPattern.MatchString(providerID) {
		return errors.New("email acknowledgment unavailable")
	}
	intent.Status, intent.ProviderID = "accepted", providerID
	return Immutable(ctx, store, base+"/_ACCEPTED.json", intent)
}

// API reads receipts without credentials or private address configuration.
func (x *Engine) emailStatus(ctx context.Context, e Event) string {
	for _, item := range []struct{ file, status string }{{"_ACCEPTED.json", "accepted"}, {"_REJECTED.json", "rejected"}, {"_INTENT.json", "pending"}} {
		raw, err := x.Store.Read(ctx, EmailBase(x.NotificationPrefix, e)+"/"+item.file, 8<<10)
		if errors.Is(err, analysis.ErrMissing) {
			continue
		}
		if err != nil {
			return "unavailable"
		}
		var receipt EmailReceipt
		if analysis.Decode(raw, &receipt) != nil || !SchemaValid("email-receipt", raw) || receipt.EventID != e.EventID || receipt.AnalysisID != e.AnalysisID || receipt.Status != item.status || !hashPattern.MatchString(receipt.PayloadSHA256) {
			return "unavailable"
		}
		if _, err := time.Parse(time.RFC3339Nano, receipt.StartedAt); err != nil {
			return "unavailable"
		}
		if item.status == "accepted" && !providerIDPattern.MatchString(receipt.ProviderID) {
			return "unavailable"
		}
		return item.status
	}
	return ""
}
