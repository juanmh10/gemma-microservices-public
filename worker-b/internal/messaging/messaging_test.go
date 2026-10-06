package messaging

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"google.golang.org/api/option"
	pubsub "google.golang.org/api/pubsub/v1"
)

type fakePublisher struct {
	calls    int
	fail     bool
	payloads [][]byte
}

func (p *fakePublisher) Publish(_ context.Context, b []byte) (string, error) {
	p.calls++
	p.payloads = append(p.payloads, append([]byte{}, b...))
	if p.fail {
		return "", errors.New("lost response")
	}
	return fmt.Sprint(100 + p.calls), nil
}
func fixture(t *testing.T) (*Engine, Event, analysis.Report) {
	t.Helper()
	root := t.TempDir()
	r := analysis.Report{AnalysisID: "test-analysis", CreatedAt: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), Metrics: analysis.Metrics{SourceRunID: "pilot-004", RecordsExpected: 300, RecordsValid: 297, RecordsInvalid: 3}, Decision: analysis.Decision{Status: "not_evaluated", Reasons: []string{"ground_truth_unavailable"}}}
	hash := strings.Repeat("a", 64)
	x := &Engine{Store: &analysis.Objects{}, PublicationPrefix: filepath.Join(root, "publication"), NotificationPrefix: filepath.Join(root, "notifications"), Load: func(_ context.Context, id string) (analysis.Report, string, error) {
		if id != r.AnalysisID {
			return analysis.Report{}, "", analysis.ErrMissing
		}
		return r, hash, nil
	}}
	return x, NewEvent(r, hash), r
}
func TestAmbiguousPublicationRequiresExplicitReplay(t *testing.T) {
	x, _, _ := fixture(t)
	ctx := context.Background()
	p := &fakePublisher{fail: true}
	if _, err := x.Publish(ctx, p, "test-analysis", false); err == nil {
		t.Fatal("lost response accepted")
	}
	if got := x.Status(ctx, "test-analysis", strings.Repeat("a", 64)); got["publication_status"] != "pending" {
		t.Fatal(got)
	}
	p.fail = false
	if _, err := x.Publish(ctx, p, "test-analysis", false); err == nil || p.calls != 1 {
		t.Fatal("implicit replay")
	}
	if _, err := x.Publish(ctx, p, "test-analysis", true); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.payloads[0], p.payloads[1]) {
		t.Fatal("event changed during recovery")
	}
	if _, err := x.Publish(ctx, p, "test-analysis", false); err != nil || p.calls != 2 {
		t.Fatal("completed event republished automatically", err)
	}
	if _, err := x.Publish(ctx, p, "test-analysis", true); err != nil || p.calls != 3 {
		t.Fatal("explicit replay failed", err)
	}
	if x.Status(ctx, "test-analysis", strings.Repeat("a", 64))["publication_status"] != "published" {
		t.Fatal("missing ack")
	}
}
func TestConcurrentAndInterruptedDeliveryDeduplicates(t *testing.T) {
	x, e, _ := fixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := x.Deliver(ctx, e); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	// A response lost after receipt commit safely retries the same sink operation.
	if err := x.Deliver(ctx, e); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Dir(NotificationPath(x.NotificationPrefix, e)) + "/*")
	if err != nil || len(matches) != 1 {
		t.Fatal("duplicate sink receipts", matches, err)
	}
	if x.Status(ctx, e.AnalysisID, e.ReportSHA256)["notification_status"] != "delivered" {
		t.Fatal("missing status")
	}
}
func TestForgedEventsAndConflictingReceiptRejected(t *testing.T) {
	x, e, _ := fixture(t)
	ctx := context.Background()
	for _, mutate := range []func(*Event){func(e *Event) { e.ReportURI = "https://example.com" }, func(e *Event) { e.RecipientID = "other" }, func(e *Event) { e.SourceRunID = "other-run" }, func(e *Event) { e.QualityStatus = "pass" }, func(e *Event) { e.CreatedAt = "bad" }, func(e *Event) {
		e.ReportSHA256 = strings.Repeat("b", 64)
		e.EventID = EventID(e.AnalysisID, e.ReportSHA256)
	}} {
		bad := e
		mutate(&bad)
		if !errors.Is(x.Deliver(ctx, bad), ErrInvalid) {
			t.Fatal("forged event accepted")
		}
	}
	if err := x.Store.Create(ctx, NotificationPath(x.NotificationPrefix, e), []byte(`{"status":"delivered"}`)); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(x.Deliver(ctx, e), ErrInvalid) {
		t.Fatal("conflicting receipt accepted")
	}
	if x.Status(ctx, e.AnalysisID, e.ReportSHA256)["notification_status"] != "unavailable" {
		t.Fatal("conflict misreported")
	}
}

type outageStore struct {
	analysis.Store
	fail bool
}

func (s *outageStore) Create(ctx context.Context, n string, b []byte) error {
	if s.fail {
		return errors.New("storage unavailable")
	}
	return s.Store.Create(ctx, n, b)
}
func pushBody(e Event, attempt int) []byte {
	env := Envelope{Subscription: Subscription, DeliveryAttempt: attempt}
	env.Message.MessageID = "12345"
	env.Message.PublishTime = "2026-10-02T12:00:00Z"
	env.Message.Data = base64.StdEncoding.EncodeToString(analysis.JSON(e))
	return analysis.JSON(env)
}
func call(p *Push, body []byte) int {
	w := httptest.NewRecorder()
	p.ServeHTTP(w, httptest.NewRequest("POST", "/events", strings.NewReader(string(body))))
	return w.Code
}
func TestPushOutageRecoveryAndDurableRejection(t *testing.T) {
	x, e, _ := fixture(t)
	store := &outageStore{Store: x.Store, fail: true}
	x.Store = store
	p := &Push{Engine: x}
	if call(p, pushBody(e, 1)) != 503 {
		t.Fatal("outage acknowledged")
	}
	store.fail = false
	if call(p, pushBody(e, 2)) != 204 || call(p, pushBody(e, 3)) != 204 {
		t.Fatal("recovery/duplicate failed")
	}
	bad := e
	bad.RecipientID = "other"
	store.fail = true
	if call(p, pushBody(bad, 1)) != 503 {
		t.Fatal("rejection acknowledged before durable write")
	}
	store.fail = false
	if call(p, pushBody(bad, 2)) != 204 || call(p, pushBody(bad, 3)) != 204 {
		t.Fatal("rejection redelivery failed")
	}
	raw, err := store.Read(context.Background(), x.NotificationPrefix+"/rejected/12345.json", 32<<10)
	if err != nil || !SchemaValid("rejection", raw) || strings.Contains(string(raw), "other") {
		t.Fatal("rejection privacy/contract", err)
	}

}
func TestPushBoundsAndWrongSubscription(t *testing.T) {
	x, e, _ := fixture(t)
	p := &Push{Engine: x}
	if call(p, []byte(strings.Repeat("a", 17<<10))) != 400 {
		t.Fatal("oversize envelope")
	}
	var env Envelope
	_ = json.Unmarshal(pushBody(e, 1), &env)
	env.Subscription = "other"
	if call(p, analysis.JSON(env)) != 400 {
		t.Fatal("wrong subscription")
	}
	env.Subscription = Subscription
	env.Message.Data = base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 9<<10)))
	if call(p, analysis.JSON(env)) != 204 {
		t.Fatal("oversize event not durably rejected")
	}
}
func TestMissingOrUnavailableArtifactsNeverDeliver(t *testing.T) {
	x, e, _ := fixture(t)
	ctx := context.Background()
	x.Load = func(context.Context, string) (analysis.Report, string, error) {
		return analysis.Report{}, "", analysis.ErrMissing
	}
	if !errors.Is(x.Deliver(ctx, e), ErrInvalid) {
		t.Fatal("missing completed analysis delivered")
	}
	x.Load = func(context.Context, string) (analysis.Report, string, error) {
		return analysis.Report{}, "", errors.New("temporary unavailable")
	}
	if call(&Push{Engine: x}, pushBody(e, 1)) != 503 {
		t.Fatal("artifact outage acknowledged")
	}
}
func TestQualityEligibilityAndContracts(t *testing.T) {
	x, e, r := fixture(t)
	ctx := context.Background()
	for _, q := range []string{"pass", "fail", "not_evaluated"} {
		r.Decision.Status = q
		e = NewEvent(r, e.ReportSHA256)
		if e.Validate() != nil || !SchemaValid("notification", analysis.JSON(Render(e, r))) || !SchemaValid("publication", analysis.JSON(Publication{Version, e, "pending", ""})) {
			t.Fatal("contract mismatch", q)
		}
	}
	if err := x.Deliver(ctx, NewEvent(analysis.Report{AnalysisID: "incomplete"}, e.ReportSHA256)); err == nil {
		t.Fatal("incomplete accepted")
	}
}
func TestRESTPublishAmbiguousResponseDoesNotRetry(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		if r.Method != "POST" || r.URL.Path != "/v1/"+Topic+":publish" {
			t.Error("unexpected request", r.URL.Path)
		}
		var req pubsub.PublishRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil || len(req.Messages) != 1 {
			t.Error("invalid publish request")
		}
		http.Error(w, "temporary unavailable", 500)
	}))
	defer server.Close()
	service, err := pubsub.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	_, e, _ := fixture(t)
	_, err = (&PubSub{service}).Publish(context.Background(), analysis.JSON(e))
	if err == nil || posts != 1 {
		t.Fatal("REST retries", posts, err)
	}
}
