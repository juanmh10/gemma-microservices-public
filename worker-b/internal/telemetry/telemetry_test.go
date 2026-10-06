package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/jsonschema-go/jsonschema"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

func events(t *testing.T, b *bytes.Buffer) []Event {
	t.Helper()
	raw, err := os.ReadFile("../../../schemas/observability/observation-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract jsonschema.Schema
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	resolved, err := contract.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []Event
	d := json.NewDecoder(bytes.NewReader(b.Bytes()))
	for {
		var e Event
		if err := d.Decode(&e); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if e.Version != Version || e.ElapsedUS < 0 || e.CompletedAt.Before(e.StartedAt) {
			t.Fatal("invalid timing contract")
		}
		encoded, _ := json.Marshal(e)
		var value any
		if err := json.Unmarshal(encoded, &value); err != nil {
			t.Fatal(err)
		}
		if err := resolved.Validate(value); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func TestBoundedConcurrentLogsAndSafeFields(t *testing.T) {
	var b bytes.Buffer
	ctx := New(&b).Context(context.Background(), "payload\nPRIVATE_MARKER")
	ctx = Mode(Bind(ctx, "bad/id", "PRIVATE_MARKER"), "PRIVATE_MARKER")
	var wg sync.WaitGroup
	for range MaxEvents + 20 {
		wg.Go(func() { s := Begin(ctx, "analysis", "run"); s.End(true); s.End(false) })
	}
	wg.Wait()
	got := events(t, &b)
	if len(got) != MaxEvents || !got[len(got)-1].EventLimitReached {
		t.Fatal("unbounded or unmarked logging")
	}
	for i, e := range got {
		if e.Sequence != i+1 || e.Status != "error" || e.AnalysisID != "" || e.EventID != "" || e.ModelMode != "" {
			t.Fatal("unsafe correlation or duplicated span")
		}
	}
	if strings.Contains(b.String(), "PRIVATE_MARKER") {
		t.Fatal("private field logged")
	}
	Begin(ctx, "PRIVATE_MARKER", "run").End(false)
	if len(events(t, &b)) != MaxEvents {
		t.Fatal("untrusted component logged")
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("private sink failure") }

func TestHTTPFailureTimingNeverLogsRequestOrChangesResponse(t *testing.T) {
	var b bytes.Buffer
	l := New(&b)
	h := HTTP(l, "api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Value(key{}) == nil {
			t.Fatal("missing request trace")
		}
		http.Error(w, "private response detail", 503)
	}))
	r := httptest.NewRequest("GET", "/analyses/test-analysis?private=PRIVATE_MARKER", strings.NewReader("PRIVATE_MARKER"))
	r.Header.Set("Authorization", "PRIVATE_MARKER")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	got := events(t, &b)
	if w.Code != 503 || len(got) != 1 || got[0].Status != "error" || got[0].AnalysisID != "test-analysis" {
		t.Fatal("request semantics or correlation changed")
	}
	if strings.Contains(b.String(), "PRIVATE_MARKER") || strings.Contains(b.String(), "private response detail") {
		t.Fatal("payload/header/response logged")
	}
	w = httptest.NewRecorder()
	HTTP(New(brokenWriter{}), "api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })).ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatal("logging sink failure changed the response")
	}
}

func TestDisabledObservationAndExpectedStorageState(t *testing.T) {
	Begin(context.Background(), "analysis", "run").End(true)
	var b bytes.Buffer
	ctx := New(&b).Context(context.Background(), "test-analysis")
	s := Begin(ctx, "storage", "read")
	s.Expected("missing")
	s.End(true)
	if got := events(t, &b); len(got) != 1 || got[0].Status != "missing" {
		t.Fatal("expected absence treated as outage")
	}
}

func TestBrokerAndLifecycleClocksRemainSeparateFromLocalElapsed(t *testing.T) {
	var b bytes.Buffer
	ctx := New(&b).Context(context.Background(), "test-analysis")
	ctx = Bind(ctx, "test-analysis", strings.Repeat("a", 64))
	ctx = Published(ctx, "2026-10-03T01:00:00Z")
	Begin(ctx, "messaging", "notification").End(false)
	Lifecycle(ctx, "analysis", "projects/your-gcp-project-id/locations/us-central1/jobs/poc-gemma-analysis/executions/poc-gemma-analysis-test", "2026-10-03T01:00:00Z", "2026-10-03T01:00:10Z", "2026-10-03T01:04:00Z", false)
	got := events(t, &b)
	if len(got) != 2 || got[0].SourcePublishedAt == nil || got[0].EventID != strings.Repeat("a", 64) {
		t.Fatal("broker correlation lost")
	}
	e := got[1]
	if e.RemoteCreatedAt == nil || e.RemoteCompletedAt == nil || e.ExecutionID != "poc-gemma-analysis-test" || e.JobStage != "analysis" {
		t.Fatal("lifecycle metadata lost")
	}
	if e.RemoteCompletedAt.Sub(*e.RemoteCreatedAt).Seconds() != 240 || e.ElapsedUS >= 240000000 {
		t.Fatal("remote lifecycle was mislabeled local processing")
	}
}
