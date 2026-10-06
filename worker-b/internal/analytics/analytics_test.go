package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

type memoryWarehouse struct {
	Calls  int
	Fail   bool
	Rows   map[string]string
	Filter Filter
}

func (w *memoryWarehouse) Index(_ context.Context, _ string, r analysis.Report, hash string) (string, error) {
	w.Calls++
	if w.Fail {
		return "", errors.New("outage")
	}
	if old := w.Rows[r.AnalysisID]; old != "" && old != hash {
		return "", ErrInvalid
	}
	w.Rows[r.AnalysisID] = hash
	return "stable_job", nil
}
func (w *memoryWarehouse) History(ctx context.Context, f Filter) ([]json.RawMessage, error) {
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 15*time.Second {
		return nil, errors.New("missing deadline")
	}
	w.Filter = f
	if w.Fail {
		return nil, errors.New("outage")
	}
	return []json.RawMessage{json.RawMessage(`{"analysis_id":"test-analysis"}`)}, nil
}
func fixture(t *testing.T) (*analysis.Objects, string, string, string) {
	t.Helper()
	base := t.TempDir()
	prefix := filepath.Join(base, "analysis")
	index := filepath.Join(base, "index")
	id := "test-analysis"
	store := &analysis.Objects{}
	hash := strings.Repeat("a", 64)
	source := "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/worker-a@sha256:" + hash
	req := analysis.Request{SchemaVersion: analysis.SchemaVersion, Mode: "analyze", AnalysisID: id, SourceRunID: "pilot-004", TaskIndex: 0, SourceImage: source, Manifest: analysis.ObjectRef{URI: "manifest", SHA256: hash}, Shard: analysis.ObjectRef{URI: "shard", SHA256: hash}, Marker: analysis.ObjectRef{URI: "marker", SHA256: hash}, Chunks: []analysis.ObjectRef{{URI: "chunk", SHA256: hash}}, Rules: analysis.QualityRules{Version: "quality-v1", MinValidRate: .99, MinOutcomeAccuracy: .98, MinCauseF1: .90}, Model: analysis.ModelID, ModelMode: "fake", Location: analysis.Location, PromptVersion: analysis.PromptVersion, OutputPrefix: prefix}
	m := analysis.Metrics{SchemaVersion: analysis.SchemaVersion, EvaluatorVersion: analysis.EvaluatorVersion, SourceRunID: "pilot-004", ManifestSHA256: hash, SourceImage: source, InputFingerprint: analysis.Fingerprint(req), RecordsExpected: 1, SourceRecords: 1, RecordsValid: 1, SchemaValidRate: 1, Evidence: []analysis.Evidence{}}
	// Produce non-null empty per-cause metrics through JSON to match the schema.
	raw := analysis.JSON(m)
	raw = []byte(strings.Replace(string(raw), `"per_cause": null`, `"per_cause": {}`, 1))
	if analysis.Decode(raw, &m) != nil {
		t.Fatal("metrics")
	}
	n := analysis.Narrative{Summary: "Synthetic local fixture.", Findings: []analysis.Finding{{Code: "coverage", Observation: "One record.", MetricRefs: []string{"records_expected"}}}, Recommendations: []string{"Inspect coverage."}, Limitations: []string{"Synthetic."}}
	request := analysis.JSON(req)
	r := analysis.Report{SchemaVersion: analysis.SchemaVersion, AnalysisID: id, CreatedAt: time.Now().UTC(), RequestSHA256: analysis.Digest(request), Metrics: m, Decision: analysis.Decide(m, req.Rules), Narrative: n, Model: analysis.ModelID, Location: analysis.Location, PromptVersion: analysis.PromptVersion, PromptSHA256: hash, ImageDigest: "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/analysis@sha256:" + hash, SourceSHA256: hash, SourceRevision: strings.Repeat("a", 40), ModelMode: "fake", Usage: analysis.Usage{Requests: 1, ModelVersion: "offline-fake"}}
	report := analysis.JSON(r)
	marker := analysis.Completion{SchemaVersion: analysis.SchemaVersion, AnalysisID: id, RequestSHA256: r.RequestSHA256, ReportSHA256: analysis.Digest(report), Status: "completed"}
	for name, data := range map[string][]byte{"request.json": request, "metrics.json": raw, "report.json": report, "_SUCCESS.json": analysis.JSON(marker)} {
		if err := store.Create(context.Background(), prefix+"/"+id+"/"+name, data); err != nil {
			t.Fatal(err)
		}
	}
	return store, prefix, index, id
}

func TestNotificationOutageDoesNotBlockReportOrAnalysis(t *testing.T) {
	s, p, index, id := fixture(t)
	api := &API{Store: s, Prefix: p, IndexPrefix: index, NotificationStatus: func(context.Context, string, string) map[string]string {
		return map[string]string{"publication_status": "unavailable", "notification_status": "unavailable"}
	}}
	for _, suffix := range []string{"", "/report"} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest("GET", "/analyses/"+id+suffix, nil))
		if response.Code != 200 {
			t.Fatal("notification outage blocked existing artifacts", response.Code)
		}
		if suffix == "" && !strings.Contains(response.Body.String(), `"notification_status":"unavailable"`) {
			t.Fatal("notification outage hidden")
		}
	}
}
func TestProjectionAndSchemaParity(t *testing.T) {
	s, p, _, id := fixture(t)
	r, hash, err := Load(context.Background(), s, p, id)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := "private-source-excerpt-sentinel"
	r.Metrics.Evidence = []analysis.Evidence{{RecordID: "conv_000001", CauseCode: "price", TurnID: 1, Text: sentinel}}
	row, report := Project(r, hash)
	a, _ := json.Marshal(row)
	b, _ := json.Marshal(report)
	if strings.Contains(string(a)+string(b), sentinel) || strings.Contains(string(a)+string(b), "evidence") || row.CorrectOutcomes != nil || row.OutcomeAccuracy != nil {
		t.Fatal("evidence leak or fabricated scores")
	}
	for name, fields := range map[string][]Field{"analysis_runs": RunFields, "analysis_reports": ReportFields} {
		raw, err := os.ReadFile("../../../schemas/analytics/" + name + "-v1.json")
		if err != nil {
			t.Fatal(err)
		}
		var got []Field
		if json.Unmarshal(raw, &got) != nil || !reflect.DeepEqual(got, fields) {
			t.Fatal("schema order drift", name)
		}
	}
}
func TestCorruptionPreventsIndex(t *testing.T) {
	s, p, index, id := fixture(t)
	if err := os.WriteFile(p+"/"+id+"/report.json", []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	w := &memoryWarehouse{Rows: map[string]string{}}
	if _, err := Index(context.Background(), s, w, p, index, id, "initial"); err == nil || w.Calls != 0 {
		t.Fatal("corrupt report reached warehouse")
	}
}
func TestOutageRecoveryAndReindex(t *testing.T) {
	s, p, index, id := fixture(t)
	w := &memoryWarehouse{Rows: map[string]string{}, Fail: true}
	if _, err := Index(context.Background(), s, w, p, index, id, "initial"); err == nil {
		t.Fatal("outage hidden")
	}
	_, hash, err := Load(context.Background(), s, p, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Read(context.Background(), receiptPath(index, id, hash), 1024); !errors.Is(err, analysis.ErrMissing) {
		t.Fatal("false success")
	}
	w.Fail = false
	for range 2 {
		if _, err := Index(context.Background(), s, w, p, index, id, "initial"); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.Rows) != 1 {
		t.Fatal("duplicate rows")
	}
}
func TestHealthNeedsNoStorageOrWarehouse(t *testing.T) {
	api := &API{}
	for _, route := range []string{"/health", "/healthz"} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest("GET", route, nil))
		if response.Code != 200 || !strings.Contains(response.Body.String(), `"status":"ok"`) {
			t.Fatal("health must succeed without data clients")
		}
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest("POST", "/health", nil))
	if response.Code != 405 {
		t.Fatal("health must remain read-only")
	}
}

func TestAPIContractsAndFailureIsolation(t *testing.T) {
	s, p, index, id := fixture(t)
	w := &memoryWarehouse{Rows: map[string]string{}}
	api := &API{Store: s, Warehouse: w, Prefix: p, IndexPrefix: index}
	for uri, want := range map[string]int{"/analyses/" + id: 200, "/analyses/" + id + "/report": 200, "/analyses/unknown": 404, "/analyses?limit=101": 400, "/analyses?from=2026-01-01&to=2028-01-01": 400, "/analyses?source_run_id=a%27%3BSELECT": 400, "/analyses?limit=1&limit=2": 400, "/analyses?sql=SELECT": 400, "/analyses/../secret": 404} {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest("GET", uri, nil))
		if rec.Code != want {
			t.Fatal(uri, rec.Code, rec.Body.String())
		}
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest("GET", "/analyses?limit=1", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"next_cursor":"test-analysis"`) {
		t.Fatal(rec.Body.String())
	}
	w.Fail = true
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest("GET", "/analyses", nil))
	if rec.Code != 503 {
		t.Fatal("outage hidden")
	}
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest("GET", "/analyses/"+id+"/report", nil))
	if rec.Code != 200 {
		t.Fatal("BigQuery failure blocked GCS report")
	}
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest("POST", "/analyses", nil))
	if rec.Code != 405 {
		t.Fatal("submission enabled")
	}
}
func TestRESTAmbiguousSubmissionIsReconciledOnce(t *testing.T) {
	s, p, _, id := fixture(t)
	r, hash, err := Load(context.Background(), s, p, id)
	if err != nil {
		t.Fatal(err)
	}
	var saved bq.Job
	posts, gets := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if req.Method == "POST" {
			posts++
			if json.NewDecoder(req.Body).Decode(&saved) != nil {
				t.Fatal("decode")
			}
			if saved.Configuration.Query.MaximumBytesBilled != MaxBytesBilled || saved.JobReference.Location != Region || !strings.Contains(saved.Configuration.Query.Query, "BEGIN TRANSACTION") || strings.Count(saved.Configuration.Query.Query, "MERGE ") != 2 {
				t.Fatal("unbounded or non-atomic query")
			}
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"code":500,"message":"ambiguous"}}`))
			return
		}
		gets++
		saved.Status = &bq.JobStatus{State: "DONE"}
		_ = json.NewEncoder(w).Encode(saved)
	}))
	defer server.Close()
	svc, err := bq.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication(), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	warehouse := &BigQuery{Service: svc}
	if _, err := warehouse.Index(context.Background(), "initial", r, hash); err != nil {
		t.Fatal(err)
	}
	if posts != 1 || gets != 1 {
		t.Fatal("submission retried", posts, gets)
	}
}

func TestPersistedMetricForgeryRejected(t *testing.T) {
	s, p, _, id := fixture(t)
	raw, err := s.Read(context.Background(), p+"/"+id+"/report.json", analysis.MaxObjectBytes)
	if err != nil {
		t.Fatal(err)
	}
	var r analysis.Report
	if analysis.Decode(raw, &r) != nil {
		t.Fatal("decode")
	}
	r.Metrics.SchemaValidRate = .5
	report := analysis.JSON(r)
	var c analysis.Completion
	marker, err := s.Read(context.Background(), p+"/"+id+"/_SUCCESS.json", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if analysis.Decode(marker, &c) != nil {
		t.Fatal("marker")
	}
	c.ReportSHA256 = analysis.Digest(report)
	for name, data := range map[string][]byte{"report.json": report, "metrics.json": analysis.JSON(r.Metrics), "_SUCCESS.json": analysis.JSON(c)} {
		if os.WriteFile(p+"/"+id+"/"+name, data, 0600) != nil {
			t.Fatal("write")
		}
	}
	if _, _, err := Load(context.Background(), s, p, id); err == nil {
		t.Fatal("forged metrics accepted")
	}
}

func TestRemoteStorageTransportInitializes(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if !strings.Contains(r.URL.Path, "your-gcp-project-id-results") {
			t.Fatal("unexpected bucket")
		}
		_, _ = w.Write([]byte("bounded-artifact"))
	}))
	defer server.Close()
	t.Setenv("STORAGE_EMULATOR_HOST", server.URL)
	t.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", analysis.Project)
	store := &analysis.Objects{Remote: true}
	defer store.Close()
	raw, err := store.Read(context.Background(), "gs://your-gcp-project-id-results/analysis/test/report.json", 1024)
	if err != nil || string(raw) != "bounded-artifact" || calls != 1 {
		t.Fatal("storage transport initialization/read failed", err, calls)
	}
}

func TestHistorySendsEmptyFiltersAsStrings(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" {
			posts++
			var raw struct {
				Configuration struct {
					Query struct {
						QueryParameters []struct {
							Name           string         `json:"name"`
							ParameterValue map[string]any `json:"parameterValue"`
						} `json:"queryParameters"`
						MaximumBytesBilled string `json:"maximumBytesBilled"`
					} `json:"query"`
				} `json:"configuration"`
			}
			if json.NewDecoder(r.Body).Decode(&raw) != nil {
				t.Fatal("decode")
			}
			if raw.Configuration.Query.MaximumBytesBilled != "104857600" {
				t.Fatal("budget absent")
			}
			for _, p := range raw.Configuration.Query.QueryParameters {
				if p.Name == "source" || p.Name == "quality" || p.Name == "cursor" {
					v, ok := p.ParameterValue["value"]
					if !ok || v != "" {
						t.Fatal("empty filter became NULL", p.Name)
					}
				}
			}
			_, _ = w.Write([]byte(`{"status":{"state":"DONE"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"jobComplete":true,"rows":[{"f":[{"v":"{\"analysis_id\":\"test-analysis\"}"}]}]}`))
	}))
	defer server.Close()
	svc, err := bq.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication(), option.WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	w := &BigQuery{Service: svc}
	rows, err := w.History(context.Background(), Filter{From: "2026-10-01", To: "2026-10-03", Limit: 100})
	if err != nil || len(rows) != 1 || posts != 1 {
		t.Fatal("history failed", err, len(rows), posts)
	}
}
