package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

func fixture(t *testing.T) Request {
	t.Helper()
	dir := t.TempDir()
	ref := func(name string, data []byte) ObjectRef {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		return ObjectRef{URI: p, SHA256: Digest(data)}
	}
	encoder, _ := zstd.NewWriter(nil)
	shard := ref("shard.zst", encoder.EncodeAll([]byte("{\"record_id\":\"conv_000001\",\"messages\":[{\"turn_id\":1,\"role\":\"customer\",\"text\":\"Synthetic\"}]}\n{\"record_id\":\"conv_000002\",\"messages\":[{\"turn_id\":1,\"role\":\"customer\",\"text\":\"Synthetic\"}]}\n"), nil))
	encoder.Close()
	mf := ref("manifest.json", JSON(map[string]any{"run_id": "pilot-004", "prompt_version": "worker-a-v2", "records_total": 2, "shards": []any{map[string]any{"index": 0, "records": 2, "sha256": shard.SHA256}}}))
	image := "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/worker-a@sha256:" + Digest([]byte("fixture"))
	marker := ref("_SUCCESS.json", JSON(benchmark.Marker{RunID: "pilot-004", GPUProfile: "rtx6000", Status: "success", ManifestSHA256: mf.SHA256, RecordsTotal: 2, RecordsProcessed: 2, ModelRevision: "fixture", Quantization: "fixture", VLLMVersion: "fixture", PromptVersion: "worker-a-v2", PromptSHA256: Digest([]byte("prompt")), ImageDigest: image}))
	chunkPath := filepath.Join(dir, "chunk.parquet")
	err := parquet.WriteFile(chunkPath, []benchmark.PredictionRow{{ConversationID: "conv_000001", Status: "ok", Outcome: "success", Causes: []benchmark.Cause{{Code: "price_gap", Confidence: .9, EvidenceTurns: []int32{1}}}}, {ConversationID: "conv_000002", Status: "invalid_output"}})
	if err != nil {
		t.Fatal(err)
	}
	chunkData, _ := os.ReadFile(chunkPath)
	chunk := ObjectRef{URI: chunkPath, SHA256: Digest(chunkData)}
	truthPath := filepath.Join(dir, "labels.parquet")
	err = parquet.WriteFile(truthPath, []benchmark.TruthRow{{RecordID: "conv_000001", Outcome: "success", MetadataJSON: `{"hidden_objection_ids":["price_gap"]}`}, {RecordID: "conv_000002", Outcome: "failure", MetadataJSON: `{"hidden_objection_ids":["price_gap"]}`}})
	if err != nil {
		t.Fatal(err)
	}
	truthData, _ := os.ReadFile(truthPath)
	truth := ObjectRef{URI: truthPath, SHA256: Digest(truthData)}
	return Request{SchemaVersion: SchemaVersion, Mode: "analyze", ModelMode: "fake", AnalysisID: "test-analysis", SourceRunID: "pilot-004", SourceImage: image, Manifest: mf, Shard: shard, Marker: marker, Chunks: []ObjectRef{chunk}, Truth: &truth, Rules: QualityRules{Version: "quality-v1", MinValidRate: .99, MinOutcomeAccuracy: .98, MinCauseF1: .90}, Model: ModelID, Location: Location, PromptVersion: PromptVersion, OutputPrefix: filepath.Join(dir, "outputs")}
}

type narrator struct {
	calls int
	err   error
}

func (n *narrator) Generate(context.Context, Metrics, Decision) (Narrative, Usage, error) {
	n.calls++
	return Narrative{Summary: "Test narrative", Findings: []Finding{{Code: "coverage", Observation: "Test coverage", MetricRefs: []string{"records_invalid"}}}, Recommendations: []string{}, Limitations: []string{"Synthetic test"}}, Usage{Requests: 1, InputTokens: 10, OutputTokens: 10}, n.err
}

func TestInvalidPenaltyAndNoLabels(t *testing.T) {
	r := fixture(t)
	store := &Objects{}
	m, err := Evaluate(context.Background(), store, r)
	if err != nil {
		t.Fatal(err)
	}
	if m.RecordsValid != 1 || m.RecordsInvalid != 1 || *m.OutcomeAccuracy != .5 || *m.CauseRecall != .5 {
		t.Fatalf("invalid predictions were not penalized: %+v", m)
	}
	if Decide(m, r.Rules).Status != "fail" {
		t.Fatal("quality rules ignored")
	}
	r.Truth = nil
	m, err = Evaluate(context.Background(), store, r)
	if err != nil {
		t.Fatal(err)
	}
	if m.CauseF1 != nil || m.CorrectOutcomes != nil || len(m.PerCause) != 0 || Decide(m, r.Rules).Status != "not_evaluated" {
		t.Fatal("fabricated supervised scores")
	}
}
func TestIdempotentCompletionAndConflict(t *testing.T) {
	r := fixture(t)
	n := &narrator{}
	store := &Objects{}
	ctx := context.Background()
	if _, err := Run(ctx, store, r, n, Config{}); err != nil {
		t.Fatal(err)
	}
	if status, err := Run(ctx, store, r, n, Config{}); err != nil || status != "existing" || n.calls != 1 {
		t.Fatalf("duplicate invocation: %s %v %d", status, err, n.calls)
	}
	r.Truth = nil
	if _, err := Run(ctx, store, r, n, Config{}); err == nil || n.calls != 1 {
		t.Fatal("conflicting request accepted")
	}
}
func TestModelFailurePreservesMetricsAndClaim(t *testing.T) {
	r := fixture(t)
	n := &narrator{err: context.DeadlineExceeded}
	store := &Objects{}
	ctx := context.Background()
	if _, err := Run(ctx, store, r, n, Config{}); err == nil {
		t.Fatal("model failure ignored")
	}
	data, err := store.Read(ctx, objectPath(r, "metrics.json"), MaxObjectBytes)
	if err != nil {
		t.Fatal("committed metrics lost")
	}
	if _, err = Run(ctx, store, r, n, Config{}); err == nil || n.calls != 1 {
		t.Fatal("failed claim retried model")
	}
	previous := r
	r.AnalysisID = "replay-analysis"
	r.Metrics = &ObjectRef{URI: objectPath(previous, "metrics.json"), SHA256: Digest(data)}
	n.err = nil
	if _, err = Run(ctx, store, r, n, Config{}); err != nil {
		t.Fatal(err)
	}
	if n.calls != 2 {
		t.Fatal("explicit metrics replay did not complete")
	}
}
func TestCorruptionPreventsModelCall(t *testing.T) {
	r := fixture(t)
	n := &narrator{}
	r.Chunks[0].SHA256 = Digest([]byte("wrong"))
	if _, err := Run(context.Background(), &Objects{}, r, n, Config{}); err == nil || n.calls != 0 {
		t.Fatal("corrupt source reached model")
	}
}
func TestReplayRejectsForgedCoverage(t *testing.T) {
	r := fixture(t)
	m, err := Evaluate(context.Background(), &Objects{}, r)
	if err != nil {
		t.Fatal(err)
	}
	m.RecordsInvalid = 0
	payload := JSON(m)
	p := filepath.Join(t.TempDir(), "metrics.json")
	_ = os.WriteFile(p, payload, 0600)
	r.Metrics = &ObjectRef{URI: p, SHA256: Digest(payload)}
	if _, err = Evaluate(context.Background(), &Objects{}, r); err == nil {
		t.Fatal("forged coverage accepted")
	}
}
func TestUnknownNarrativeReferenceAndRequestFields(t *testing.T) {
	n := Narrative{Summary: "Test", Findings: []Finding{{Code: "fake", Observation: "fake", MetricRefs: []string{"invented"}}}, Limitations: []string{"Test"}}
	if n.Validate(Metrics{}) == nil {
		t.Fatal("unknown metric reference accepted")
	}
	var r Request
	if Decode([]byte(`{"unexpected":true}`), &r) == nil {
		t.Fatal("unknown request field accepted")
	}
}
func TestAtomicLocalClaim(t *testing.T) {
	store := &Objects{}
	p := filepath.Join(t.TempDir(), "claim.json")
	ctx := context.Background()
	result := make(chan error, 2)
	for range 2 {
		go func() { result <- store.Create(ctx, p, []byte("complete")) }()
	}
	a, b := <-result, <-result
	if (a == nil) == (b == nil) || (!errors.Is(a, ErrExists) && !errors.Is(b, ErrExists)) {
		t.Fatalf("claim results %v/%v", a, b)
	}
	data, err := store.Read(ctx, p, 100)
	if err != nil || string(data) != "complete" {
		t.Fatal("partial claim published")
	}
}
func TestCompletionIntegrity(t *testing.T) {
	r := fixture(t)
	store := &Objects{}
	_, err := Run(context.Background(), store, r, &narrator{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(objectPath(r, "report.json"), json.RawMessage(`{}`), 0600)
	if _, err = Run(context.Background(), store, r, &narrator{}, Config{}); err == nil {
		t.Fatal("corrupt completed report accepted")
	}
}

func TestEvidenceSampleIsBoundedAndValidated(t *testing.T) {
	r := fixture(t)
	m, err := Evaluate(context.Background(), &Objects{}, r)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Evidence) != 1 || m.Evidence[0].RecordID != "conv_000001" || m.Evidence[0].Text != "Synthetic" {
		t.Fatal("evidence not derived from accepted customer turn")
	}
	n := Narrative{Summary: "Test", Findings: []Finding{{Code: "evidence", Observation: "Test", MetricRefs: []string{"evidence.conv_000001.1"}}}, Recommendations: []string{}, Limitations: []string{"Synthetic"}}
	if n.Validate(m) != nil {
		t.Fatal("verified evidence reference rejected")
	}
	m.Evidence[0].Text = string(make([]byte, 1025))
	if ValidateMetrics(m, r) == nil {
		t.Fatal("oversized evidence replay accepted")
	}
}

func TestCompletedFakeCannotBeUsedAsVertexRun(t *testing.T) {
	r := fixture(t)
	n := &narrator{}
	store := &Objects{}
	ctx := context.Background()
	if _, err := Run(ctx, store, r, n, Config{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, store, r, n, Config{ModelMode: "vertex"}); err == nil || n.calls != 1 {
		t.Fatal("fake completion accepted as real model execution")
	}
}

func TestTelemetryPreservesCompletionFailureAndNoModelReplay(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			r := fixture(t)
			var logs bytes.Buffer
			logger := telemetry.New(&logs)
			n := &narrator{}
			if fail {
				n.err = errors.New("PRIVATE_MARKER")
			}
			cfg := Config{PromptSHA256: Digest([]byte("prompt")), ModelMode: "fake"}
			status, err := Run(logger.Context(context.Background(), ""), &Objects{}, r, n, cfg)
			if (err != nil) != fail || (!fail && status != "completed") {
				t.Fatal("observed analysis changed outcome")
			}
			if n.calls != 1 {
				t.Fatal("observation retried model")
			}
			snapshot := logs.String()
			for _, stage := range []string{"evaluation", "model_generation", "run"} {
				if !strings.Contains(snapshot, `"stage":"`+stage+`"`) {
					t.Fatal("missing stage observation", stage)
				}
			}
			if strings.Contains(snapshot, "PRIVATE_MARKER") || strings.Contains(snapshot, r.Shard.URI) || strings.Contains(snapshot, "Synthetic") {
				t.Fatal("private error/path/evidence logged")
			}
			var seenFailure bool
			for _, line := range strings.Split(strings.TrimSpace(snapshot), "\n") {
				var e telemetry.Event
				if json.Unmarshal([]byte(line), &e) != nil || e.Version != telemetry.Version {
					t.Fatal("invalid observation")
				}
				if e.Component == "analysis" && e.Stage == "run" {
					seenFailure = e.Status == "error"
					if e.ModelMode != "fake" || e.AnalysisID != r.AnalysisID {
						t.Fatal("model mode/correlation missing")
					}
				}
			}
			if seenFailure != fail {
				t.Fatal("failure without a success artifact was hidden")
			}
			_, replayErr := Run(logger.Context(context.Background(), ""), &Objects{}, r, n, cfg)
			if (replayErr != nil) != fail || n.calls != 1 {
				t.Fatal("instrumentation changed idempotency")
			}
		})
	}
}

func TestModelFailurePersistsOnlyAllowlistedCategory(t *testing.T) {
	for _, category := range []string{"metric_reference", "untrusted-diagnostic"} {
		t.Run(category, func(t *testing.T) {
			r := fixture(t)
			store := &Objects{}
			_, err := Run(context.Background(), store, r, &narrator{err: NarrationError{Category: category}}, Config{ModelMode: "fake"})
			if err == nil {
				t.Fatal("model failure lost")
			}
			raw, err := store.Read(context.Background(), objectPath(r, "_FAILED.json"), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			var failure map[string]string
			if Decode(raw, &failure) != nil || failure["stage"] != "model" {
				t.Fatal("failure stage unavailable")
			}
			if len(failure) != 2 {
				t.Fatal("historical failure marker changed")
			}
			diagnostic, readErr := store.Read(context.Background(), objectPath(r, "_MODEL_FAILURE.json"), 1<<20)
			if category == "metric_reference" {
				var detail map[string]string
				if readErr != nil || Decode(diagnostic, &detail) != nil || detail["category"] != category || detail["schema_version"] != "analysis-model-failure-v1" {
					t.Fatal("safe versioned category missing")
				}
				assertSchema(t, "model-failure", diagnostic)
			} else if !errors.Is(readErr, ErrMissing) {
				t.Fatal("untrusted diagnostic persisted")
			}
		})
	}
}

func TestProviderFailureDiagnosticIsVersionedAndPayloadFree(t *testing.T) {
	for _, tc := range []struct {
		diagnostic NarrationError
		persisted  bool
	}{
		{NarrationError{Category: "provider_http", HTTPStatus: 400, ProviderStatus: "INVALID_ARGUMENT"}, true},
		{NarrationError{Category: "provider_transport"}, true},
		{NarrationError{Category: "model_deadline"}, true},
		{NarrationError{Category: "provider_http", HTTPStatus: 400, ProviderStatus: "PRIVATE_MARKER"}, false},
		{NarrationError{Category: "provider_http", HTTPStatus: 0}, false},
		{NarrationError{Category: "provider_transport", ProviderStatus: "PRIVATE_MARKER"}, false},
	} {
		r := fixture(t)
		store := &Objects{}
		_, err := Run(context.Background(), store, r, &narrator{err: tc.diagnostic}, Config{ModelMode: "fake"})
		if err == nil {
			t.Fatal("provider failure ignored")
		}
		raw, readErr := store.Read(context.Background(), objectPath(r, "_MODEL_FAILURE.json"), 1<<20)
		if !tc.persisted {
			if !errors.Is(readErr, ErrMissing) {
				t.Fatal("untrusted provider diagnostic persisted")
			}
			continue
		}
		var detail map[string]any
		if readErr != nil || Decode(raw, &detail) != nil || detail["schema_version"] != "analysis-model-failure-v2" || detail["category"] != tc.diagnostic.Category || strings.Contains(string(raw), "PRIVATE_MARKER") {
			t.Fatal("invalid safe diagnostic")
		}
		schemaBytes, err := os.ReadFile("../../../schemas/analysis/model-failure-v2.json")
		if err != nil {
			t.Fatal(err)
		}
		var schema jsonschema.Schema
		if json.Unmarshal(schemaBytes, &schema) != nil {
			t.Fatal("invalid diagnostic schema")
		}
		resolved, err := schema.Resolve(nil)
		if err != nil || resolved.Validate(detail) != nil {
			t.Fatal("produced provider diagnostic violates versioned contract")
		}
		failed, err := store.Read(context.Background(), objectPath(r, "_FAILED.json"), 1<<20)
		var marker map[string]string
		if err != nil || Decode(failed, &marker) != nil || len(marker) != 2 {
			t.Fatal("historical failure marker changed")
		}
	}
}
