// Package integration exercises the 900-record contracts without cloud or model calls.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/juanmh10/gemma-microservices/internal/gcs"
	"github.com/juanmh10/gemma-microservices/internal/preparation"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/agent"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analytics"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/messaging"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/pipeline"
	"github.com/parquet-go/parquet-go"
)

func batchFixture(t *testing.T) analysis.Request {
	t.Helper()
	root := t.TempDir()
	ctx := context.Background()
	var data, labels bytes.Buffer
	for i := range 900 {
		_ = json.NewEncoder(&data).Encode(map[string]any{"sample_id": i, "outcome": "failure", "dialogue": []map[string]string{{"role": "customer", "text": "Synthetic fixture"}}})
		_ = json.NewEncoder(&labels).Encode(map[string]any{"sample_id": i, "hidden_objection_ids": []string{"price"}})
	}
	source, metadata := filepath.Join(root, "source.jsonl"), filepath.Join(root, "metadata.jsonl")
	if err := os.WriteFile(source, data.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadata, labels.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	runID := "pipeline-fresh-fixture"
	store := gcs.New()
	defer store.Close()
	prepared, truth := filepath.Join(root, "prepared"), filepath.Join(root, "truth")
	if err := preparation.Run(ctx, store, source, metadata, prepared, truth, runID, "worker-a-v2", 900, 3); err != nil {
		t.Fatal(err)
	}
	ref := func(p string) analysis.ObjectRef {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return analysis.ObjectRef{URI: p, SHA256: analysis.Digest(raw)}
	}
	mf := ref(filepath.Join(prepared, runID, "manifest.json"))
	var manifest struct {
		Shards []struct {
			URI string `json:"uri"`
		} `json:"shards"`
	}
	raw, _ := os.ReadFile(mf.URI)
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	label := ref(filepath.Join(truth, runID, "labels.parquet"))
	r := analysis.Request{SchemaVersion: analysis.BatchSchemaVersion, Mode: "analyze", ModelMode: "fake", AnalysisID: "full-fixture", SourceRunID: runID, SourceImage: pipeline.CachedWorkerImage, Manifest: mf, Truth: &label, Rules: analysis.QualityRules{Version: "quality-v1", MinValidRate: .99, MinOutcomeAccuracy: .98, MinCauseF1: .9}, Model: analysis.ModelID, Location: analysis.Location, PromptVersion: analysis.PromptVersion, OutputPrefix: filepath.Join(root, "analysis")}
	for task := range 3 {
		taskDir := filepath.Join(root, fmt.Sprintf("task-%05d", task))
		if err := os.Mkdir(taskDir, 0700); err != nil {
			t.Fatal(err)
		}
		marker := benchmark.Marker{RunID: runID, TaskIndex: task, GPUProfile: "rtx6000", Status: "success", ImageDigest: r.SourceImage, ManifestSHA256: mf.SHA256, RecordsTotal: 300, RecordsProcessed: 300, ModelRevision: "fixture", Quantization: "fixture", VLLMVersion: "fixture", PromptVersion: "worker-a-v2", PromptSHA256: analysis.Digest([]byte("fixture"))}
		markerPath := filepath.Join(taskDir, "_SUCCESS.json")
		if err := os.WriteFile(markerPath, analysis.JSON(marker), 0600); err != nil {
			t.Fatal(err)
		}
		var rows []benchmark.PredictionRow
		for i := task; i < 900; i += 3 {
			causes := []benchmark.Cause{}
			if task != 1 {
				code := "price"
				if task == 2 {
					code = "other"
				}
				causes = append(causes, benchmark.Cause{Code: code, Confidence: .9, EvidenceTurns: []int32{1}})
			}
			rows = append(rows, benchmark.PredictionRow{ConversationID: fmt.Sprintf("conv_%06d", i), Status: "ok", Outcome: "failure", Causes: causes})
		}
		chunkPath := filepath.Join(taskDir, "chunk.parquet")
		if err := parquet.WriteFile(chunkPath, rows); err != nil {
			t.Fatal(err)
		}
		taskSource := analysis.TaskSource{TaskIndex: task, Shard: ref(manifest.Shards[task].URI), Marker: ref(markerPath), Chunks: []analysis.ObjectRef{ref(chunkPath)}}
		if task == 0 {
			r.Shard = taskSource.Shard
			r.Marker = taskSource.Marker
			r.Chunks = taskSource.Chunks
		} else {
			r.AdditionalTasks = append(r.AdditionalTasks, taskSource)
		}
	}
	return r
}

type narrator struct{ calls int }

func (n *narrator) Generate(ctx context.Context, m analysis.Metrics, d analysis.Decision) (analysis.Narrative, analysis.Usage, error) {
	n.calls++
	return (&agent.Reporter{Fake: true, Instruction: "Report only verified full-population metrics."}).Generate(ctx, m, d)
}

type emailSender struct {
	calls int
	text  string
}

func (s *emailSender) Send(_ context.Context, _ string, p messaging.EmailRequest) (string, error) {
	s.calls++
	s.text = p.Text
	return "provider-fixture", nil
}

type warehouse struct{ report analysis.Report }

func (w *warehouse) Index(_ context.Context, _ string, r analysis.Report, _ string) (string, error) {
	w.report = r
	return "fixture-index", nil
}
func (w *warehouse) History(context.Context, analytics.Filter) ([]json.RawMessage, error) {
	return nil, nil
}

type publisher struct{ calls int }

func (p *publisher) Publish(context.Context, []byte) (string, error) {
	p.calls++
	return "fixture-message", nil
}
func TestFresh900RecordsEvaluateLoadNotifyAndReplay(t *testing.T) {
	for _, version := range []string{analysis.PromptVersion, analysis.BatchPromptVersion} {
		t.Run(version, func(t *testing.T) { checkFresh900RecordsEvaluateLoadNotifyAndReplay(t, version) })
	}
}

func checkFresh900RecordsEvaluateLoadNotifyAndReplay(t *testing.T, version string) {
	r := batchFixture(t)
	r.PromptVersion = version
	ctx := context.Background()
	s := &analysis.Objects{}
	if err := r.Validate(false); err != nil {
		t.Fatal(err)
	}
	if _, err := analysis.Evaluate(ctx, s, r); err != nil {
		t.Fatal(err)
	}
	n := &narrator{}
	cfg := analysis.Config{ModelMode: "fake", ImageDigest: "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/analysis@sha256:" + strings.Repeat("a", 64), SourceRevision: strings.Repeat("a", 40), SourceSHA256: strings.Repeat("a", 64), PromptSHA256: strings.Repeat("a", 64)}
	if _, err := analysis.Run(ctx, s, r, n, cfg); err != nil {
		t.Fatal(err)
	}
	report, hash, err := analytics.Load(ctx, s, r.OutputPrefix, r.AnalysisID)
	if err != nil {
		t.Fatal(err)
	}
	m := report.Metrics
	if m.RecordsExpected != 900 || m.SourceRecords != 900 || m.RecordsValid != 900 || *m.CorrectOutcomes != 900 || *m.CauseF1 != .4 {
		t.Fatal("incorrect population metrics", m.RecordsExpected, m.CauseF1)
	}
	engine := messaging.Engine{Store: s, PublicationPrefix: filepath.Join(t.TempDir(), "publication"), NotificationPrefix: filepath.Join(t.TempDir(), "notification"), Load: func(context.Context, string) (analysis.Report, string, error) { return report, hash, nil }}
	w := &warehouse{}
	if _, err := analytics.Index(ctx, s, w, r.OutputPrefix, filepath.Join(t.TempDir(), "index"), r.AnalysisID, "first"); err != nil || w.report.Metrics.RecordsExpected != 900 {
		t.Fatal("full indexing failed", err)
	}
	sender := &emailSender{}
	engine.Email, err = messaging.NewEmailDelivery(sender, "sender"+"@"+"example.invalid", "recipient"+"@"+"example.invalid", "example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	p := &publisher{}
	event, err := engine.Publish(ctx, p, r.AnalysisID, false)
	if err != nil {
		t.Fatal(err)
	}
	notification := messaging.Render(event, report)
	if !messaging.SchemaValid("notification", analysis.JSON(notification)) || notification.RecordsExpected != 900 {
		t.Fatal("900-record notification rejected")
	}
	if err := engine.Deliver(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := engine.Deliver(ctx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Publish(ctx, p, r.AnalysisID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := analysis.Run(ctx, s, r, n, cfg); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 || !strings.Contains(sender.text, "900/900") || strings.Contains(sender.text, "retained") {
		t.Fatal("incorrect or duplicate full-batch email")
	}
	if n.calls != 1 || p.calls != 1 || engine.Status(ctx, r.AnalysisID, hash)["notification_status"] != "delivered" {
		t.Fatal("replay performed duplicate work")
	}
}
func TestPartialBatchCannotCallNarratorOrPublishReport(t *testing.T) {
	r := batchFixture(t)
	if err := os.Remove(r.AdditionalTasks[1].Marker.URI); err != nil {
		t.Fatal(err)
	}
	n := &narrator{}
	if _, err := analysis.Run(context.Background(), &analysis.Objects{}, r, n, analysis.Config{ModelMode: "fake"}); err == nil || n.calls != 0 {
		t.Fatal("partial batch accepted")
	}
}

func TestBatchRejectsDuplicatePreparedShardAndPartialMetrics(t *testing.T) {
	r := batchFixture(t)
	r.AdditionalTasks[1].Shard = r.Shard
	if _, err := analysis.Evaluate(context.Background(), &analysis.Objects{}, r); err == nil {
		t.Fatal("duplicate prepared records accepted")
	}
	r = batchFixture(t)
	m, err := analysis.Evaluate(context.Background(), &analysis.Objects{}, r)
	if err != nil {
		t.Fatal(err)
	}
	m.RecordsExpected = 300
	m.RecordsValid = 300
	m.RecordsInvalid = 0
	if err := analysis.ValidateMetrics(m, r); err == nil {
		t.Fatal("partial population accepted")
	}
}
