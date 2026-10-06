package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/klauspost/compress/zstd"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// The writer schema matches Worker A's PyArrow nested LIST fields.
type workerCause struct {
	Code          string  `parquet:"code"`
	Confidence    float32 `parquet:"confidence"`
	EvidenceTurns []int32 `parquet:"evidence_turns,list"`
}

type workerPrediction struct {
	ConversationID string        `parquet:"conversation_id"`
	Status         string        `parquet:"status"`
	Outcome        string        `parquet:"outcome"`
	Causes         []workerCause `parquet:"causes,list"`
	InputTokens    int32         `parquet:"input_tokens"`
	OutputTokens   int32         `parquet:"output_tokens"`
	DurationMS     int64         `parquet:"duration_ms"`
}

func TestReadWorkerCausesAndEvidenceTurns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chunk-000000.parquet")
	want := workerPrediction{
		ConversationID: "conv_000001",
		Status:         "ok",
		Outcome:        "failure",
		Causes: []workerCause{{
			Code: "contract_trap", Confidence: 0.8, EvidenceTurns: []int32{2, 4},
		}},
	}
	if err := parquet.WriteFile(path, []workerPrediction{want}); err != nil {
		t.Fatal(err)
	}
	got, err := parquet.ReadFile[PredictionRow](path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Causes) != 1 {
		t.Fatalf("expected one prediction with one cause, got %+v", got)
	}
	if got[0].Causes[0].Code != want.Causes[0].Code ||
		len(got[0].Causes[0].EvidenceTurns) != 2 ||
		got[0].Causes[0].EvidenceTurns[0] != 2 ||
		got[0].Causes[0].EvidenceTurns[1] != 4 {
		t.Fatalf("nested cause data was not preserved: %+v", got[0].Causes[0])
	}
}

func TestCanaryRequiresExactCoverageAndCountsInvalidOutputs(t *testing.T) {
	root := t.TempDir()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll([]byte("{\"record_id\":\"a\"}\n{\"record_id\":\"b\"}\n"), nil)
	encoder.Close()
	shard := filepath.Join(root, "shard.zst")
	if err := os.WriteFile(shard, compressed, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(compressed)
	manifest, _ := json.Marshal(map[string]any{"run_id": "test", "prompt_version": "worker-a-v2", "shards": []map[string]any{{"index": 0, "records": 2, "sha256": hex.EncodeToString(sum[:])}}})
	manifestPath := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(manifestPath, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(manifest)
	out := filepath.Join(root, "output")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	marker := Marker{RunID: "test", TaskIndex: 0, GPUProfile: "rtx6000", Status: "success", ManifestSHA256: hex.EncodeToString(hash[:]), RecordsTotal: 2, RecordsProcessed: 2, PromptVersion: "worker-a-v2", PromptSHA256: "prompt-hash", ImageDigest: "image@sha256:digest", ModelRevision: "revision", Quantization: "nvfp4", VLLMVersion: "pinned"}
	markerBytes, _ := json.Marshal(marker)
	if err := os.WriteFile(filepath.Join(out, "_SUCCESS.json"), markerBytes, 0600); err != nil {
		t.Fatal(err)
	}
	rows := []PredictionRow{{ConversationID: "a", Status: "ok", Outcome: "failure", Causes: []Cause{{Code: "price"}, {Code: "contract_lock"}}, DurationMS: 10}, {ConversationID: "b", Status: "invalid_output"}}
	chunk := filepath.Join(out, "chunk-000000.parquet")
	if err := parquet.WriteFile(chunk, rows); err != nil {
		t.Fatal(err)
	}
	truth := map[string]Reference{"a": {Outcome: "failure", Causes: map[string]bool{"price": true}}, "b": {Outcome: "success", Causes: map[string]bool{"boredom": true}}, "c": {Outcome: "failure"}}
	options := Options{Tasks: 1, ManifestPath: manifestPath, ShardPath: shard}
	report, err := Evaluate(out, "rtx6000", truth, options)
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 2 || report.SourceRecords != 3 || report.SchemaValidRate != 0.5 || report.CauseF1 != 0.5 || report.PerCause["boredom"].FalseNegative != 1 {
		t.Fatalf("unexpected canary metrics: %+v", report)
	}
	t.Run("missing prediction", func(t *testing.T) {
		if err := parquet.WriteFile(chunk, rows[:1]); err != nil {
			t.Fatal(err)
		}
		if _, err := Evaluate(out, "rtx6000", truth, options); err == nil {
			t.Fatal("missing prediction accepted")
		}
	})
	t.Run("unknown prediction", func(t *testing.T) {
		bad := append([]PredictionRow(nil), rows...)
		bad[1].ConversationID = "c"
		if err := parquet.WriteFile(chunk, bad); err != nil {
			t.Fatal(err)
		}
		if _, err := Evaluate(out, "rtx6000", truth, options); err == nil {
			t.Fatal("off-shard prediction accepted")
		}
	})
	t.Run("corrupt shard", func(t *testing.T) {
		if err := os.WriteFile(shard, []byte("corrupt"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Evaluate(out, "rtx6000", truth, options); err == nil {
			t.Fatal("corrupt shard accepted")
		}
	})
}

func TestGenericReferencesAcceptDomainCauseCodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "labels.parquet")
	rows := []TruthRow{{RecordID: "conv_100001", Outcome: "success", MetadataJSON: `{"cause_codes":["billing"],"evidence_turns":{"billing":[2]}}`}}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}
	truth, err := ReadTruth(path)
	if err != nil {
		t.Fatal(err)
	}
	if !truth["conv_100001"].Causes["billing"] {
		t.Fatal("generic cause was dropped")
	}
	rows[0].MetadataJSON = `{"cause_codes":["billing"],"hidden_objection_ids":["boredom"]}`
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTruth(path); err == nil {
		t.Fatal("ambiguous reference accepted")
	}
}

func TestV3EvaluationBindsConfiguredDomainIdentity(t *testing.T) {
	root := t.TempDir()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll([]byte("{\"record_id\":\"conv_100001\"}\n"), nil)
	encoder.Close()
	shardPath := filepath.Join(root, "shard.zst")
	if err := os.WriteFile(shardPath, compressed, 0600); err != nil {
		t.Fatal(err)
	}
	shardHash := sha256.Sum256(compressed)
	domainHash := sha256.Sum256([]byte("support-configuration"))
	domainSHA := hex.EncodeToString(domainHash[:])
	data, err := json.Marshal(map[string]any{"run_id": "support-heldout", "prompt_version": "worker-a-v3", "domain_version": "support-resolution-v1", "domain_sha256": domainSHA, "shards": []map[string]any{{"index": 0, "records": 1, "sha256": hex.EncodeToString(shardHash[:])}}})
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	manifestHash := sha256.Sum256(data)
	marker := Marker{RunID: "support-heldout", TaskIndex: 0, GPUProfile: "rtx6000", Status: "success", RecordsTotal: 1, RecordsProcessed: 1, ManifestSHA256: hex.EncodeToString(manifestHash[:]), PromptVersion: "worker-a-v3", PromptSHA256: "prompt", ImageDigest: "image@sha256:digest", ModelRevision: "revision", Quantization: "nvfp4", VLLMVersion: "pinned", DomainVersion: "support-resolution-v1", DomainSHA256: domainSHA}
	out := filepath.Join(root, "output")
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(filepath.Join(out, "chunk-000000.parquet"), []PredictionRow{{ConversationID: "conv_100001", Status: "ok", Outcome: "success", Causes: []Cause{{Code: "billing"}}}}); err != nil {
		t.Fatal(err)
	}
	truth := map[string]Reference{"conv_100001": {Outcome: "success", Causes: map[string]bool{"billing": true}}}
	options := Options{Tasks: 1, ManifestPath: manifestPath, ShardPath: shardPath}
	for _, bad := range []bool{false, true} {
		if bad {
			marker.DomainVersion = "gym-sales-v1"
		}
		raw, err := json.Marshal(marker)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "_SUCCESS.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		report, err := Evaluate(out, "rtx6000", truth, options)
		if bad {
			if err == nil {
				t.Fatal("accepted wrong domain version")
			}
		} else if err != nil || report.DomainSHA256 != domainSHA || report.CauseF1 != 1 {
			t.Fatalf("configured-domain evaluation failed: %+v %v", report, err)
		}
	}
}
