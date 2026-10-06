package preparation

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/juanmh10/gemma-microservices/internal/gcs"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

func TestLargerBatchSeparatesLabelsAndVersionsPrompt(t *testing.T) {
	root := t.TempDir()
	source, metadata := filepath.Join(root, "source.jsonl"), filepath.Join(root, "metadata.jsonl")
	var conversations, labels bytes.Buffer
	for i := 0; i < 300; i++ {
		if err := json.NewEncoder(&conversations).Encode(map[string]any{"sample_id": i, "outcome": "failure", "dialogue": []map[string]string{{"role": "sales", "text": "A plan"}, {"role": "customer", "text": "Too expensive"}}}); err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(&labels).Encode(map[string]any{"sample_id": i, "hidden_objection_ids": []string{"price"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(source, conversations.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadata, labels.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	store := gcs.New()
	defer store.Close()
	prepared, truth := filepath.Join(root, "prepared"), filepath.Join(root, "truth")
	if err := Run(context.Background(), store, source, metadata, prepared, truth, "batch", "worker-a-v2", 300, 3); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(prepared, "batch", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.PromptVersion != "worker-a-v2" || m.RecordsTotal != 300 || len(m.Shards) != 3 {
		t.Fatalf("incorrect manifest: %+v", m)
	}
	seen := map[string]bool{}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	for _, shard := range m.Shards {
		if shard.Records != 100 {
			t.Fatalf("unbalanced shard: %+v", shard)
		}
		compressed, err := os.ReadFile(shard.URI)
		if err != nil {
			t.Fatal(err)
		}
		data, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("outcome")) || bytes.Contains(data, []byte("hidden_objection")) {
			t.Fatal("labels leaked into worker input")
		}
		for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
			var row conversation
			if err := json.Unmarshal(line, &row); err != nil {
				t.Fatal(err)
			}
			if seen[row.RecordID] || row.Messages[0].Role != "seller" || row.Messages[1].TurnID != 2 {
				t.Fatal("invalid prepared conversation")
			}
			seen[row.RecordID] = true
		}
	}
	// A new full-pipeline run can prepare all 300 records for one bounded GPU task.
	if err := Run(context.Background(), store, source, metadata, prepared, truth, "pipeline-fresh-test", "worker-a-v2", 300, 1); err != nil {
		t.Fatal(err)
	}
	fresh, err := os.ReadFile(filepath.Join(prepared, "pipeline-fresh-test", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var single manifest
	if err := json.Unmarshal(fresh, &single); err != nil {
		t.Fatal(err)
	}
	if single.RecordsTotal != 300 || len(single.Shards) != 1 || single.Shards[0].Records != 300 {
		t.Fatal("fresh run did not retain all records in task zero")
	}
	original, err := os.ReadFile(filepath.Join(prepared, "batch", "manifest.json"))
	if err != nil || !bytes.Equal(original, raw) {
		t.Fatal("fresh preparation changed the historical run")
	}

	rows, err := parquet.ReadFile[label](filepath.Join(truth, "batch", "labels.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 300 || len(seen) != 300 || !bytes.Contains([]byte(rows[0].MetadataJSON), []byte("hidden_objection_ids")) {
		t.Fatal("ground truth coverage was lost")
	}
}

func TestCanonicalSupportDomainKeepsReferencesIsolated(t *testing.T) {
	root := t.TempDir()
	source, metadata := filepath.Join(root, "source.jsonl"), filepath.Join(root, "metadata.jsonl")
	// Tiny contract fixtures are not evidence of model generalization.
	input := `{"record_id":"conv_100001","messages":[{"turn_id":1,"role":"seller","text":"Has the refund arrived?"},{"turn_id":2,"role":"customer","text":"I was charged twice. The refund has now arrived and my billing issue is resolved."}]}` + "\n"
	reference := `{"record_id":"conv_100001","outcome":"success","cause_codes":["billing"],"evidence_turns":{"billing":[2]},"review_status":"draft"}` + "\n"
	if err := os.WriteFile(source, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadata, []byte(reference), 0600); err != nil {
		t.Fatal(err)
	}
	store := gcs.New()
	defer store.Close()
	prepared, truth := filepath.Join(root, "prepared"), filepath.Join(root, "truth")
	domainPath := filepath.Join("..", "..", "schemas", "domains", "support-resolution-v1.json")
	if err := RunConfigured(context.Background(), store, source, metadata, prepared, truth, "support-contract", "support-contract-v1", domainPath, true, 0, 1); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(prepared, "support-contract", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.DomainVersion != "support-resolution-v1" || len(m.DomainSHA256) != 64 || m.PromptVersion != "worker-a-v3" || m.SchemaVersion != "conversation-analysis-v1" {
		t.Fatalf("bad domain manifest: %+v", m)
	}
	if hash, err := fileSHA256(m.DomainURI); err != nil || hash != m.DomainSHA256 {
		t.Fatal("domain checksum mismatch")
	}
	compressed, err := os.ReadFile(m.Shards[0].URI)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	data, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"cause_codes", "evidence_turns", "review_status", "outcome"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("reference field leaked: %s", forbidden)
		}
	}
	labels, err := parquet.ReadFile[label](filepath.Join(truth, "support-contract", "labels.parquet"))
	if err != nil || len(labels) != 1 {
		t.Fatal("missing isolated references", err)
	}
	if labels[0].Outcome != "success" || !bytes.Contains([]byte(labels[0].MetadataJSON), []byte("billing")) {
		t.Fatal("reference changed")
	}
	// Reject labels accidentally supplied as canonical worker input.
	if err := os.WriteFile(source, []byte(`{"record_id":"conv_100001","messages":[],"outcome":"success"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RunConfigured(context.Background(), store, source, metadata, prepared, truth, "bad-support", "support-contract-v1", domainPath, true, 0, 1); err == nil {
		t.Fatal("accepted labels in canonical source")
	}
}

func TestDomainConfigurationRejectsUnknownOrInvalidDefinitions(t *testing.T) {
	for _, raw := range []string{`{}`, `{"version":"support-v1","task":"x","outcomes":{"success":"yes","failure":"no"},"evidence_role":"agent","causes":{"billing":"charge"}}`, `{"version":"support-v1","task":"x","outcomes":{"success":"yes","failure":"no"},"evidence_role":"customer","causes":{"billing":"charge"},"labels":[]}`} {
		if _, err := parseDomain([]byte(raw)); err == nil {
			t.Fatal("accepted invalid domain")
		}
	}
}
