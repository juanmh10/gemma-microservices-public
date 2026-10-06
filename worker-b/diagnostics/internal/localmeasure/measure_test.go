package localmeasure

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

func validRequest() analysis.Request {
	ref := func(name string) analysis.ObjectRef {
		return analysis.ObjectRef{URI: name, SHA256: strings.Repeat("a", 64)}
	}
	truth := ref("truth")
	return analysis.Request{SchemaVersion: analysis.SchemaVersion, Mode: "analyze", ModelMode: "fake", SourceRunID: "pilot-004", SourceImage: "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/worker-a@sha256:" + strings.Repeat("a", 64), Manifest: ref("manifest"), Shard: ref("shard"), Marker: ref("marker"), Chunks: []analysis.ObjectRef{ref("chunk")}, Truth: &truth, Rules: analysis.QualityRules{Version: "quality-v1", MinValidRate: .99, MinOutcomeAccuracy: .98, MinCauseF1: .9}, Model: analysis.ModelID, Location: analysis.Location, PromptVersion: analysis.PromptVersion}
}

func TestRejectsCloudAndUnboundedWorkBeforeWriting(t *testing.T) {
	for _, name := range []string{"vertex", "remote-source", "remote-output", "too-many", "no-truth", "reuse-metrics"} {
		t.Run(name, func(t *testing.T) {
			r, count := validRequest(), 3
			output := filepath.Join(t.TempDir(), "new")
			switch name {
			case "vertex":
				r.ModelMode = "vertex"
			case "remote-source":
				r.Chunks[0].URI = "gs://your-gcp-project-id-results/chunk"
			case "remote-output":
				output = "gs://your-gcp-project-id-results/analysis"
			case "too-many":
				count = 11
			case "no-truth":
				r.Truth = nil
			case "reuse-metrics":
				r.Metrics = &r.Marker
			}
			if _, err := Run(context.Background(), r, "missing", output, count); err == nil {
				t.Fatal("unsafe request accepted")
			}
			if !strings.Contains(output, "://") {
				if _, err := os.Stat(output); !os.IsNotExist(err) {
					t.Fatal("output created before rejection")
				}
			}
		})
	}
}

func TestMedianDoesNotMutateOrInventPercentiles(t *testing.T) {
	values := []int64{9, 1, 7, 3}
	stats := Summarize(values)
	if stats.Count != 4 || stats.Median != 5 || stats.Min != 1 || stats.Max != 9 || values[0] != 9 {
		t.Fatal("incorrect summary")
	}
	if Summarize(nil).Count != 0 {
		t.Fatal("empty population fabricated")
	}
}
