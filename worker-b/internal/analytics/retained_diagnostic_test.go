//go:build diagnostics

package analytics

import (
	"context"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"os"
	"testing"
)

func TestRetainedCompletedReport(t *testing.T) {
	prefix := os.Getenv("RETAINED_REPORT_PREFIX")
	if prefix == "" {
		t.Skip("optional retained artifact validation")
	}
	id := "part2-v2-cached-test-01"
	store := &analysis.Objects{}
	for _, name := range []string{"request", "metrics", "report"} {
		raw, err := store.Read(context.Background(), prefix+"/"+id+"/"+name+".json", analysis.MaxObjectBytes)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(name, "schema_valid", schemaValid(name, raw))
	}
	raw, _ := store.Read(context.Background(), prefix+"/"+id+"/request.json", analysis.MaxObjectBytes)
	var req analysis.Request
	_ = analysis.Decode(raw, &req)
	raw, _ = store.Read(context.Background(), prefix+"/"+id+"/report.json", analysis.MaxObjectBytes)
	var report analysis.Report
	_ = analysis.Decode(raw, &report)
	t.Log("request_validation", req.Validate(true))
	t.Log("metric_validation", analysis.ValidateMetrics(report.Metrics, req))
	t.Log("narrative_validation", report.Narrative.Validate(report.Metrics))
	if _, _, err := Load(context.Background(), store, prefix, id); err != nil {
		t.Fatal(err)
	}
}
