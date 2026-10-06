//go:build diagnostics

package localmeasure

import (
	"context"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"os"
	"path/filepath"
	"testing"
)

func TestRetainedRoundAndMetricDriftStopsWithoutRetry(t *testing.T) {
	request, baseline := os.Getenv("MEASURE_REQUEST"), os.Getenv("MEASURE_BASELINE")
	if request == "" || baseline == "" {
		t.Skip("set local retained request and baseline for integration")
	}
	raw, err := os.ReadFile(request)
	if err != nil {
		t.Fatal(err)
	}
	var r analysis.Request
	if analysis.Decode(raw, &r) != nil {
		t.Fatal("invalid test request")
	}
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	output := filepath.Join(t.TempDir(), "measurement")
	report, err := Run(context.Background(), r, baseline, output, 2)
	if err != nil || report.Status != "completed" || len(report.Samples) != 2 || report.Stages["analysis/run"].Count != 2 {
		t.Fatalf("round failed: %v", err)
	}
	if _, err := Run(context.Background(), r, baseline, output, 2); err == nil {
		t.Fatal("existing outputs reused")
	}
	raw, err = os.ReadFile(baseline)
	if err != nil {
		t.Fatal(err)
	}
	var metrics analysis.Metrics
	if analysis.Decode(raw, &metrics) != nil {
		t.Fatal("invalid baseline")
	}
	metrics.Evidence = []analysis.Evidence{}
	// Change a validated supervised metric while keeping input identity intact.
	correct := *metrics.CorrectOutcomes - 1
	accuracy := float64(correct) / float64(metrics.RecordsExpected)
	metrics.CorrectOutcomes, metrics.OutcomeAccuracy = &correct, &accuracy
	changed := filepath.Join(t.TempDir(), "baseline.json")
	if err := os.WriteFile(changed, analysis.JSON(metrics), 0600); err != nil {
		t.Fatal(err)
	}
	output = filepath.Join(t.TempDir(), "drift")
	if _, err := Run(context.Background(), r, changed, output, 2); err == nil {
		t.Fatal("metric drift accepted")
	}
	if _, err := os.Stat(filepath.Join(output, "cpu-sample-01", "_SUCCESS.json")); err != nil {
		t.Fatal("first analysis was not preserved")
	}
	if _, err := os.Stat(filepath.Join(output, "cpu-sample-02")); !os.IsNotExist(err) {
		t.Fatal("started another sample after drift")
	}
	if _, err := os.Stat(filepath.Join(output, "measurement.json")); err != nil {
		t.Fatal("failure report missing")
	}
}
