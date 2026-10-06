// Package localmeasure measures retained-file analysis without cloud clients.
package localmeasure

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	prompt "github.com/juanmh10/gemma-microservices/prompts/worker-b"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/agent"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
)

type Sample struct {
	Index         int              `json:"index"`
	WallUS        int64            `json:"wall_us"`
	StageUS       map[string]int64 `json:"stage_us"`
	CPUUS         *int64           `json:"process_cpu_delta_us,omitempty"`
	PeakRSS       *int64           `json:"process_peak_rss_bytes,omitempty"`
	MetricsSHA256 string           `json:"metrics_sha256"`
}
type Stats struct {
	Count  int     `json:"count"`
	Min    int64   `json:"min_us"`
	Median float64 `json:"median_us"`
	Max    int64   `json:"max_us"`
}
type Report struct {
	Version        string           `json:"schema_version"`
	Status         string           `json:"status"`
	Mode           string           `json:"model_mode"`
	Concurrency    int              `json:"concurrency"`
	Requested      int              `json:"requested_samples"`
	RequestSHA256  string           `json:"request_sha256"`
	BaselineSHA256 string           `json:"baseline_metrics_sha256"`
	Samples        []Sample         `json:"samples"`
	Wall           Stats            `json:"wall"`
	Stages         map[string]Stats `json:"stages"`
}

func Summarize(values []int64) Stats {
	if len(values) == 0 {
		return Stats{}
	}
	v := append([]int64(nil), values...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	median := float64(v[len(v)/2])
	if len(v)%2 == 0 {
		median = float64(v[len(v)/2-1])/2 + median/2
	}
	return Stats{len(v), v[0], median, v[len(v)-1]}
}

// Validate rejects remote or real-model input before creating output or a narrator.
func Validate(r analysis.Request, output string, count int) error {
	if count < 1 || count > 10 || r.ModelMode != "fake" || r.Mode != "analyze" || r.Metrics != nil || r.Truth == nil || output == "" || strings.Contains(output, "://") {
		return errors.New("local measurement requires fake analysis, truth and 1..10 samples")
	}
	r.AnalysisID = "cpu-sample-01"
	r.OutputPrefix = output
	return r.Validate(false)
}

func boundedRead(name string, limit int64) ([]byte, error) {
	return (&analysis.Objects{}).Read(context.Background(), name, limit)
}

// Run uses fresh immutable analysis IDs in one sequential resident process.
// It stops on the first failure or metric mismatch and never retries a sample.
func Run(ctx context.Context, r analysis.Request, baseline, output string, count int) (Report, error) {
	report := Report{Version: "local-cpu-measurement-v1", Status: "failed", Mode: "fake", Concurrency: 1, Requested: count, RequestSHA256: analysis.Digest(analysis.JSON(r)), Stages: map[string]Stats{}}
	if err := Validate(r, output, count); err != nil {
		return report, err
	}
	if strings.Contains(baseline, "://") {
		return report, errors.New("local baseline required")
	}
	raw, err := boundedRead(baseline, analysis.MaxObjectBytes)
	if err != nil {
		return report, errors.New("baseline unavailable")
	}
	var metrics analysis.Metrics
	if analysis.Decode(raw, &metrics) != nil || analysis.ValidateMetrics(metrics, r) != nil {
		return report, errors.New("baseline invalid")
	}
	report.BaselineSHA256 = analysis.Digest(analysis.JSON(metrics))
	if err := os.Mkdir(output, 0700); err != nil {
		return report, errors.New("fresh output directory required")
	}
	store := &analysis.Objects{}
	defer store.Close()
	narrator := &agent.Reporter{Instruction: prompt.Instruction, Fake: true}
	for i := 1; i <= count; i++ {
		if ctx.Err() != nil {
			return finish(report, output, ctx.Err())
		}
		r.AnalysisID = fmt.Sprintf("cpu-sample-%02d", i)
		r.OutputPrefix = output
		var events bytes.Buffer
		sampleCtx := telemetry.New(&events).Context(ctx, r.AnalysisID)
		started := time.Now()
		status, runErr := analysis.Run(sampleCtx, store, r, narrator, analysis.Config{ModelMode: "fake", PromptSHA256: analysis.Digest([]byte(prompt.Instruction))})
		sample := Sample{Index: i, WallUS: time.Since(started).Microseconds(), StageUS: map[string]int64{}}
		if runErr != nil || status != "completed" {
			return finish(report, output, errors.New("sample failed; no retry"))
		}
		artifact, err := store.Read(ctx, filepath.Join(output, r.AnalysisID, "metrics.json"), analysis.MaxObjectBytes)
		if err != nil {
			return finish(report, output, errors.New("sample metrics unavailable"))
		}
		sample.MetricsSHA256 = analysis.Digest(artifact)
		if sample.MetricsSHA256 != report.BaselineSHA256 {
			return finish(report, output, errors.New("sample metrics differ from baseline"))
		}
		scanner := bufio.NewScanner(&events)
		runs := 0
		for scanner.Scan() {
			var event telemetry.Event
			if json.Unmarshal(scanner.Bytes(), &event) != nil || event.Version != telemetry.Version || event.EventLimitReached {
				return finish(report, output, errors.New("incomplete telemetry"))
			}
			sample.StageUS[event.Component+"/"+event.Stage] += event.ElapsedUS
			if event.Component == "analysis" && event.Stage == "run" {
				runs++
				sample.CPUUS, sample.PeakRSS = event.ProcessCPUUS, event.ProcessPeakRSSBytes
			}
		}
		if scanner.Err() != nil || runs != 1 {
			return finish(report, output, errors.New("missing run observation"))
		}
		report.Samples = append(report.Samples, sample)
	}
	report.Status = "completed"
	return finish(report, output, nil)
}

func finish(report Report, output string, cause error) (Report, error) {
	var wall []int64
	stages := map[string][]int64{}
	for _, sample := range report.Samples {
		wall = append(wall, sample.WallUS)
		for stage, value := range sample.StageUS {
			stages[stage] = append(stages[stage], value)
		}
	}
	report.Wall = Summarize(wall)
	for stage, values := range stages {
		report.Stages[stage] = Summarize(values)
	}
	if err := (&analysis.Objects{}).Create(context.Background(), filepath.Join(output, "measurement.json"), analysis.JSON(report)); err != nil {
		return report, errors.New("measurement report write failed")
	}
	return report, cause
}
