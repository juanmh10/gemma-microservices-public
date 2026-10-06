package orchestration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/option"
	run "google.golang.org/api/run/v2"
)

func TestMonitorSuccessAndCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "success"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			launches, reads, cancels := 0, 0, 0
			manifest := "gs://your-gcp-project-id-prepared/runs/test/manifest.json"
			results := "gs://your-gcp-project-id-results/canary/test"
			executionName := JobName + "/executions/test"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":run"):
					launches++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body["overrides"] != nil {
						t.Error("unexpected execution overrides")
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"name": "projects/your-gcp-project-id/locations/us-central1/operations/test", "metadata": map[string]any{"name": executionName}})
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":cancel"):
					cancels++
					_ = json.NewEncoder(w).Encode(map[string]any{"done": true})
				case strings.HasSuffix(r.URL.Path, "/executions/test"):
					reads++
					e := map[string]any{"name": executionName, "runningCount": 1}
					if !deadline || cancels > 0 {
						e["runningCount"] = 0
						e["completionTime"] = "2026-09-29T21:00:00Z"
						if deadline {
							e["cancelledCount"] = 1
						} else {
							e["succeededCount"] = 1
						}
					}
					_ = json.NewEncoder(w).Encode(e)
				default:
					_ = json.NewEncoder(w).Encode(map[string]any{"etag": "test", "template": map[string]any{"taskCount": 1, "parallelism": 1, "template": map[string]any{"maxRetries": 0, "timeout": "900s", "containers": []any{map[string]any{"image": "image@sha256:abc", "resources": map[string]any{"limits": map[string]string{"nvidia.com/gpu": "1", "cpu": "20", "memory": "80Gi"}}, "env": []any{map[string]string{"name": "MANIFEST_URI", "value": manifest}, map[string]string{"name": "RESULTS_PREFIX", "value": results}, map[string]string{"name": "SOFT_DEADLINE_SECONDS", "value": "600"}, map[string]string{"name": "TASK_TIMEOUT_SECONDS", "value": "900"}}}}}}})
				}
			}))
			defer server.Close()
			service, err := run.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			monitor := Monitor{Service: service, PollInterval: time.Millisecond, CancelAfter: 20 * time.Millisecond, CleanupTimeout: time.Second}
			execution, err := monitor.Wait(context.Background(), manifest, results)
			if deadline && err == nil {
				t.Fatal("expected deadline failure")
			}
			if !deadline && err != nil {
				t.Fatal(err)
			}
			if execution == nil || execution.RunningCount != 0 || launches != 1 || reads < 2 {
				t.Fatalf("unconfirmed execution: launches=%d reads=%d execution=%+v", launches, reads, execution)
			}
			if deadline && cancels != 1 || !deadline && cancels != 0 {
				t.Fatalf("unexpected cancellation count %d", cancels)
			}
		})
	}
}
