package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	run "google.golang.org/api/run/v2"
)

func testCloud(t *testing.T, handler http.HandlerFunc) *Cloud {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	svc, err := run.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	return &Cloud{Service: svc}
}
func jobFixture(p Plan, stage string) *run.GoogleCloudRunV2Job {
	image, memory, timeout, account := bounds(p, stage)
	env := []*run.GoogleCloudRunV2EnvVar{{Name: "GOOGLE_CLOUD_PROJECT", Value: analysis.Project}}
	if stage == "analysis" {
		env = append(env, &run.GoogleCloudRunV2EnvVar{Name: "GOOGLE_CLOUD_LOCATION", Value: analysis.Location}, &run.GoogleCloudRunV2EnvVar{Name: "IMAGE_DIGEST", Value: image}, &run.GoogleCloudRunV2EnvVar{Name: "SOURCE_SHA256", Value: strings.Repeat("a", 64)}, &run.GoogleCloudRunV2EnvVar{Name: "SOURCE_REVISION", Value: strings.Repeat("b", 40)})
	}
	return &run.GoogleCloudRunV2Job{Name: p.Job(stage), Etag: "approved-etag", Template: &run.GoogleCloudRunV2ExecutionTemplate{TaskCount: 1, Parallelism: 1, Template: &run.GoogleCloudRunV2TaskTemplate{ServiceAccount: account + "@" + analysis.Project + ".iam.gserviceaccount.com", Timeout: timeout, Containers: []*run.GoogleCloudRunV2Container{{Name: "worker", Image: image, Env: env, Resources: &run.GoogleCloudRunV2ResourceRequirements{Limits: map[string]string{"cpu": "1", "memory": memory}}}}}}}
}
func notifierFixture(p Plan) *run.GoogleCloudRunV2Service {
	return &run.GoogleCloudRunV2Service{Name: Notifier, LatestReadyRevision: p.NotifierRevision, LatestCreatedRevision: p.NotifierRevision, TrafficStatuses: []*run.GoogleCloudRunV2TrafficTargetStatus{{Revision: strings.TrimPrefix(p.NotifierRevision, Notifier+"/revisions/"), Percent: 100}}, Scaling: &run.GoogleCloudRunV2ServiceScaling{MaxInstanceCount: 1}, Template: &run.GoogleCloudRunV2RevisionTemplate{ServiceAccount: "poc-gemma-notifier" + "@" + analysis.Project + ".iam.gserviceaccount.com", MaxInstanceRequestConcurrency: 1, Timeout: "30s", Containers: []*run.GoogleCloudRunV2Container{{Image: p.MessagingImage, Env: []*run.GoogleCloudRunV2EnvVar{{Name: "RESEND_API_KEY", ValueSource: &run.GoogleCloudRunV2EnvVarSource{SecretKeyRef: &run.GoogleCloudRunV2SecretKeySelector{Secret: "poc-gemma-resend-api-key", Version: p.SecretVersion}}}}}}}}
}
func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func TestSDKLaunchIsSingleMutationWithBoundedOverrides(t *testing.T) {
	p := example(t)
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged", true: "unavailable"}[fail], func(t *testing.T) {
			posts := 0
			var request run.GoogleCloudRunV2RunJobRequest
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					reply(w, jobFixture(p, "index"))
					return
				}
				posts++
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if fail {
					w.WriteHeader(http.StatusServiceUnavailable)
					reply(w, map[string]any{"error": map[string]any{"code": 503, "message": "unavailable"}})
					return
				}
				reply(w, &run.GoogleLongrunningOperation{Name: Root + "/operations/launch-test"})
			})
			h, err := c.Start(context.Background(), p, "index")
			if (err != nil) != fail || (!fail && h.Operation == "") {
				t.Fatalf("unexpected acknowledgment: %+v %v", h, err)
			}
			if posts != 1 {
				t.Fatalf("SDK retried mutation: %d", posts)
			}
			if request.Etag != "approved-etag" || request.Overrides == nil || request.Overrides.TaskCount != 1 || request.Overrides.Timeout != "300s" || len(request.Overrides.ContainerOverrides) != 1 || !reflect.DeepEqual(request.Overrides.ContainerOverrides[0].Args, p.Args("index")) {
				t.Fatalf("unsafe overrides: %+v", request)
			}
		})
	}
}
func TestCPUConfigurationGuardsBeforeMutation(t *testing.T) {
	p := example(t)
	cases := map[string]func(*run.GoogleCloudRunV2Job){
		"gpu": func(j *run.GoogleCloudRunV2Job) {
			j.Template.Template.Containers[0].Resources.Limits["nvidia.com/gpu"] = "1"
		},
		"mutable image":  func(j *run.GoogleCloudRunV2Job) { j.Template.Template.Containers[0].Image = "mutable:latest" },
		"retries":        func(j *run.GoogleCloudRunV2Job) { j.Template.Template.MaxRetries = 1 },
		"task count":     func(j *run.GoogleCloudRunV2Job) { j.Template.TaskCount = 2 },
		"timeout":        func(j *run.GoogleCloudRunV2Job) { j.Template.Template.Timeout = "3600s" },
		"wrong identity": func(j *run.GoogleCloudRunV2Job) { j.Name = p.Job("analysis") },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			j := jobFixture(p, "index")
			change(j)
			posts := 0
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
				}
				reply(w, j)
			})
			if _, err := c.Start(context.Background(), p, "index"); err == nil || posts != 0 {
				t.Fatal("unsafe job reached mutation")
			}
		})
	}
}
func TestNotifierPinAndPrivateFieldSelection(t *testing.T) {
	p := example(t)
	for _, changed := range []string{"", "revision", "secret", "traffic", "reconciling"} {
		t.Run("pin-"+changed, func(t *testing.T) {
			svc := notifierFixture(p)
			if changed == "revision" {
				svc.LatestReadyRevision += "-changed"
			}
			if changed == "traffic" {
				svc.TrafficStatuses[0].Revision += "-other"
			}
			if changed == "reconciling" {
				svc.Reconciling = true
			}
			if changed == "secret" {
				svc.Template.Containers[0].Env[0].ValueSource.SecretKeyRef.Version = "latest"
			}
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				fields := r.URL.Query().Get("fields")
				if strings.Contains(fields, "env(name,value)") || !strings.Contains(fields, "env(name,valueSource)") {
					t.Error("private field selection missing")
				}
				reply(w, svc)
			})
			if err := c.notifier(context.Background(), p); (err != nil) != (changed != "") {
				t.Fatalf("pin result: %v", err)
			}
		})
	}
}
func TestWaitCheckpointsIdentityAndConfirmsTerminalResult(t *testing.T) {
	p := example(t)
	for _, retried := range []int64{0, 1} {
		t.Run(map[int64]string{0: "success", 1: "unexpected retry"}[retried], func(t *testing.T) {
			h := Handle{Operation: Root + "/operations/wait-test"}
			execution := p.Job("index") + "/executions/test-index"
			reads := 0
			saved := Handle{}
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/operations/") {
					if r.URL.Query().Get("fields") != "name,done,error(code),metadata,response" {
						t.Error("unsupported nested projection of protobuf Any")
					}
					reply(w, &run.GoogleLongrunningOperation{Name: h.Operation, Done: true, Metadata: googleapi.RawMessage(`{"name":"` + execution + `"}`)})
					return
				}
				reads++
				task := jobFixture(p, "index").Template.Template
				task.Containers[0].Args = p.Args("index")
				reply(w, &run.GoogleCloudRunV2Execution{Name: execution, TaskCount: 1, Template: task, CompletionTime: "2026-10-02T12:00:00Z", SucceededCount: 1, RetriedCount: retried})
			})
			err := c.Wait(context.Background(), p, "index", h, func(h Handle) error { saved = h; return nil })
			if (err != nil) != (retried != 0) || reads != 2 || saved.Execution != execution {
				t.Fatalf("terminal confirmation: %d %+v %v", reads, saved, err)
			}
		})
	}
}

func TestGPUDeadlineCancelsKnownExecutionWithoutRelaunch(t *testing.T) {
	for _, count := range []int64{1, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			p := fullPlan(t)
			if count == 3 {
				p = batchPlan(t)
			}
			execution := p.Job("worker-a") + "/executions/gpu-test"
			h := Handle{Execution: execution, RequestedAt: time.Now().Add(-14*time.Minute - 10*time.Second).UTC().Format(time.RFC3339Nano)}
			cancels := 0
			launches := 0
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, ":cancel") {
					cancels++
					reply(w, &run.GoogleLongrunningOperation{Name: Root + "/operations/cancel-test"})
					return
				}
				if r.Method == http.MethodPost {
					launches++
				}
				if cancels == 0 {
					reply(w, &run.GoogleCloudRunV2Execution{Name: execution, RunningCount: count})
					return
				}
				reply(w, &run.GoogleCloudRunV2Execution{Name: execution, CompletionTime: "2026-10-02T12:00:00Z", CancelledCount: count})
			})
			if err := c.Wait(context.Background(), p, "worker-a", h, func(Handle) error { return nil }); err == nil {
				t.Fatal("cancelled execution marked successful")
			}
			if cancels != 1 || launches != 0 {
				t.Fatalf("unsafe deadline recovery: cancel=%d launch=%d", cancels, launches)
			}

		})
	}
}

func TestGPUTransientObservationResumesSameExecution(t *testing.T) {
	for _, phase := range []string{"operation", "execution", "confirmation", "truncated"} {
		t.Run(phase, func(t *testing.T) {
			p := fullPlan(t)
			execution := p.Job("worker-a") + "/executions/gpu-test"
			h := Handle{Execution: execution, RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			if phase == "operation" {
				h.Execution = ""
				h.Operation = Root + "/operations/gpu-test"
			}
			mutations, reads, failures := 0, 0, 0
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations++
				}
				isOperation := strings.Contains(r.URL.Path, "/operations/")
				if !isOperation {
					reads++
				}
				fail := failures == 0 && ((phase == "operation" && isOperation) || ((phase == "execution" || phase == "truncated") && !isOperation) || (phase == "confirmation" && reads == 2))
				if fail {
					failures++
					if phase == "truncated" {
						w.Header().Set("Content-Length", "999")
						_, _ = w.Write([]byte("{"))
						return
					}
					w.WriteHeader(http.StatusServiceUnavailable)
					reply(w, map[string]any{"error": map[string]any{"code": 503, "message": "private provider detail"}})
					return
				}
				if isOperation {
					reply(w, &run.GoogleLongrunningOperation{Name: h.Operation, Metadata: googleapi.RawMessage(analysis.JSON(map[string]string{"name": execution}))})
					return
				}
				reply(w, &run.GoogleCloudRunV2Execution{Name: execution, TaskCount: 1, SucceededCount: 1, CompletionTime: "2026-10-03T12:00:00Z", Template: gpuJobFixture(p).Template.Template})
			})
			c.PollInterval = time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := c.Wait(ctx, p, "worker-a", h, func(next Handle) error {
				if next.Execution != execution || next.RequestedAt != h.RequestedAt {
					t.Error("recovery changed execution or absolute launch time")
				}
				return nil
			})
			if err != nil || failures != 1 || reads < 2 || mutations != 0 {
				t.Fatalf("unsafe read recovery: err=%v failures=%d reads=%d mutations=%d", err, failures, reads, mutations)
			}
		})
	}
}

func TestGPUObservationOutageStillCancelsAtAbsoluteGuard(t *testing.T) {
	p := fullPlan(t)
	execution := p.Job("worker-a") + "/executions/gpu-test"
	started := time.Now().Add(-14*time.Minute + 80*time.Millisecond)
	h := Handle{Execution: execution, RequestedAt: started.UTC().Format(time.RFC3339Nano)}
	cancels, launches := 0, 0
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ":cancel") {
			cancels++
			if time.Now().Before(started.Add(14 * time.Minute)) {
				t.Error("transient outage cancelled GPU before absolute guard")
			}
			// Lost acknowledgment must still reconcile terminal status, never POST twice.
			w.WriteHeader(http.StatusServiceUnavailable)
			reply(w, map[string]any{"error": map[string]any{"code": 503, "message": "private cancellation detail"}})
			return
		}
		if r.Method == http.MethodPost {
			launches++
		}
		if cancels == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			reply(w, map[string]any{"error": map[string]any{"code": 429}})
			return
		}
		reply(w, &run.GoogleCloudRunV2Execution{Name: execution, CompletionTime: "2026-10-03T12:00:00Z", CancelledCount: 1})
	})
	c.PollInterval = time.Millisecond
	if err := c.Wait(context.Background(), p, "worker-a", h, func(Handle) error { return nil }); err == nil || cancels != 1 || launches != 0 {
		t.Fatalf("unsafe outage deadline: error=%v cancel=%d launch=%d", err, cancels, launches)
	}
}

func TestGPUForeignExecutionNeverReadOrCancelled(t *testing.T) {
	p := fullPlan(t)
	requests := 0
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(500) })
	h := Handle{Execution: p.Job("index") + "/executions/foreign", RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := c.Wait(context.Background(), p, "worker-a", h, func(Handle) error { return nil }); err == nil || requests != 0 {
		t.Fatal("foreign execution was inspected or mutated")
	}
}

func TestGPUIdentityDriftStillCancelsWithoutRelaunch(t *testing.T) {
	p := fullPlan(t)
	execution := p.Job("worker-a") + "/executions/gpu-test"
	cancels, launches := 0, 0
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ":cancel") {
			cancels++
			reply(w, &run.GoogleLongrunningOperation{Name: Root + "/operations/cancel-test"})
			return
		}
		if r.Method == http.MethodPost {
			launches++
		}
		if cancels != 0 {
			reply(w, &run.GoogleCloudRunV2Execution{Name: execution, CompletionTime: "2026-10-03T12:00:00Z", CancelledCount: 1})
			return
		}
		task := gpuJobFixture(p).Template.Template
		task.Containers[0].Image = "unapproved-image"
		reply(w, &run.GoogleCloudRunV2Execution{Name: execution, TaskCount: 1, RunningCount: 1, Template: task})
	})
	h := Handle{Execution: execution, RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := c.Wait(context.Background(), p, "worker-a", h, func(Handle) error { return nil }); err == nil || cancels != 1 || launches != 0 {
		t.Fatalf("unsafe drift cleanup: error=%v cancel=%d launch=%d", err, cancels, launches)
	}
}

func gpuJobFixture(p Plan) *run.GoogleCloudRunV2Job {
	j := jobFixture(p, "worker-a")
	task := j.Template.Template
	task.NodeSelector = &run.GoogleCloudRunV2NodeSelector{Accelerator: "nvidia-rtx-pro-6000"}
	task.GpuZonalRedundancyDisabled = true
	container := task.Containers[0]
	container.Resources.Limits = map[string]string{"cpu": "20", "memory": "80Gi", "nvidia.com/gpu": "1"}
	values := map[string]string{"MANIFEST_URI": p.Request.Manifest.URI, "RESULTS_PREFIX": p.WorkerPrefix(), "SOFT_DEADLINE_SECONDS": "600", "TASK_TIMEOUT_SECONDS": "900", "REQUIRE_COMPILE_CACHE": "1", "EXPORT_COMPILE_CACHE": "0", "GPU_PROFILE": "rtx6000", "GIT_SHA": p.WorkerA.GitSHA, "IMAGE_DIGEST": p.WorkerA.Image, "MODEL_MANIFEST_URI": "gs://your-gcp-project-id-models/diffusiongemma/26b-a4b-it/rtx6000/model-manifest.json"}
	container.Env = nil
	for k, v := range values {
		container.Env = append(container.Env, &run.GoogleCloudRunV2EnvVar{Name: k, Value: v})
	}
	return j
}
func TestGPUCheckedSingleLaunchHasNoOverrides(t *testing.T) {
	p := fullPlan(t)
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "approved", true: "cache disabled"}[invalid], func(t *testing.T) {
			j := gpuJobFixture(p)
			if invalid {
				for _, v := range j.Template.Template.Containers[0].Env {
					if v.Name == "REQUIRE_COMPILE_CACHE" {
						v.Value = "0"
					}
				}
			}
			posts := 0
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					reply(w, j)
					return
				}
				posts++
				var request run.GoogleCloudRunV2RunJobRequest
				if json.NewDecoder(r.Body).Decode(&request) != nil || request.Etag != j.Etag || request.Overrides != nil {
					t.Error("GPU launch changed bounded task configuration")
				}
				reply(w, &run.GoogleLongrunningOperation{Name: Root + "/operations/gpu-launch"})
			})
			if _, err := c.Start(context.Background(), p, "worker-a"); (err != nil) != invalid || posts != map[bool]int{false: 1, true: 0}[invalid] {
				t.Fatal("GPU configuration gate failed")
			}
		})
	}
}
func TestNewGPUOutputsResolveOnlyAfterValidatedCompletion(t *testing.T) {
	p := fullPlan(t)
	s := &artifactStore{files: map[string][]byte{}}
	refs := []*analysis.ObjectRef{&p.Request.Manifest, &p.Request.Shard, p.Request.Truth}
	for _, ref := range refs {
		raw := []byte(ref.URI)
		ref.SHA256 = analysis.Digest(raw)
		s.files[ref.URI] = raw
	}
	c := Cloud{Store: s}
	if _, ready, err := c.ResolveSource(context.Background(), p); err != nil || ready {
		t.Fatal("incomplete GPU source accepted")
	}
	marker := benchmark.Marker{Status: "success", RunID: "pilot-004", GPUProfile: "rtx6000", ImageDigest: p.WorkerA.Image, ManifestSHA256: p.Request.Manifest.SHA256, RecordsTotal: 300, RecordsProcessed: 300, PromptVersion: "worker-a-v2", PromptSHA256: "17c5f97f389ab084f37498e8800763a9537dbc7ef39c0394edfeacc3b65254aa", ModelRevision: "c333706ed87619f80159b1f0c5685b71dfafeea8"}
	uri := p.WorkerTaskPrefix() + "/_SUCCESS.json"
	s.files[uri] = analysis.JSON(marker)
	chunkURI := p.WorkerTaskPrefix() + "/chunk-000000.parquet"
	s.files[chunkURI] = []byte("checksum-bound fixture")
	request, ready, err := c.ResolveSource(context.Background(), p)
	if err != nil || !ready || request.Marker.URI != uri || request.Marker.SHA256 != analysis.Digest(s.files[uri]) || len(request.Chunks) != 1 || request.Chunks[0].SHA256 != analysis.Digest(s.files[chunkURI]) {
		t.Fatalf("fresh source not bound: ready=%v err=%v", ready, err)
	}
	marker.RecordsProcessed = 299
	s.files[uri] = analysis.JSON(marker)
	if _, _, err := c.ResolveSource(context.Background(), p); err == nil {
		t.Fatal("partial output accepted as complete")
	}
}

func TestExecutionAcceptsVerifiedPlatformManifestOnly(t *testing.T) {
	p := fullPlan(t)
	task := gpuJobFixture(p).Template.Template
	task.Containers[0].Image = CachedWorkerPlatformImage
	if !executionTaskValid(p, "worker-a", task) {
		t.Fatal("verified amd64 manifest rejected")
	}
	if taskValid(p, "worker-a", task) {
		t.Fatal("job preflight accepted a changed parent reference")
	}
	task.Containers[0].Image = "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/worker-a@sha256:" + strings.Repeat("e", 64)
	if executionTaskValid(p, "worker-a", task) {
		t.Fatal("unrelated platform manifest accepted")
	}
}

func TestCancelledGPURecoveryRequiresCompleteSourceWithoutMutation(t *testing.T) {
	for _, processed := range []int{150, 300} {
		t.Run(map[int]string{150: "partial", 300: "complete"}[processed], func(t *testing.T) {
			p := fullPlan(t)
			execution := p.Job("worker-a") + "/executions/gpu-test"
			s := &artifactStore{files: map[string][]byte{}}
			for _, ref := range []*analysis.ObjectRef{&p.Request.Manifest, &p.Request.Shard, p.Request.Truth} {
				raw := []byte(ref.URI)
				ref.SHA256 = analysis.Digest(raw)
				s.files[ref.URI] = raw
			}
			marker := benchmark.Marker{Status: "success", RunID: "pilot-004", GPUProfile: "rtx6000", ImageDigest: p.WorkerA.Image, ManifestSHA256: p.Request.Manifest.SHA256, RecordsTotal: 300, RecordsProcessed: processed, PromptVersion: "worker-a-v2", PromptSHA256: "17c5f97f389ab084f37498e8800763a9537dbc7ef39c0394edfeacc3b65254aa", ModelRevision: "c333706ed87619f80159b1f0c5685b71dfafeea8"}
			s.files[p.WorkerTaskPrefix()+"/_SUCCESS.json"] = analysis.JSON(marker)
			s.files[p.WorkerTaskPrefix()+"/chunk-000000.parquet"] = []byte("checksum-bound fixture")
			mutations := 0
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations++
				}
				reply(w, &run.GoogleCloudRunV2Execution{Name: execution, CompletionTime: "2026-10-02T12:00:00Z", CancelledCount: 1, TaskCount: 1, Template: gpuJobFixture(p).Template.Template})
			})
			c.Store = s
			h := Handle{Execution: execution, RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			recovered := false
			err := c.Wait(context.Background(), p, "worker-a", h, func(next Handle) error { recovered = next.SourceRecovered; return nil })
			if (err == nil) != (processed == 300) || recovered != (processed == 300) || mutations != 0 {
				t.Fatalf("unsafe terminal recovery: error=%v recovered=%v mutations=%d", err, recovered, mutations)
			}
		})
	}
}

func TestFreshPreparedRunResolvesCompletionWithoutRetainedFallback(t *testing.T) {
	p := fullPlan(t)
	runID := "pipeline-fresh-20261003-01"
	p.Request.SourceRunID = runID
	for _, r := range []*analysis.ObjectRef{&p.Request.Manifest, &p.Request.Shard, p.Request.Truth, &p.Request.Marker} {
		r.URI = strings.ReplaceAll(r.URI, "pilot-004", runID)
	}
	p.Request.Marker.URI = p.WorkerTaskPrefix() + "/_SUCCESS.json"
	p.Request.Chunks = []analysis.ObjectRef{{URI: p.WorkerTaskPrefix() + "/chunk-000000.parquet", SHA256: strings.Repeat("a", 64)}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	s := &artifactStore{files: map[string][]byte{}}
	for _, r := range []*analysis.ObjectRef{&p.Request.Manifest, &p.Request.Shard, p.Request.Truth} {
		raw := []byte(r.URI)
		r.SHA256 = analysis.Digest(raw)
		s.files[r.URI] = raw
	}
	marker := benchmark.Marker{Status: "success", RunID: runID, GPUProfile: "rtx6000", ImageDigest: p.WorkerA.Image, ManifestSHA256: p.Request.Manifest.SHA256, RecordsTotal: 300, RecordsProcessed: 300, PromptVersion: "worker-a-v2", PromptSHA256: "17c5f97f389ab084f37498e8800763a9537dbc7ef39c0394edfeacc3b65254aa", ModelRevision: "c333706ed87619f80159b1f0c5685b71dfafeea8"}
	s.files[p.WorkerTaskPrefix()+"/_SUCCESS.json"] = analysis.JSON(marker)
	s.files[p.WorkerTaskPrefix()+"/chunk-000000.parquet"] = []byte("fresh fixture")
	c := Cloud{Store: s}
	if _, ready, err := c.ResolveSource(context.Background(), p); err != nil || !ready {
		t.Fatal("fresh completion rejected", err)
	}
	marker.RunID = "pilot-004"
	s.files[p.WorkerTaskPrefix()+"/_SUCCESS.json"] = analysis.JSON(marker)
	if _, _, err := c.ResolveSource(context.Background(), p); err == nil {
		t.Fatal("retained completion accepted as fresh")
	}
}

func batchPlan(t *testing.T) Plan {
	p := fullPlan(t)
	p.Request.SchemaVersion = analysis.BatchSchemaVersion
	p.Request.SourceRunID = "pipeline-fresh-batch"
	for _, r := range []*analysis.ObjectRef{&p.Request.Manifest, &p.Request.Shard, p.Request.Truth} {
		r.URI = strings.ReplaceAll(r.URI, "pilot-004", p.Request.SourceRunID)
	}
	p.Request.Marker.URI = p.WorkerTask(0) + "/_SUCCESS.json"
	p.Request.Chunks = []analysis.ObjectRef{{URI: p.WorkerTask(0) + "/chunk-000000.parquet", SHA256: strings.Repeat("a", 64)}}
	for i := 1; i < 3; i++ {
		p.Request.AdditionalTasks = append(p.Request.AdditionalTasks, analysis.TaskSource{TaskIndex: i, Shard: analysis.ObjectRef{URI: strings.ReplaceAll(p.Request.Shard.URI, "00000", fmt.Sprintf("%05d", i)), SHA256: strings.Repeat("a", 64)}, Marker: analysis.ObjectRef{URI: p.WorkerTask(i) + "/_SUCCESS.json", SHA256: strings.Repeat("a", 64)}, Chunks: []analysis.ObjectRef{{URI: p.WorkerTask(i) + "/chunk-000000.parquet", SHA256: strings.Repeat("a", 64)}}})
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestThreeTaskGPUPreflightAndTerminalConfirmation(t *testing.T) {
	p := batchPlan(t)
	j := gpuJobFixture(p)
	j.Template.TaskCount = 3
	j.Template.Parallelism = 3
	posts := 0
	execution := p.Job("worker-a") + "/executions/batch-test"
	c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ":run") {
			posts++
			reply(w, &run.GoogleLongrunningOperation{Name: Root + "/operations/batch"})
			return
		}
		if strings.Contains(r.URL.Path, "/executions/") {
			reply(w, &run.GoogleCloudRunV2Execution{Name: execution, TaskCount: 3, SucceededCount: 3, CompletionTime: "2026-10-03T12:00:00Z", Template: j.Template.Template})
			return
		}
		reply(w, j)
	})
	if _, err := c.Start(context.Background(), p, "worker-a"); err != nil || posts != 1 {
		t.Fatal("bounded three-task launch rejected", err)
	}
	if err := c.Wait(context.Background(), p, "worker-a", Handle{Execution: execution, RequestedAt: time.Now().UTC().Format(time.RFC3339Nano)}, func(Handle) error { return nil }); err != nil {
		t.Fatal("three-task success rejected", err)
	}
	j.Template.Parallelism = 1
	if _, err := c.Start(context.Background(), p, "worker-a"); err == nil || posts != 1 {
		t.Fatal("changed concurrency launched")
	}
}

func TestBatchSourceRequiresAllThreePinnedCompletions(t *testing.T) {
	p := batchPlan(t)
	s := &artifactStore{files: map[string][]byte{}}
	for _, ref := range []*analysis.ObjectRef{&p.Request.Manifest, &p.Request.Shard, p.Request.Truth, &p.Request.AdditionalTasks[0].Shard, &p.Request.AdditionalTasks[1].Shard} {
		raw := []byte(ref.URI)
		ref.SHA256 = analysis.Digest(raw)
		s.files[ref.URI] = raw
	}
	for i := range 3 {
		marker := benchmark.Marker{Status: "success", RunID: p.Request.SourceRunID, TaskIndex: i, GPUProfile: "rtx6000", ImageDigest: p.WorkerA.Image, ManifestSHA256: p.Request.Manifest.SHA256, RecordsTotal: 300, RecordsProcessed: 300, PromptVersion: "worker-a-v2", PromptSHA256: "17c5f97f389ab084f37498e8800763a9537dbc7ef39c0394edfeacc3b65254aa", ModelRevision: "c333706ed87619f80159b1f0c5685b71dfafeea8"}
		s.files[p.WorkerTask(i)+"/_SUCCESS.json"] = analysis.JSON(marker)
		s.files[p.WorkerTask(i)+"/chunk-000000.parquet"] = []byte("batch fixture")
	}
	c := Cloud{Store: s}
	r, ready, err := c.ResolveSource(context.Background(), p)
	if err != nil || !ready || r.TaskCount() != 3 {
		t.Fatal("batch source rejected", err)
	}
	delete(s.files, p.WorkerTask(2)+"/_SUCCESS.json")
	if _, ready, err := c.ResolveSource(context.Background(), p); err != nil || ready {
		t.Fatal("two tasks passed as full batch")
	}
}
