package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"time"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"google.golang.org/api/googleapi"
	run "google.golang.org/api/run/v2"
)

const CachedWorkerImage = "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/worker-a@sha256:581ba5a7ec8e3061f22306ead9c43a5315ef0048eea601ea5e1afddda64c1312"
const CachedWorkerPlatformImage = "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/worker-a@sha256:702ac9ac33246ef946373e6c1f328aa9c09233ab4bdd62d8f902c99e7175c426"

const CachedWorkerRevision = "2c73d83521727c4a73185c71638347330f4964e7"

type SourceBackend interface {
	ResolveSource(context.Context, Plan) (*analysis.Request, bool, error)
}

func (p Plan) WorkerPrefix() string {
	if p.WorkerA == nil {
		return ""
	}
	return "gs://your-gcp-project-id-results/canary/" + p.WorkerA.CanaryID
}
func (p Plan) WorkerTaskPrefix() string { return p.WorkerTask(0) }
func (p Plan) WorkerTask(index int) string {
	return p.WorkerPrefix() + "/runs/" + p.Request.SourceRunID + fmt.Sprintf("/worker-a/rtx6000/task-%05d", index)
}

// ResolveSource reads named objects only. The completion pins a fresh source;
// the eventual Worker B request records each observed object's checksum.
func (c *Cloud) ResolveSource(ctx context.Context, p Plan) (*analysis.Request, bool, error) {
	if p.WorkerA == nil || p.Request == nil {
		return nil, false, errors.New("Worker A plan required")
	}
	refs := []analysis.ObjectRef{p.Request.Manifest, *p.Request.Truth}
	for _, task := range p.Request.Sources() {
		refs = append(refs, task.Shard)
	}
	for _, ref := range refs {
		raw, err := c.Store.Read(ctx, ref.URI, analysis.MaxObjectBytes)
		if err != nil || analysis.Digest(raw) != ref.SHA256 {
			return nil, false, errors.New("prepared source checksum mismatch")
		}
	}
	request := *p.Request
	request.AdditionalTasks = append([]analysis.TaskSource(nil), p.Request.AdditionalTasks...)
	for i, task := range p.Request.Sources() {
		uri := p.WorkerTask(task.TaskIndex) + "/_SUCCESS.json"
		raw, err := c.Store.Read(ctx, uri, 1<<20)
		if errors.Is(err, analysis.ErrMissing) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, errors.New("Worker A completion unavailable")
		}
		var marker benchmark.Marker
		if json.Unmarshal(raw, &marker) != nil || marker.Status != "success" || marker.RunID != p.Request.SourceRunID || marker.TaskIndex != task.TaskIndex || marker.GPUProfile != "rtx6000" || marker.ImageDigest != p.WorkerA.Image || marker.ManifestSHA256 != p.Request.Manifest.SHA256 || marker.RecordsTotal != 300 || marker.RecordsProcessed != 300 || marker.PromptVersion != "worker-a-v2" || marker.PromptSHA256 != "17c5f97f389ab084f37498e8800763a9537dbc7ef39c0394edfeacc3b65254aa" || marker.ModelRevision != "c333706ed87619f80159b1f0c5685b71dfafeea8" {
			return nil, false, errors.New("Worker A completion identity mismatch")
		}
		task.Marker = analysis.ObjectRef{URI: uri, SHA256: analysis.Digest(raw)}
		task.Chunks = nil
		for index := range 17 {
			uri := fmt.Sprintf("%s/chunk-%06d.parquet", p.WorkerTask(task.TaskIndex), index)
			raw, err := c.Store.Read(ctx, uri, analysis.MaxObjectBytes)
			if errors.Is(err, analysis.ErrMissing) {
				break
			}
			if err != nil || index == 16 {
				return nil, false, errors.New("Worker A chunk bound or read failed")
			}
			task.Chunks = append(task.Chunks, analysis.ObjectRef{URI: uri, SHA256: analysis.Digest(raw)})
		}

		if i == 0 {
			request.Marker = task.Marker
			request.Chunks = task.Chunks
		} else {
			request.AdditionalTasks[i-1] = task
		}
	}
	if request.Validate(true) != nil {
		return nil, false, errors.New("resolved Worker B source invalid")
	}
	return &request, true, nil
}

func (c *Cloud) waitWorkerA(ctx context.Context, p Plan, h Handle, save func(Handle) error) error {
	if h.Execution != "" && !executionValid(p.Job("worker-a"), h.Execution) {
		return errors.New("GPU execution identity invalid; no remote mutation")
	}
	started, err := time.Parse(time.RFC3339Nano, h.RequestedAt)
	if err != nil || started.After(time.Now().Add(time.Second)) {
		return errors.New("GPU launch timestamp invalid")
	}
	guard, cancel := context.WithDeadline(ctx, started.Add(14*time.Minute))
	defer cancel()
	err = c.waitExecution(guard, p, "worker-a", h, func(next Handle) error { h = next; return save(next) })
	if err == nil {
		return nil
	}
	// An interrupted observation must not leave a known GPU execution unmonitored.
	if h.Execution == "" {
		return errors.New("GPU identity unknown; reconcile immediately without relaunch")
	}
	cleanupDeadline := started.Add(15 * time.Minute)
	// A resumed expired budget still permits emergency cancellation, never launch.
	if cleanupDeadline.Before(time.Now()) {
		cleanupDeadline = time.Now().Add(time.Minute)
	}
	cleanup, stop := context.WithDeadline(context.Background(), cleanupDeadline)
	defer stop()
	get := func() (*run.GoogleCloudRunV2Execution, error) {
		read, done := context.WithTimeout(cleanup, 10*time.Second)
		defer done()
		return c.Service.Projects.Locations.Jobs.Executions.Get(h.Execution).Fields("name,createTime,startTime,completionTime,runningCount,succeededCount,failedCount,cancelledCount,retriedCount").Context(read).Do()
	}
	terminal := func(e *run.GoogleCloudRunV2Execution) bool {
		return e != nil && e.Name == h.Execution && e.CompletionTime != "" && e.RunningCount == 0 && e.SucceededCount+e.FailedCount+e.CancelledCount == p.TaskCount("worker-a")
	}
	var current *run.GoogleCloudRunV2Execution
	var readErr error
	// At the guard deadline, cancellation must not wait behind another GET.
	if time.Now().Before(started.Add(14 * time.Minute)) {
		current, readErr = get()
	}
	if readErr == nil && terminal(current) {
		telemetry.Lifecycle(ctx, "worker-a", current.Name, current.CreateTime, current.StartTime, current.CompletionTime, true)
		// Reconcile a terminal platform failure only against a complete, pinned
		// source. This never hides the execution outcome or starts another GPU.
		fields := "name,createTime,startTime,completionTime,runningCount,succeededCount,failedCount,cancelledCount,retriedCount,taskCount,template(serviceAccount,maxRetries,timeout,nodeSelector,gpuZonalRedundancyDisabled,containers(image,args,command,resources))"
		verify := func() bool {
			v, e := c.Service.Projects.Locations.Jobs.Executions.Get(h.Execution).Fields(googleapi.Field(fields)).Context(cleanup).Do()
			return e == nil && terminal(v) && v.RetriedCount == 0 && v.TaskCount == p.TaskCount("worker-a") && executionTaskValid(p, "worker-a", v.Template)
		}
		if verify() && verify() {
			_, ready, e := c.ResolveSource(cleanup, p)
			if e == nil && ready {
				h.SourceRecovered = true
				if e = save(h); e != nil {
					return e
				}
				return nil
			}
		}
		return errors.New("GPU terminated unsuccessfully; preserve artifacts, never retry")
	}
	// Exactly one cancellation request; this is not a new execution or retry.
	cancelRead, cancelDone := context.WithTimeout(cleanup, 10*time.Second)
	_, _ = c.Service.Projects.Locations.Jobs.Executions.Cancel(h.Execution, &run.GoogleCloudRunV2CancelExecutionRequest{}).Context(cancelRead).Do()
	cancelDone()
	interval := c.PollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		current, e := get()
		if e == nil && terminal(current) {
			confirmed, e := get()
			if e == nil && terminal(confirmed) {
				telemetry.Lifecycle(ctx, "worker-a", confirmed.Name, confirmed.CreateTime, confirmed.StartTime, confirmed.CompletionTime, true)
				return errors.New("GPU cancellation/termination confirmed; never retry")
			}
		}
		if pause(cleanup, interval) != nil {
			return errors.New("GPU termination unconfirmed; operator inspection required")
		}
	}
}
