package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"io"
	"net"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analytics"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/messaging"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	run "google.golang.org/api/run/v2"
)

type Cloud struct {
	Service      *run.Service
	Store        analysis.Store
	PollInterval time.Duration
}

func NewCloud(ctx context.Context, store analysis.Store) (*Cloud, error) {
	service, err := run.NewService(ctx, option.WithQuotaProject(analysis.Project))
	if err != nil {
		return nil, errors.New("operator ADC initialization failed")
	}
	return &Cloud{Service: service, Store: store, PollInterval: 5 * time.Second}, nil
}
func operationValid(name string) bool {
	return regexp.MustCompile(`^projects/your-gcp-project-id/locations/us-central1/operations/[a-zA-Z0-9-]+$`).MatchString(name)
}
func executionValid(job, name string) bool {
	return regexp.MustCompile(`^` + regexp.QuoteMeta(job) + `/executions/[a-z0-9-]+$`).MatchString(name)
}
func bounds(p Plan, stage string) (string, string, string, string) {
	switch stage {
	case "prepare":
		if p.Preparation != nil {
			return p.Preparation.Image, "1Gi", "1200s", "poc-gemma-preparer"
		}
	case "worker-a":
		if p.WorkerA != nil {
			return p.WorkerA.Image, "80Gi", "900s", "poc-gemma-worker-a"
		}
	case "analysis":
		return p.AnalysisImage, "1Gi", "600s", "poc-gemma-analysis"
	case "index":
		return p.DataImage, "512Mi", "300s", "poc-gemma-index"
	case "publish":
		return p.MessagingImage, "512Mi", "120s", "poc-gemma-publisher"
	}
	return "", "", "", ""
}
func taskValid(p Plan, stage string, t *run.GoogleCloudRunV2TaskTemplate) bool {
	image, memory, timeout, account := bounds(p, stage)
	if t == nil || t.MaxRetries != 0 || t.Timeout != timeout || t.ServiceAccount != account+"@"+analysis.Project+".iam.gserviceaccount.com" || len(t.Containers) != 1 {
		return false
	}
	c := t.Containers[0]
	if stage == "worker-a" {
		return c != nil && c.Image == image && len(c.Command) == 0 && c.Resources != nil && reflect.DeepEqual(c.Resources.Limits, map[string]string{"cpu": "20", "memory": "80Gi", "nvidia.com/gpu": "1"}) && t.NodeSelector != nil && t.NodeSelector.Accelerator == "nvidia-rtx-pro-6000" && t.GpuZonalRedundancyDisabled
	}
	return c != nil && c.Image == image && len(c.Command) == 0 && c.Resources != nil && reflect.DeepEqual(c.Resources.Limits, map[string]string{"cpu": "1", "memory": memory})
}

// Cloud Run resolves an OCI index to the selected platform manifest in executions.
func executionTaskValid(p Plan, stage string, task *run.GoogleCloudRunV2TaskTemplate) bool {
	if stage == "worker-a" && task != nil && len(task.Containers) == 1 && task.Containers[0] != nil && task.Containers[0].Image == CachedWorkerPlatformImage {
		normalized := *task
		container := *task.Containers[0]
		container.Image = CachedWorkerImage
		normalized.Containers = []*run.GoogleCloudRunV2Container{&container}
		return taskValid(p, stage, &normalized)
	}
	return taskValid(p, stage, task)
}
func (c *Cloud) job(ctx context.Context, p Plan, stage string) (*run.GoogleCloudRunV2Job, error) {
	if p.Job(stage) == "" {
		return nil, errors.New("unsupported CPU stage")
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	j, err := c.Service.Projects.Locations.Jobs.Get(p.Job(stage)).Context(deadline).Do()
	if err != nil || j == nil || j.Name != p.Job(stage) || j.Etag == "" || j.Template == nil || j.Template.TaskCount != p.TaskCount(stage) || j.Template.Parallelism != p.TaskCount(stage) || !taskValid(p, stage, j.Template.Template) {
		return nil, errors.New("CPU job differs from approved bounded configuration")
	}
	env := map[string]string{}
	for _, v := range j.Template.Template.Containers[0].Env {
		env[v.Name] = v.Value
	}
	if stage == "worker-a" {
		if env["MANIFEST_URI"] != p.Request.Manifest.URI || env["RESULTS_PREFIX"] != p.WorkerPrefix() || env["SOFT_DEADLINE_SECONDS"] != "600" || env["TASK_TIMEOUT_SECONDS"] != "900" || env["REQUIRE_COMPILE_CACHE"] != "1" || env["EXPORT_COMPILE_CACHE"] != "0" || env["GPU_PROFILE"] != "rtx6000" || env["GIT_SHA"] != p.WorkerA.GitSHA || env["IMAGE_DIGEST"] != p.WorkerA.Image || env["MODEL_MANIFEST_URI"] != "gs://your-gcp-project-id-models/diffusiongemma/26b-a4b-it/rtx6000/model-manifest.json" {
			return nil, errors.New("Worker A inputs or cost guard differ")
		}
		return j, nil
	}
	if stage == "prepare" {
		q := p.Preparation
		if q == nil || env["SOURCE_URI"] != q.Source.URI || env["METADATA_URI"] != q.Metadata.URI || env["RUN_ID"] != p.Request.SourceRunID || env["GIT_SHA"] != q.GitSHA || env["PREPARED_PREFIX"] != "gs://your-gcp-project-id-prepared/runs" || env["GROUND_TRUTH_PREFIX"] != "gs://your-gcp-project-id-ground-truth/runs" {
			return nil, errors.New("preparation inputs differ from approved workload")
		}
		configured := j.Template.Template.Containers[0].Args
		defaults := []string{"--limit", "900", "--prompt-version", "worker-a-v2"}
		if !reflect.DeepEqual(configured, defaults) && !reflect.DeepEqual(configured, p.Args(stage)) {
			return nil, errors.New("preparation arguments differ from bounded scope")
		}
		return j, nil
	}
	if env["GOOGLE_CLOUD_PROJECT"] != analysis.Project {
		return nil, errors.New("job project configuration differs")
	}
	if stage == "analysis" && (env["GOOGLE_CLOUD_LOCATION"] != analysis.Location || env["IMAGE_DIGEST"] != p.AnalysisImage || !digestPattern.MatchString(env["SOURCE_SHA256"]) || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(env["SOURCE_REVISION"])) {
		return nil, errors.New("analysis provenance differs from approved image")
	}
	return j, nil
}

// Traffic status uses a short revision name while service metadata uses the full name.
func revisionMatches(full, observed string) bool {
	return observed == full || observed == strings.TrimPrefix(full, Notifier+"/revisions/")
}
func (c *Cloud) notifier(ctx context.Context, p Plan) error {
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Exclude env values, creator identity, request payloads and address configuration.
	fields := googleapi.Field("name,latestReadyRevision,latestCreatedRevision,reconciling,trafficStatuses(revision,percent),invokerIamDisabled,scaling,template(serviceAccount,maxInstanceRequestConcurrency,timeout,containers(image,env(name,valueSource)))")
	s, err := c.Service.Projects.Locations.Services.Get(Notifier).Fields(fields).Context(deadline).Do()
	if err != nil || s == nil || s.Name != Notifier || s.LatestReadyRevision != p.NotifierRevision || s.LatestCreatedRevision != p.NotifierRevision || s.Reconciling || len(s.TrafficStatuses) != 1 || s.TrafficStatuses[0] == nil || s.TrafficStatuses[0].Percent != 100 || !revisionMatches(p.NotifierRevision, s.TrafficStatuses[0].Revision) || s.InvokerIamDisabled || s.Scaling == nil || s.Scaling.MinInstanceCount != 0 || s.Scaling.MaxInstanceCount != 1 || s.Template == nil || s.Template.MaxInstanceRequestConcurrency != 1 || s.Template.Timeout != "30s" || s.Template.ServiceAccount != "poc-gemma-notifier@"+analysis.Project+".iam.gserviceaccount.com" || len(s.Template.Containers) != 1 || s.Template.Containers[0].Image != p.MessagingImage {
		return errors.New("notifier differs from approved private revision")
	}
	if p.RequireEmail {
		found := false
		for _, env := range s.Template.Containers[0].Env {
			if env.Name == "RESEND_API_KEY" && env.ValueSource != nil && env.ValueSource.SecretKeyRef != nil {
				ref := env.ValueSource.SecretKeyRef
				found = (ref.Secret == "poc-gemma-resend-api-key" || ref.Secret == "projects/"+analysis.Project+"/secrets/poc-gemma-resend-api-key") && ref.Version == p.SecretVersion
			}
		}
		if !found {
			return errors.New("approved numeric email-secret reference unavailable")
		}
	}
	return nil
}
func (c *Cloud) Preflight(ctx context.Context, p Plan) error {
	if p.Validate() != nil {
		return errors.New("invalid plan")
	}
	if p.Preparation != nil {
		if _, err := c.job(ctx, p, "prepare"); err != nil {
			return err
		}
		if err := c.verifyRaw(ctx, p); err != nil {
			return err
		}
	}
	if p.WorkerA != nil {
		if _, err := c.job(ctx, p, "worker-a"); err != nil {
			return err
		}
	}
	if p.AnalysisMode == "analyze" {
		if _, err := c.job(ctx, p, "analysis"); err != nil {
			return err
		}
	}
	for _, stage := range []string{"index", "publish"} {
		if _, err := c.job(ctx, p, stage); err != nil {
			return err
		}
	}
	return c.notifier(ctx, p)
}
func (c *Cloud) PrepareRequest(ctx context.Context, p Plan) error {
	if p.AnalysisMode != "analyze" || p.Request == nil {
		return errors.New("analysis preparation not authorized by plan")
	}
	return messaging.Immutable(ctx, c.Store, p.RequestURI(), p.Request)
}
func (c *Cloud) Snapshot(ctx context.Context, p Plan) (Snapshot, error) {
	var out Snapshot
	r, hash, err := analytics.Load(ctx, c.Store, analytics.Prefix, p.AnalysisID)
	if errors.Is(err, analysis.ErrMissing) {
		_, e := c.Store.Read(ctx, analytics.Prefix+"/"+p.AnalysisID+"/request.json", 1<<20)
		if e == nil {
			out.ClaimPresent = true
		} else if !errors.Is(e, analysis.ErrMissing) {
			return out, errors.New("analysis claim unavailable")
		}
		return out, nil
	}
	if err != nil {
		return out, errors.New("analysis artifact validation failed")
	}
	if (p.AnalysisMode == "reuse" && hash != p.ExpectedReportSHA256) || (p.AnalysisMode == "analyze" && r.RequestSHA256 != analysis.Digest(analysis.JSON(p.Request))) {
		return out, errors.New("analysis identity differs from approved plan")
	}
	out.Completed = true
	out.ReportSHA256 = hash
	raw, err := c.Store.Read(ctx, analytics.IndexPrefix+"/"+p.AnalysisID+"/index-v1/"+hash+"/_SUCCESS.json", 1<<20)
	if err == nil {
		var receipt analytics.Receipt
		if analysis.Decode(raw, &receipt) != nil || receipt.SchemaVersion != "index-v1" || receipt.AnalysisID != p.AnalysisID || receipt.ReportSHA256 != hash || receipt.Status != "indexed" || receipt.JobID == "" {
			return out, errors.New("invalid index receipt")
		}
		out.Indexed = true
	} else if !errors.Is(err, analysis.ErrMissing) {
		return out, errors.New("index status unavailable")
	}
	engine := messaging.Engine{Store: c.Store, PublicationPrefix: messaging.PublicationPrefix, NotificationPrefix: messaging.NotificationPrefix}
	status := engine.Status(ctx, p.AnalysisID, hash)
	out.Publication = status["publication_status"]
	out.Notification = status["notification_status"]
	out.Email = status["email_status"]
	return out, nil
}
func (c *Cloud) Start(ctx context.Context, p Plan, stage string) (Handle, error) {
	if stage == "prepare" {
		if err := c.verifyRaw(ctx, p); err != nil {
			return Handle{}, err
		}
	}
	j, err := c.job(ctx, p, stage)
	if err != nil {
		return Handle{}, err
	}
	if stage == "publish" {
		if err = c.notifier(ctx, p); err != nil {
			return Handle{}, err
		}
	}
	_, _, timeout, _ := bounds(p, stage)
	req := &run.GoogleCloudRunV2RunJobRequest{Etag: j.Etag, Overrides: &run.GoogleCloudRunV2Overrides{TaskCount: 1, Timeout: timeout, ContainerOverrides: []*run.GoogleCloudRunV2ContainerOverride{{Name: j.Template.Template.Containers[0].Name, Args: p.Args(stage)}}}}
	if stage == "worker-a" {
		req.Overrides = nil
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Exactly one mutation; no SDK WithRetry is configured.
	op, err := c.Service.Projects.Locations.Jobs.Run(p.Job(stage), req).Context(deadline).Do()
	if err != nil || op == nil || !operationValid(op.Name) {
		return Handle{}, errors.New("launch acknowledgment unavailable")
	}
	return Handle{Operation: op.Name}, nil
}
func (c *Cloud) Wait(ctx context.Context, p Plan, stage string, h Handle, save func(Handle) error) error {
	if stage == "worker-a" {
		return c.waitWorkerA(ctx, p, h, save)
	}
	return c.waitExecution(ctx, p, stage, h, save)
}
func (c *Cloud) waitExecution(ctx context.Context, p Plan, stage string, h Handle, save func(Handle) error) error {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		if h.Execution == "" {
			if !operationValid(h.Operation) {
				return errors.New("invalid operation identity")
			}
			readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			op, err := c.Service.Projects.Locations.Operations.Get(h.Operation).Fields("name,done,error(code),metadata,response").Context(readCtx).Do()
			cancel()
			if err != nil && retryObservation(ctx, stage, err, interval) {
				continue
			}
			if err != nil || op == nil || op.Error != nil {
				return errors.New("operation unavailable or failed")
			}
			for _, raw := range []json.RawMessage{json.RawMessage(op.Metadata), json.RawMessage(op.Response)} {
				var v struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(raw, &v) == nil && executionValid(p.Job(stage), v.Name) {
					h.Execution = v.Name
					break
				}
			}
			if h.Execution != "" {
				if err = save(h); err != nil {
					return err
				}
			} else if op.Done {
				return errors.New("completed operation has no execution identity")
			}
		}
		if h.Execution != "" {
			if !executionValid(p.Job(stage), h.Execution) {
				return errors.New("invalid execution identity")
			}
			get := func() (*run.GoogleCloudRunV2Execution, error) {
				readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				fields := googleapi.Field("name,createTime,startTime,completionTime,runningCount,succeededCount,failedCount,cancelledCount,retriedCount,taskCount,template(serviceAccount,maxRetries,timeout,nodeSelector,gpuZonalRedundancyDisabled,containers(image,args,command,resources))")
				return c.Service.Projects.Locations.Jobs.Executions.Get(h.Execution).Fields(fields).Context(readCtx).Do()
			}
			v, err := get()
			if err != nil {
				if retryObservation(ctx, stage, err, interval) {
					continue
				}
				return errors.New("execution read unavailable")
			}
			if v == nil || v.Name != h.Execution || v.TaskCount != p.TaskCount(stage) || !executionTaskValid(p, stage, v.Template) || !reflect.DeepEqual(v.Template.Containers[0].Args, p.Args(stage)) {
				return errors.New("execution differs from approved CPU stage")
			}
			if v.CompletionTime != "" && v.RunningCount == 0 {
				confirmed, err := get()
				if err != nil && retryObservation(ctx, stage, err, interval) {
					continue
				}
				if err != nil || confirmed == nil || confirmed.Name != h.Execution || confirmed.CompletionTime == "" || confirmed.RunningCount != 0 || confirmed.TaskCount != p.TaskCount(stage) || confirmed.SucceededCount != p.TaskCount(stage) || confirmed.FailedCount != 0 || confirmed.CancelledCount != 0 || confirmed.RetriedCount != 0 {
					return errors.New("successful terminal execution unconfirmed")
				}
				telemetry.Lifecycle(ctx, stage, confirmed.Name, confirmed.CreateTime, confirmed.StartTime, confirmed.CompletionTime, false)
				return nil
			}
		}
		if err := pause(ctx, interval); err != nil {
			return errors.New("observation deadline reached; remote execution may still be active")
		}
	}
}

// Only GPU observation GETs recover automatically. Launch and cancellation POSTs
// are never retried; the caller's absolute GPU deadline bounds every read/pause.
func retryObservation(ctx context.Context, stage string, err error, interval time.Duration) bool {
	if stage != "worker-a" || ctx.Err() != nil || !transientObservation(err) {
		return false
	}
	return pause(ctx, interval) == nil
}

func transientObservation(err error) bool {
	var api *googleapi.Error
	if errors.As(err, &api) {
		return api.Code == 408 || api.Code == 429 || api.Code == 500 || api.Code == 502 || api.Code == 503 || api.Code == 504
	}
	var network net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (errors.As(err, &network) && network.Timeout())
}
