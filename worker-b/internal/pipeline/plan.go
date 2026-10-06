// Package pipeline coordinates the existing CPU Jobs without provisioning resources.
package pipeline

import (
	"errors"
	"regexp"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analytics"
)

const Version = "pipeline-plan-v1"
const Root = "projects/your-gcp-project-id/locations/us-central1"
const Notifier = Root + "/services/poc-gemma-notifier"

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var revisionPattern = regexp.MustCompile(`^projects/your-gcp-project-id/locations/us-central1/services/poc-gemma-notifier/revisions/poc-gemma-notifier-[a-z0-9-]+$`)
var objectPattern = regexp.MustCompile(`^gs://your-gcp-project-id-(prepared|ground-truth|results)/[a-zA-Z0-9/_.-]+$`)
var versionPattern = regexp.MustCompile(`^[1-9][0-9]*$`)

type WorkerA struct {
	CanaryID string `json:"canary_id"`
	Image    string `json:"image"`
	GitSHA   string `json:"git_sha"`
}

type Plan struct {
	Preparation          *Preparation      `json:"preparation,omitempty"`
	WorkerA              *WorkerA          `json:"worker_a,omitempty"`
	SchemaVersion        string            `json:"schema_version"`
	Project              string            `json:"project"`
	Region               string            `json:"region"`
	AnalysisID           string            `json:"analysis_id"`
	AnalysisMode         string            `json:"analysis_mode"`
	ExpectedReportSHA256 string            `json:"expected_report_sha256,omitempty"`
	Request              *analysis.Request `json:"request,omitempty"`
	AnalysisImage        string            `json:"analysis_image,omitempty"`
	DataImage            string            `json:"data_image"`
	MessagingImage       string            `json:"messaging_image"`
	NotifierRevision     string            `json:"notifier_revision"`
	SecretVersion        string            `json:"secret_version"`
	IndexAttempt         string            `json:"index_attempt"`
	Republish            bool              `json:"republish"`
	RequireEmail         bool              `json:"require_email"`
}

func imageValid(s, component string) bool {
	return regexp.MustCompile(`^us-central1-docker\.pkg\.dev/your-gcp-project-id/pipeline/` + component + `@sha256:[a-f0-9]{64}$`).MatchString(s)
}
func (p Plan) Validate() error {
	if p.SchemaVersion != Version || p.Project != analysis.Project || p.Region != analytics.Region || !analytics.IDValid(p.AnalysisID) || !analytics.IDValid(p.IndexAttempt) || !imageValid(p.DataImage, "analytics") || !imageValid(p.MessagingImage, "messaging") || !revisionPattern.MatchString(p.NotifierRevision) || !versionPattern.MatchString(p.SecretVersion) {
		return errors.New("invalid fixed-project pipeline plan")
	}
	if p.Preparation != nil {
		if err := p.validatePreparation(); err != nil {
			return err
		}
	}
	if p.WorkerA != nil {
		if p.AnalysisMode != "analyze" || p.WorkerA.Image != CachedWorkerImage || p.WorkerA.GitSHA != CachedWorkerRevision || !regexp.MustCompile(`^pipeline-[a-z0-9-]{3,30}$`).MatchString(p.WorkerA.CanaryID) || p.Request == nil || p.Request.SourceImage != p.WorkerA.Image || (p.Request.SourceRunID != "pilot-004" && !regexp.MustCompile(`^pipeline-[a-z0-9-]{3,30}$`).MatchString(p.Request.SourceRunID)) || p.Request.Metrics != nil || p.Request.Truth == nil {
			return errors.New("invalid single-shard cached Worker A scope")
		}
	}
	switch p.AnalysisMode {
	case "reuse":
		if !digestPattern.MatchString(p.ExpectedReportSHA256) || p.Request != nil || p.AnalysisImage != "" {
			return errors.New("reuse requires an immutable report hash and no analysis launch")
		}
	case "analyze":
		if p.ExpectedReportSHA256 != "" || p.Request == nil || p.Request.Validate(true) != nil || p.Request.Mode != "analyze" || p.Request.ModelMode != "vertex" || p.Request.AnalysisID != p.AnalysisID || p.Request.OutputPrefix != analytics.Prefix || !imageValid(p.AnalysisImage, "analysis") {
			return errors.New("analyze requires a pinned retained-input request and CPU image")
		}
		refs := []analysis.ObjectRef{p.Request.Manifest, p.Request.Shard, p.Request.Marker}
		refs = append(refs, p.Request.Chunks...)
		if p.Request.Truth != nil {
			refs = append(refs, *p.Request.Truth)
		}
		if p.Request.Metrics != nil {
			refs = append(refs, *p.Request.Metrics)
		}
		for _, ref := range refs {
			if !objectPattern.MatchString(ref.URI) {
				return errors.New("plan source must be a metadata-only object path")
			}
		}
	default:
		return errors.New("unsupported analysis mode")
	}
	return nil
}
func (p Plan) Digest() string { return analysis.Digest(analysis.JSON(p)) }
func (p Plan) RequestURI() string {
	return "gs://your-gcp-project-id-results/analysis-requests/" + p.AnalysisID + ".json"
}
func (p Plan) Job(stage string) string {
	switch stage {
	case "prepare":
		if p.Preparation != nil {
			return Root + "/jobs/poc-gemma-dataset-preparer"
		}
		return ""
	case "worker-a":
		if p.WorkerA != nil {
			return Root + "/jobs/poc-gemma-worker-a-rtx6000"
		}
		return ""
	case "analysis":
		return Root + "/jobs/poc-gemma-analysis"
	case "index":
		return Root + "/jobs/poc-gemma-index"
	case "publish":
		return Root + "/jobs/poc-gemma-publisher"
	}
	return ""
}
func (p Plan) Args(stage string) []string {
	switch stage {
	case "prepare":
		return []string{"--limit", "900", "--shards", "3", "--prompt-version", "worker-a-v2"}
	case "analysis":
		return []string{"--remote", "--model", "vertex", "--request", p.RequestURI()}
	case "index":
		return []string{"--remote", "--mode", "index", "--analysis-id", p.AnalysisID, "--attempt", p.IndexAttempt}
	case "publish":
		a := []string{"--remote", "--mode", "publish", "--analysis-id", p.AnalysisID}
		if p.Republish {
			a = append(a, "--republish")
		}
		return a
	}
	return nil
}
func Authorized(p Plan, execute bool, approved string) error {
	if p.Validate() != nil {
		return errors.New("invalid pipeline plan")
	}
	if !execute || approved != p.Digest() {
		return errors.New("execution blocked; explicit approval of the exact plan is required")
	}
	return nil
}

func (p Plan) TaskCount(stage string) int64 {
	if stage == "worker-a" && p.Request != nil {
		return int64(p.Request.TaskCount())
	}
	return 1
}
