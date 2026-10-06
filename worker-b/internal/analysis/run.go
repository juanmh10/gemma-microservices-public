package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"path"
	"strings"
	"time"
)

type Narrator interface {
	Generate(context.Context, Metrics, Decision) (Narrative, Usage, error)
}
type Config struct {
	Remote                                                             bool
	ImageDigest, SourceRevision, SourceSHA256, PromptSHA256, ModelMode string
}

type Completion struct {
	SchemaVersion string `json:"schema_version"`
	AnalysisID    string `json:"analysis_id"`
	RequestSHA256 string `json:"request_sha256"`
	ReportSHA256  string `json:"report_sha256"`
	Status        string `json:"status"`
}

type StageError struct{ Stage string }

func (e StageError) Error() string { return "analysis failed at " + e.Stage }

// NarrationError exposes only a fixed category, never a provider response.
type NarrationError struct {
	Category       string `json:"category"`
	HTTPStatus     int    `json:"http_status,omitempty"`
	ProviderStatus string `json:"provider_status,omitempty"`
}

func (e NarrationError) Error() string { return "narration failed: " + e.Category }

func objectPath(r Request, name string) string {
	return strings.TrimRight(r.OutputPrefix, "/") + "/" + path.Join(r.AnalysisID, name)
}
func JSON(v any) []byte { data, _ := json.MarshalIndent(v, "", "  "); return append(data, '\n') }

// Run commits the claim before evaluation/model calls. Unknown claims are never retried.
func Run(ctx context.Context, s Store, r Request, n Narrator, cfg Config) (status string, retErr error) {
	if cfg.ModelMode == "" {
		cfg.ModelMode = "fake"
	}
	ctx = telemetry.Mode(telemetry.Bind(ctx, r.AnalysisID, ""), cfg.ModelMode)
	span := telemetry.Begin(ctx, "analysis", "run")
	defer func() { span.End(retErr != nil) }()
	if r.ModelMode != cfg.ModelMode {
		return "", StageError{"model_mode_conflict"}
	}
	if err := r.Validate(cfg.Remote); err != nil {
		return "", StageError{"request_validation"}
	}
	if cfg.Remote && (!analysisImagePattern.MatchString(cfg.ImageDigest) || !revisionPattern.MatchString(cfg.SourceRevision) || !digestPattern.MatchString(cfg.SourceSHA256) || cfg.ModelMode != "vertex") {
		return "", StageError{"runtime_provenance"}
	}
	started := time.Now()
	requestBytes := JSON(r)
	requestHash := Digest(requestBytes)
	markerName := "_SUCCESS.json"
	if r.Mode == "evaluate" {
		markerName = "_EVALUATED.json"
	}
	completion, err := s.Read(ctx, objectPath(r, markerName), 1<<20)
	if err == nil {
		var marker Completion
		if Decode(completion, &marker) != nil || marker.AnalysisID != r.AnalysisID || marker.RequestSHA256 != requestHash || marker.Status != "completed" {
			return "", StageError{"completion_conflict"}
		}
		name := "report.json"
		if r.Mode == "evaluate" {
			name = "metrics.json"
		}
		artifact, err := s.Read(ctx, objectPath(r, name), MaxObjectBytes)
		if err != nil || Digest(artifact) != marker.ReportSHA256 {
			return "", StageError{"completion_integrity"}
		}
		return "existing", nil
	}
	if !errors.Is(err, ErrMissing) {
		return "", StageError{"completion_read"}
	}
	if err = s.Create(ctx, objectPath(r, "request.json"), requestBytes); err != nil {
		return "", StageError{"claim_conflict_or_unknown"}
	}
	failureCategory := ""
	var providerDiagnostic *NarrationError
	fail := func(stage string) (string, error) {
		// Preserve the claim and committed artifacts. Failure recording never calls the model.
		failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		failure := map[string]string{"analysis_id": r.AnalysisID, "stage": stage}
		_ = s.Create(failureCtx, objectPath(r, "_FAILED.json"), JSON(failure))
		if providerDiagnostic != nil {
			detail := map[string]any{"schema_version": "analysis-model-failure-v2", "analysis_id": r.AnalysisID, "category": providerDiagnostic.Category}
			if providerDiagnostic.HTTPStatus != 0 {
				detail["http_status"] = providerDiagnostic.HTTPStatus
			}
			if providerDiagnostic.ProviderStatus != "" {
				detail["provider_status"] = providerDiagnostic.ProviderStatus
			}
			_ = s.Create(failureCtx, objectPath(r, "_MODEL_FAILURE.json"), JSON(detail))
		} else if failureCategory != "" {
			_ = s.Create(failureCtx, objectPath(r, "_MODEL_FAILURE.json"), JSON(map[string]string{"schema_version": "analysis-model-failure-v1", "analysis_id": r.AnalysisID, "category": failureCategory}))
		}
		return "", StageError{stage}
	}
	phase := telemetry.Begin(ctx, "analysis", "evaluation")
	metrics, err := Evaluate(ctx, s, r)
	phase.End(err != nil)
	if err != nil {
		return fail("evaluation")
	}
	metricBytes := JSON(metrics)
	if err = s.Create(ctx, objectPath(r, "metrics.json"), metricBytes); err != nil {
		return fail("metrics_write")
	}
	reportBytes := metricBytes
	if r.Mode == "analyze" {
		if n == nil {
			return fail("model_configuration")
		}
		decision := Decide(metrics, r.Rules)
		phase := telemetry.Begin(ctx, "analysis", "model_generation")
		narrative, usage, err := n.Generate(ctx, metrics, decision)
		phase.Tokens(usage.InputTokens, usage.OutputTokens)
		phase.End(err != nil)
		if err != nil {
			var diagnostic NarrationError
			if errors.As(err, &diagnostic) {
				switch diagnostic.Category {
				case "provider_http", "provider_transport", "model_deadline":
					if diagnostic.SafeProviderDiagnostic() {
						providerDiagnostic = &diagnostic
					}
				case "adk_execution", "missing_final_response", "narrative_json", "narrative_bounds", "finding_bounds", "metric_reference", "narrative_text":
					failureCategory = diagnostic.Category
				}
			}
			return fail("model")
		}
		if narrative.Validate(metrics) != nil || usage.Requests < 1 || usage.Requests > 3 || usage.InputTokens < 0 || usage.InputTokens > 24000 || usage.OutputTokens < 0 || usage.OutputTokens > 6000 {
			return fail("report_validation")
		}
		report := Report{SchemaVersion: r.SchemaVersion, AnalysisID: r.AnalysisID, CreatedAt: time.Now().UTC(), RequestSHA256: requestHash, Metrics: metrics, Decision: decision, Narrative: narrative, Model: r.Model, Location: r.Location, PromptVersion: r.PromptVersion, PromptSHA256: cfg.PromptSHA256, ImageDigest: cfg.ImageDigest, SourceRevision: cfg.SourceRevision, SourceSHA256: cfg.SourceSHA256, ModelMode: cfg.ModelMode, Usage: usage, DurationMS: time.Since(started).Milliseconds()}
		reportBytes = JSON(report)
		if err = s.Create(ctx, objectPath(r, "report.json"), reportBytes); err != nil {
			return fail("report_write")
		}
		if err = s.Create(ctx, objectPath(r, "report.md"), []byte(report.Markdown())); err != nil {
			return fail("render_write")
		}
	}
	marker := Completion{SchemaVersion: r.SchemaVersion, AnalysisID: r.AnalysisID, RequestSHA256: requestHash, ReportSHA256: Digest(reportBytes), Status: "completed"}
	if err = s.Create(ctx, objectPath(r, markerName), JSON(marker)); err != nil {
		return fail("completion_write")
	}
	return "completed", nil
}

// SafeProviderDiagnostic rejects untrusted values even from a custom narrator.
func (e NarrationError) SafeProviderDiagnostic() bool {
	if e.Category == "provider_transport" || e.Category == "model_deadline" {
		return e.HTTPStatus == 0 && e.ProviderStatus == ""
	}
	if e.Category != "provider_http" || e.HTTPStatus < 400 || e.HTTPStatus > 599 {
		return false
	}
	switch e.ProviderStatus {
	case "", "INVALID_ARGUMENT", "UNAUTHENTICATED", "PERMISSION_DENIED", "NOT_FOUND", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DEADLINE_EXCEEDED", "CANCELLED", "UNKNOWN":
		return true
	}
	return false
}
