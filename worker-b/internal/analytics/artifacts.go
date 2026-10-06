// Package analytics provides bounded projections and reads of completed analyses.
package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"reflect"
	"regexp"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	contracts "github.com/juanmh10/gemma-microservices/schemas/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

const Dataset = "poc_gemma_analytics"
const Region = "us-central1"
const Prefix = "gs://your-gcp-project-id-results/analysis"
const IndexPrefix = "gs://your-gcp-project-id-results/index"
const MaxBytesBilled int64 = 100 << 20

var validID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var validHash = regexp.MustCompile(`^[a-f0-9]{64}$`)
var ErrInvalid = errors.New("invalid completed artifact")

func IDValid(id string) bool { return validID.MatchString(id) }
func schemaValid(name string, data []byte) bool {
	var envelope struct {
		SchemaVersion string `json:"schema_version"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return false
	}
	version := "v1"
	if envelope.SchemaVersion == analysis.BatchSchemaVersion {
		version = "v2"
	}
	raw, err := contracts.Files.ReadFile(name + "-" + version + ".json")
	if err != nil {
		return false
	}
	var schema jsonschema.Schema
	if json.Unmarshal(raw, &schema) != nil {
		return false
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		return false
	}
	var value any
	return json.Unmarshal(data, &value) == nil && resolved.Validate(value) == nil
}

// Load reads only the four bounded objects needed to validate a completed report.
// It never reads source dialogues, labels, model weights or request-source URLs.
func Load(ctx context.Context, s analysis.Store, prefix, id string) (analysis.Report, string, error) {
	var r analysis.Report
	if !IDValid(id) {
		return r, "", ErrInvalid
	}
	base := path.Join(id)
	read := func(name string) ([]byte, error) {
		return s.Read(ctx, prefix+"/"+base+"/"+name, analysis.MaxObjectBytes)
	}
	marker, err := read("_SUCCESS.json")
	if err != nil {
		return r, "", err
	}
	var c analysis.Completion
	if analysis.Decode(marker, &c) != nil || (c.SchemaVersion != analysis.SchemaVersion && c.SchemaVersion != analysis.BatchSchemaVersion) || c.Status != "completed" || c.AnalysisID != id || !validHash.MatchString(c.ReportSHA256) {
		return r, "", ErrInvalid
	}
	raw, err := read("report.json")
	if err != nil {
		return r, "", err
	}
	if analysis.Digest(raw) != c.ReportSHA256 || !schemaValid("report", raw) || analysis.Decode(raw, &r) != nil || r.SchemaVersion != c.SchemaVersion || r.AnalysisID != id || r.RequestSHA256 != c.RequestSHA256 {
		return r, "", ErrInvalid
	}
	request, err := read("request.json")
	if err != nil {
		return r, "", err
	}
	var req analysis.Request
	if analysis.Digest(request) != c.RequestSHA256 || !schemaValid("request", request) || analysis.Decode(request, &req) != nil || req.SchemaVersion != r.SchemaVersion || req.AnalysisID != id || req.SourceRunID != r.Metrics.SourceRunID || req.Model != r.Model || req.ModelMode != r.ModelMode || req.Location != r.Location || req.PromptVersion != r.PromptVersion {
		return r, "", ErrInvalid
	}
	metrics, err := read("metrics.json")
	if err != nil {
		return r, "", err
	}
	var m analysis.Metrics
	if !schemaValid("metrics", metrics) || analysis.Decode(metrics, &m) != nil || !reflect.DeepEqual(m, r.Metrics) || r.Narrative.Validate(m) != nil {
		return r, "", ErrInvalid
	}
	if analysis.ValidateMetrics(m, req) != nil {
		return r, "", ErrInvalid
	}
	decision := analysis.Decide(m, req.Rules)
	if !reflect.DeepEqual(decision, r.Decision) || m.RecordsExpected != m.RecordsValid+m.RecordsInvalid || m.RecordsExpected < 1 || m.RecordsExpected > req.RecordLimit() {
		return r, "", ErrInvalid
	}
	if req.ModelMode == "vertex" && req.Validate(true) != nil {
		return r, "", ErrInvalid
	}
	return r, c.ReportSHA256, nil
}

// RunRow excludes evidence excerpts while retaining immutable provenance.
type RunRow struct {
	AnalysisID      string            `json:"analysis_id"`
	SourceRunID     string            `json:"source_run_id"`
	CreatedAt       string            `json:"created_at"`
	SchemaVersion   string            `json:"schema_version"`
	ReportSHA256    string            `json:"report_sha256"`
	ReportURI       string            `json:"report_uri"`
	QualityStatus   string            `json:"quality_status"`
	RecordsExpected int               `json:"records_expected"`
	RecordsValid    int               `json:"records_valid"`
	RecordsInvalid  int               `json:"records_invalid"`
	CorrectOutcomes *int              `json:"correct_outcomes"`
	SchemaValidRate float64           `json:"schema_valid_rate"`
	OutcomeAccuracy *float64          `json:"outcome_accuracy"`
	CauseF1         *float64          `json:"cause_f1"`
	Model           string            `json:"model"`
	ModelMode       string            `json:"model_mode"`
	ModelLocation   string            `json:"model_location"`
	ModelVersion    string            `json:"model_version"`
	InputTokens     int               `json:"input_tokens"`
	OutputTokens    int               `json:"output_tokens"`
	Provenance      map[string]string `json:"provenance"`
}

func Project(r analysis.Report, hash string) (RunRow, map[string]any) {
	row := RunRow{AnalysisID: r.AnalysisID, SourceRunID: r.Metrics.SourceRunID, CreatedAt: r.CreatedAt.Truncate(time.Microsecond).Format("2006-01-02T15:04:05.999999Z07:00"), SchemaVersion: "index-v1", ReportSHA256: hash, ReportURI: Prefix + "/" + r.AnalysisID + "/report.json", QualityStatus: r.Decision.Status, RecordsExpected: r.Metrics.RecordsExpected, RecordsValid: r.Metrics.RecordsValid, RecordsInvalid: r.Metrics.RecordsInvalid, CorrectOutcomes: r.Metrics.CorrectOutcomes, SchemaValidRate: r.Metrics.SchemaValidRate, OutcomeAccuracy: r.Metrics.OutcomeAccuracy, CauseF1: r.Metrics.CauseF1, Model: r.Model, ModelMode: r.ModelMode, ModelLocation: r.Location, ModelVersion: r.Usage.ModelVersion, InputTokens: r.Usage.InputTokens, OutputTokens: r.Usage.OutputTokens, Provenance: map[string]string{"request_sha256": r.RequestSHA256, "prompt_version": r.PromptVersion, "prompt_sha256": r.PromptSHA256, "image_digest": r.ImageDigest, "source_sha256": r.SourceSHA256, "source_revision": r.SourceRevision, "manifest_sha256": r.Metrics.ManifestSHA256, "input_fingerprint": r.Metrics.InputFingerprint, "evaluator_version": r.Metrics.EvaluatorVersion, "rules_version": r.Decision.Rules.Version, "source_image": r.Metrics.SourceImage}}
	report := map[string]any{"analysis_id": r.AnalysisID, "created_at": row.CreatedAt, "schema_version": "index-v1", "report_sha256": hash, "report_uri": row.ReportURI, "summary": r.Narrative.Summary, "findings": r.Narrative.Findings, "recommendations": r.Narrative.Recommendations, "limitations": r.Narrative.Limitations, "per_cause": r.Metrics.PerCause}
	return row, report
}
