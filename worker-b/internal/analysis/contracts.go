// Package analysis validates retained Worker A artifacts and persists bounded analyses.
package analysis

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
)

const (
	Project            = "your-gcp-project-id"
	Location           = "us"
	ModelID            = "gemini-3.5-flash-lite"
	SchemaVersion      = "analysis-v1"
	BatchSchemaVersion = "analysis-v2"
	EvaluatorVersion   = "benchmark-v1"
	PromptVersion      = "worker-b-v1"
	BatchPromptVersion = "worker-b-v2"
	MaxObjectBytes     = 16 << 20
	MaxTotalBytes      = 64 << 20
	MaxRecords         = 300
)

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var revisionPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)
var analysisImagePattern = regexp.MustCompile(`^us-central1-docker\.pkg\.dev/your-gcp-project-id/pipeline/analysis@sha256:[a-f0-9]{64}$`)

type ObjectRef struct {
	URI    string `json:"uri"`
	SHA256 string `json:"sha256"`
}

type QualityRules struct {
	Version            string  `json:"version"`
	MinValidRate       float64 `json:"min_valid_rate"`
	MinOutcomeAccuracy float64 `json:"min_outcome_accuracy"`
	MinCauseF1         float64 `json:"min_cause_f1"`
}

// Request explicitly pins each object; storage listing never discovers inputs.
type TaskSource struct {
	TaskIndex int         `json:"task_index"`
	Shard     ObjectRef   `json:"shard"`
	Marker    ObjectRef   `json:"marker"`
	Chunks    []ObjectRef `json:"chunks"`
}

type Request struct {
	AdditionalTasks []TaskSource `json:"additional_tasks,omitempty"`
	SchemaVersion   string       `json:"schema_version"`
	Mode            string       `json:"mode"`
	AnalysisID      string       `json:"analysis_id"`
	SourceRunID     string       `json:"source_run_id"`
	TaskIndex       int          `json:"task_index"`
	SourceImage     string       `json:"source_image"`
	Manifest        ObjectRef    `json:"manifest"`
	Shard           ObjectRef    `json:"shard"`
	Marker          ObjectRef    `json:"marker"`
	Chunks          []ObjectRef  `json:"chunks"`
	Truth           *ObjectRef   `json:"truth,omitempty"`
	Metrics         *ObjectRef   `json:"metrics,omitempty"`
	Rules           QualityRules `json:"rules"`
	Model           string       `json:"model"`
	ModelMode       string       `json:"model_mode"`
	Location        string       `json:"location"`
	PromptVersion   string       `json:"prompt_version"`
	OutputPrefix    string       `json:"output_prefix"`
}

func Decode(data []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return errors.New("invalid JSON contract")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("trailing JSON content")
	}
	return nil
}

func (r Request) TaskCount() int   { return 1 + len(r.AdditionalTasks) }
func (r Request) RecordLimit() int { return MaxRecords * r.TaskCount() }
func (r Request) Sources() []TaskSource {
	return append([]TaskSource{{TaskIndex: r.TaskIndex, Shard: r.Shard, Marker: r.Marker, Chunks: r.Chunks}}, r.AdditionalTasks...)
}
func (r Request) Validate(remote bool) error {
	if (r.SchemaVersion == SchemaVersion && len(r.AdditionalTasks) != 0) || (r.SchemaVersion == BatchSchemaVersion && (len(r.AdditionalTasks) != 2 || r.Truth == nil || r.Metrics != nil || r.SourceRunID == "pilot-004")) {
		return errors.New("invalid full population contract")
	}
	for i, t := range r.Sources() {
		if t.TaskIndex != i || len(t.Chunks) < 1 || len(t.Chunks) > 16 {
			return errors.New("invalid ordered task sources")
		}
	}

	if (r.ModelMode != "fake" && r.ModelMode != "vertex") || (r.Mode != "evaluate" && r.Mode != "analyze") || (r.SchemaVersion != SchemaVersion && r.SchemaVersion != BatchSchemaVersion) || !idPattern.MatchString(r.AnalysisID) || !idPattern.MatchString(r.SourceRunID) || r.TaskIndex != 0 || r.Model != ModelID || r.Location != Location || (r.PromptVersion != PromptVersion && !(r.SchemaVersion == BatchSchemaVersion && r.PromptVersion == BatchPromptVersion)) || r.Rules.Version != "quality-v1" {
		return errors.New("unsupported analysis identity, scope or version")
	}
	// These POC thresholds are explicit requirements, not tuned to force a canary pass.
	if r.Rules.MinValidRate != .99 || r.Rules.MinOutcomeAccuracy != .98 || r.Rules.MinCauseF1 != .90 {
		return errors.New("quality-v1 thresholds must be 0.99/0.98/0.90")
	}
	if !strings.HasPrefix(r.SourceImage, "us-central1-docker.pkg.dev/"+Project+"/pipeline/worker-a@sha256:") || !digestPattern.MatchString(strings.Split(r.SourceImage, "@sha256:")[1]) {
		return errors.New("source image must be immutable")
	}
	if len(r.Chunks) == 0 || len(r.Chunks) > 16 {
		return errors.New("invalid chunk count")
	}
	refs := []ObjectRef{r.Manifest, r.Shard, r.Marker}
	refs = append(refs, r.Chunks...)
	for _, t := range r.AdditionalTasks {
		refs = append(refs, t.Shard, t.Marker)
		refs = append(refs, t.Chunks...)
	}
	if r.Truth != nil {
		refs = append(refs, *r.Truth)
	}
	if r.Metrics != nil {
		refs = append(refs, *r.Metrics)
	}
	seen := map[string]bool{}
	for _, ref := range refs {
		if ref.URI == "" || !digestPattern.MatchString(ref.SHA256) || seen[ref.URI] {
			return errors.New("missing digest or duplicate source reference")
		}
		seen[ref.URI] = true
		if remote && !allowedReadForRun(ref.URI, r.SourceRunID) {
			return errors.New("source reference outside allowlist")
		}
		if !remote && strings.Contains(ref.URI, "://") {
			return errors.New("local mode accepts only local sources")
		}
	}
	if remote && r.ModelMode != "vertex" {
		return errors.New("remote requests require Vertex mode")
	}
	if remote && r.OutputPrefix != "gs://"+Project+"-results/analysis" {
		return errors.New("remote output prefix is fixed")
	}
	if !remote && (r.OutputPrefix == "" || strings.Contains(r.OutputPrefix, "://")) {
		return errors.New("local output prefix required")
	}
	return nil
}

// Fresh runs remain isolated by the request run and exact Terraform IAM prefix.
func allowedReadForRun(uri, runID string) bool {
	if strings.Contains(uri, "..") || strings.ContainsAny(uri, "?#\\") {
		return false
	}
	if runID == "pilot-004" {
		return allowedRead(uri)
	}
	if !regexp.MustCompile(`^pipeline-[a-z0-9-]{3,30}$`).MatchString(runID) {
		return false
	}
	for _, bucket := range []string{"prepared", "ground-truth"} {
		prefix := "gs://" + Project + "-" + bucket + "/runs/" + runID + "/"
		if strings.HasPrefix(uri, prefix) && len(uri) > len(prefix) {
			return true
		}
	}
	return regexp.MustCompile(`^gs://your-gcp-project-id-results/canary/pipeline-[a-z0-9-]{3,30}/runs/` + regexp.QuoteMeta(runID) + `/worker-a/rtx6000/task-0000[012]/(?:_SUCCESS\.json|chunk-[0-9]{6}\.parquet)$`).MatchString(uri)
}

func allowedRead(uri string) bool {
	if strings.Contains(uri, "..") || strings.ContainsAny(uri, "?#\\") {
		return false
	}
	if regexp.MustCompile(`^gs://your-gcp-project-id-results/canary/pipeline-[a-z0-9-]{3,30}/runs/pilot-004/worker-a/rtx6000/task-00000/(?:_SUCCESS\.json|chunk-[0-9]{6}\.parquet)$`).MatchString(uri) {
		return true
	}
	for _, prefix := range []string{"gs://" + Project + "-prepared/runs/pilot-004/", "gs://" + Project + "-ground-truth/runs/pilot-004/", "gs://" + Project + "-results/canary/reject-diag-cached-300-20260930/", "gs://" + Project + "-results/analysis/"} {
		if strings.HasPrefix(uri, prefix) && len(uri) > len(prefix) {
			return true
		}
	}
	return false
}

type Evidence struct {
	RecordID  string `json:"record_id"`
	CauseCode string `json:"cause_code"`
	TurnID    int32  `json:"turn_id"`
	Text      string `json:"text"`
}

type Metrics struct {
	SchemaVersion    string                            `json:"schema_version"`
	EvaluatorVersion string                            `json:"evaluator_version"`
	SourceRunID      string                            `json:"source_run_id"`
	ManifestSHA256   string                            `json:"manifest_sha256"`
	SourceImage      string                            `json:"source_image"`
	InputFingerprint string                            `json:"input_fingerprint"`
	RecordsExpected  int                               `json:"records_expected"`
	SourceRecords    int                               `json:"source_records"`
	RecordsValid     int                               `json:"records_valid"`
	RecordsInvalid   int                               `json:"records_invalid"`
	CorrectOutcomes  *int                              `json:"correct_outcomes"`
	SchemaValidRate  float64                           `json:"schema_valid_rate"`
	OutcomeAccuracy  *float64                          `json:"outcome_accuracy"`
	CausePrecision   *float64                          `json:"cause_precision"`
	CauseRecall      *float64                          `json:"cause_recall"`
	CauseF1          *float64                          `json:"cause_f1"`
	PerCause         map[string]benchmark.CauseMetrics `json:"per_cause"`
	Evidence         []Evidence                        `json:"evidence"`
}

type Decision struct {
	Status  string       `json:"status"`
	Rules   QualityRules `json:"rules"`
	Reasons []string     `json:"reasons"`
}

func Decide(m Metrics, rules QualityRules) Decision {
	d := Decision{Status: "not_evaluated", Rules: rules, Reasons: []string{}}
	if m.OutcomeAccuracy == nil || m.CauseF1 == nil {
		d.Reasons = append(d.Reasons, "ground_truth_unavailable")
		return d
	}
	d.Status = "pass"
	if m.SchemaValidRate < rules.MinValidRate {
		d.Reasons = append(d.Reasons, "schema_valid_rate_below_threshold")
	}
	if *m.OutcomeAccuracy < rules.MinOutcomeAccuracy {
		d.Reasons = append(d.Reasons, "outcome_accuracy_below_threshold")
	}
	if *m.CauseF1 < rules.MinCauseF1 {
		d.Reasons = append(d.Reasons, "cause_f1_below_threshold")
	}
	if len(d.Reasons) > 0 {
		d.Status = "fail"
	}
	return d
}

type Finding struct {
	Code        string   `json:"code"`
	Observation string   `json:"observation"`
	MetricRefs  []string `json:"metric_refs"`
}

type Narrative struct {
	Summary         string    `json:"summary"`
	Findings        []Finding `json:"findings"`
	Recommendations []string  `json:"recommendations"`
	Limitations     []string  `json:"limitations"`
}

// MetricReferences is shared by provider output constraints and validation.
func MetricReferences(m Metrics) []string {
	allowed := map[string]bool{"records_expected": true, "records_valid": true, "records_invalid": true, "schema_valid_rate": true}
	if m.CauseF1 != nil {
		for _, k := range []string{"outcome_accuracy", "cause_precision", "cause_recall", "cause_f1"} {
			allowed[k] = true
		}
		for code := range m.PerCause {
			allowed["per_cause."+code] = true
		}
	}
	for _, e := range m.Evidence {
		allowed["evidence."+e.RecordID+"."+fmt.Sprint(e.TurnID)] = true
	}
	refs := make([]string, 0, len(allowed))
	for ref := range allowed {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return refs
}

func (n Narrative) Validate(m Metrics) error {
	if len(n.Summary) < 1 || len(n.Summary) > 4000 || n.Findings == nil || n.Recommendations == nil || len(n.Findings) > 10 || len(n.Recommendations) > 10 || len(n.Limitations) == 0 || len(n.Limitations) > 10 {
		return errors.New("invalid narrative bounds")
	}
	allowed := map[string]bool{}
	for _, ref := range MetricReferences(m) {
		allowed[ref] = true
	}
	seen := map[string]bool{}
	for _, f := range n.Findings {
		if !idPattern.MatchString(f.Code) || seen[f.Code] || len(f.Observation) == 0 || len(f.Observation) > 2000 || len(f.MetricRefs) == 0 || len(f.MetricRefs) > 10 {
			return errors.New("invalid finding")
		}
		seen[f.Code] = true
		for _, ref := range f.MetricRefs {
			if !allowed[ref] {
				return errors.New("unsupported finding metric reference")
			}
		}
	}
	for _, list := range [][]string{n.Recommendations, n.Limitations} {
		for _, v := range list {
			if len(v) == 0 || len(v) > 2000 {
				return errors.New("invalid narrative text length")
			}
		}
	}
	return nil
}

type Usage struct {
	Requests     int    `json:"requests"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
	ModelVersion string `json:"model_version"`
}

type Report struct {
	SchemaVersion  string    `json:"schema_version"`
	AnalysisID     string    `json:"analysis_id"`
	CreatedAt      time.Time `json:"created_at"`
	RequestSHA256  string    `json:"request_sha256"`
	Metrics        Metrics   `json:"metrics"`
	Decision       Decision  `json:"decision"`
	Narrative      Narrative `json:"narrative"`
	Model          string    `json:"model"`
	Location       string    `json:"location"`
	PromptVersion  string    `json:"prompt_version"`
	PromptSHA256   string    `json:"prompt_sha256"`
	ImageDigest    string    `json:"image_digest"`
	SourceSHA256   string    `json:"source_sha256"`
	SourceRevision string    `json:"source_revision"`
	ModelMode      string    `json:"model_mode"`
	Usage          Usage     `json:"usage"`
	DurationMS     int64     `json:"duration_ms"`
}

func (r Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Analysis %s\n\n%s\n\nQuality: %s\n\n", r.AnalysisID, r.Narrative.Summary, r.Decision.Status)
	for _, f := range r.Narrative.Findings {
		fmt.Fprintf(&b, "- %s: %s\n", f.Code, f.Observation)
	}
	b.WriteString("\n## Recommendations\n\n")
	for _, s := range r.Narrative.Recommendations {
		fmt.Fprintf(&b, "- %s\n", s)
	}
	b.WriteString("\n## Limitations\n\n")
	for _, s := range r.Narrative.Limitations {
		fmt.Fprintf(&b, "- %s\n", s)
	}
	return b.String()
}
