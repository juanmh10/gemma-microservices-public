// Package agent implements bounded Google ADK reporting with Vertex identity auth.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"iter"
	"os"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	adkagent "google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/gemini"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"
	"google.golang.org/genai"
)

type Reporter struct {
	Instruction string
	Fake        bool
	Model       model.LLM
}

func vertexConfig() *genai.ClientConfig {
	attempts := int32(1)
	return &genai.ClientConfig{Backend: genai.BackendVertexAI, Project: analysis.Project, Location: analysis.Location, HTTPOptions: genai.HTTPOptions{APIVersion: "v1", RetryOptions: &genai.HTTPRetryOptions{Attempts: &attempts}}}
}

func (r *Reporter) Generate(ctx context.Context, m analysis.Metrics, d analysis.Decision) (analysis.Narrative, analysis.Usage, error) {
	llm := r.Model
	if llm == nil {
		if r.Fake {
			llm = &fakeModel{}
		} else {
			// Explicit Vertex configuration prevents API-key or endpoint fallback.
			if os.Getenv("GOOGLE_API_KEY") != "" || os.Getenv("GEMINI_API_KEY") != "" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
				return analysis.Narrative{}, analysis.Usage{}, errors.New("use attached or local user ADC, not API keys or credential files")
			}
			var err error
			llm, err = gemini.NewModel(ctx, analysis.ModelID, vertexConfig())
			if err != nil {
				return analysis.Narrative{}, analysis.Usage{}, errors.New("Vertex ADC initialization failed")
			}
		}
	}
	bounded := &boundedModel{inner: llm}
	causeTool, err := functiontool.New(functiontool.Config{Name: "get_cause_metrics", Description: "Read verified mathematical metrics for one observed cause code."}, func(_ adkagent.Context, args struct {
		Code string `json:"code"`
	}) (any, error) {
		value, ok := m.PerCause[args.Code]
		if !ok {
			return nil, errors.New("cause code unavailable")
		}
		return value, nil
	})
	if err != nil {
		return analysis.Narrative{}, analysis.Usage{}, err
	}
	evidenceTool, err := functiontool.New(functiontool.Config{Name: "get_evidence", Description: "Read one prevalidated, bounded customer evidence snippet; unavailable record IDs are rejected."}, func(_ adkagent.Context, args struct {
		RecordID string `json:"record_id"`
	}) (analysis.Evidence, error) {
		for _, e := range m.Evidence {
			if e.RecordID == args.RecordID {
				return e, nil
			}
		}
		return analysis.Evidence{}, errors.New("evidence record unavailable")
	})
	if err != nil {
		return analysis.Narrative{}, analysis.Usage{}, err
	}
	a, err := llmagent.New(llmagent.Config{Name: "report_analyst", Model: bounded, Instruction: r.Instruction, Tools: []tool.Tool{causeTool, evidenceTool}, OutputSchema: narrativeSchema(m), GenerateContentConfig: &genai.GenerateContentConfig{ThinkingConfig: &genai.ThinkingConfig{ThinkingLevel: genai.ThinkingLevelMinimal}}})
	if err != nil {
		return analysis.Narrative{}, analysis.Usage{}, err
	}
	run, err := runner.NewInMemory("part2_analysis", a)
	if err != nil {
		return analysis.Narrative{}, analysis.Usage{}, err
	}
	payload, _ := json.Marshal(struct {
		Metrics  analysis.Metrics  `json:"metrics"`
		Decision analysis.Decision `json:"decision"`
	}{m, d})
	msg := genai.NewContentFromText(string(payload), genai.RoleUser)
	var final string
	events := 0
	for event, err := range run.Run(ctx, "operator", "analysis", msg, adkagent.RunConfig{StreamingMode: adkagent.StreamingModeNone}) {
		if err != nil {
			if bounded.failure != nil {
				return analysis.Narrative{}, bounded.usage, *bounded.failure
			}
			return analysis.Narrative{}, bounded.usage, analysis.NarrationError{Category: "adk_execution"}
		}
		events++
		if events > 20 {
			return analysis.Narrative{}, bounded.usage, errors.New("ADK event limit exceeded")
		}
		if event != nil && event.IsFinalResponse() && event.Content != nil {
			var b strings.Builder
			for _, p := range event.Content.Parts {
				if p != nil && !p.Thought {
					b.WriteString(p.Text)
				}
			}
			final = b.String()
			break
		}
	}
	var n analysis.Narrative
	if final == "" {
		return analysis.Narrative{}, bounded.usage, analysis.NarrationError{Category: "missing_final_response"}
	}
	if analysis.Decode([]byte(final), &n) != nil {
		return analysis.Narrative{}, bounded.usage, analysis.NarrationError{Category: "narrative_json"}
	}
	if err := n.Validate(m); err != nil {
		category := "narrative_bounds"
		switch err.Error() {
		case "invalid finding":
			category = "finding_bounds"
		case "unsupported finding metric reference":
			category = "metric_reference"
		case "invalid narrative text length":
			category = "narrative_text"
		}
		return analysis.Narrative{}, bounded.usage, analysis.NarrationError{Category: category}
	}
	return n, bounded.usage, nil
}

// boundedModel reserves a conservative UTF-8-byte token upper bound for each
// serialized request (including history, instructions and tools) before a call.
// Provider usage is required and reconciled after every response. There is no retry.
type boundedModel struct {
	inner   model.LLM
	usage   analysis.Usage
	failure *analysis.NarrationError
}

func (m *boundedModel) Name() string { return m.inner.Name() }
func (m *boundedModel) GenerateContent(ctx context.Context, req *model.LLMRequest, stream bool) iter.Seq2[*model.LLMResponse, error] {
	return func(yield func(*model.LLMResponse, error) bool) {
		if stream || req.Model != analysis.ModelID {
			yield(nil, errors.New("unexpected model or streaming mode"))
			return
		}
		encoded, err := json.Marshal(req)
		reserved := len(encoded) + 1024
		if err != nil || m.usage.Requests >= 3 || m.usage.InputTokens+reserved > 24000 || m.usage.OutputTokens >= 6000 {
			yield(nil, errors.New("model budget exceeded"))
			return
		}
		if req.Config == nil {
			req.Config = &genai.GenerateContentConfig{}
		}
		// Reserve the last permitted turn for the structured report. Read-only
		// tools may use earlier turns, but must not consume the final answer budget.
		if m.usage.Requests == 2 {
			req.Config.ToolConfig = &genai.ToolConfig{FunctionCallingConfig: &genai.FunctionCallingConfig{
				Mode: genai.FunctionCallingConfigModeAny, AllowedFunctionNames: []string{"set_model_response"},
			}}
		} else {
			// A textual early answer bypasses the structured-output tool. Require
			// a declared tool even before the final-turn reservation takes effect.
			req.Config.ToolConfig = &genai.ToolConfig{FunctionCallingConfig: &genai.FunctionCallingConfig{
				Mode: genai.FunctionCallingConfigModeAny, AllowedFunctionNames: []string{"get_cause_metrics", "get_evidence", "set_model_response"},
			}}
		}
		req.Config.MaxOutputTokens = int32(6000 - m.usage.OutputTokens)
		attempts := int32(1)
		req.Config.HTTPOptions = &genai.HTTPOptions{RetryOptions: &genai.HTTPRetryOptions{Attempts: &attempts}}
		m.usage.Requests++
		callCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		responses := 0
		span := telemetry.Begin(ctx, "analysis", "model_rpc")
		failed := true
		defer func() { span.End(failed) }()
		for response, err := range m.inner.GenerateContent(callCtx, req, false) {
			if err != nil {
				diagnostic := providerFailure(err)
				m.failure = &diagnostic
				yield(nil, diagnostic)
				return
			}
			responses++
			if response == nil || responses > 1 || response.UsageMetadata == nil {
				yield(nil, errors.New("missing model usage"))
				return
			}
			usage := response.UsageMetadata
			input := int(usage.PromptTokenCount + usage.ToolUsePromptTokenCount)
			output := int(usage.CandidatesTokenCount + usage.ThoughtsTokenCount)
			if input < 0 || output < 0 || input > reserved || m.usage.InputTokens+input > 24000 || m.usage.OutputTokens+output > 6000 {
				yield(nil, errors.New("model usage exceeds reserved budget"))
				return
			}
			m.usage.InputTokens += input
			m.usage.OutputTokens += output
			m.usage.ModelVersion = response.ModelVersion
			span.Tokens(input, output)
			failed = false
			span.End(false) // Exclude downstream ADK tool handling from this call interval.
			if !yield(response, nil) {
				return
			}
		}
		if responses == 0 {
			yield(nil, errors.New("empty model response"))
		}
	}
}

// Vertex function declarations accept a subset of schema attributes. Keep
// length, pattern and collection limits in Narrative.Validate, not on the wire.
func narrativeSchema(m analysis.Metrics) *genai.Schema {
	str := func(description string) *genai.Schema {
		return &genai.Schema{Type: genai.TypeString, Description: description}
	}
	list := func(description string) *genai.Schema {
		return &genai.Schema{Type: genai.TypeArray, Description: description, Items: str("Nonempty text, at most 2000 UTF-8 bytes.")}
	}
	refs := list("At least one authoritative metric reference.")
	refs.Items.Enum = analysis.MetricReferences(m)
	return &genai.Schema{Type: genai.TypeObject, Required: []string{"summary", "findings", "recommendations", "limitations"}, Properties: map[string]*genai.Schema{
		"summary":         str("Nonempty summary, at most 4000 UTF-8 bytes."),
		"recommendations": list("At most ten recommendations."),
		"limitations":     list("One to ten explicit limitations."),
		"findings": {Type: genai.TypeArray, Description: "At most ten findings.", Items: &genai.Schema{Type: genai.TypeObject, Required: []string{"code", "observation", "metric_refs"}, Properties: map[string]*genai.Schema{
			"code":        str("Lowercase code beginning with a letter, using letters, digits and hyphens, at most 63 bytes."),
			"observation": str("Nonempty observation, at most 2000 UTF-8 bytes."), "metric_refs": refs,
		}}},
	}}
}

// Extract only numeric HTTP status and allowlisted symbols. Never retain the
// provider's message, details, headers, URL or wrapped error text.
func providerFailure(err error) analysis.NarrationError {
	result := analysis.NarrationError{Category: "provider_transport"}
	if errors.Is(err, context.DeadlineExceeded) {
		result.Category = "model_deadline"
		return result
	}
	var api genai.APIError
	if errors.As(err, &api) && api.Code >= 400 && api.Code <= 599 {
		result.Category = "provider_http"
		result.HTTPStatus = api.Code
		switch api.Status {
		case "INVALID_ARGUMENT", "UNAUTHENTICATED", "PERMISSION_DENIED", "NOT_FOUND", "RESOURCE_EXHAUSTED", "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED", "INTERNAL", "UNAVAILABLE", "DEADLINE_EXCEEDED", "CANCELLED", "UNKNOWN":
			result.ProviderStatus = api.Status
		}
	}
	return result
}

// fakeModel exercises the same ADK output-schema tool path without credentials.
type fakeModel struct{}

func (*fakeModel) Name() string { return analysis.ModelID }
func (*fakeModel) GenerateContent(_ context.Context, _ *model.LLMRequest, _ bool) iter.Seq2[*model.LLMResponse, error] {
	return func(yield func(*model.LLMResponse, error) bool) {
		yield(&model.LLMResponse{Content: &genai.Content{Role: "model", Parts: []*genai.Part{{FunctionCall: &genai.FunctionCall{Name: "set_model_response", Args: map[string]any{"summary": "Offline fake-model report; no Vertex AI request was made.", "findings": []any{map[string]any{"code": "coverage", "observation": "Inspect verified coverage before interpreting cause metrics.", "metric_refs": []any{"records_invalid"}}}, "recommendations": []any{"Review observed cause errors before changing sales practice."}, "limitations": []any{"Synthetic input and one shard only; this report tests integration, not model quality."}}}}}}, UsageMetadata: &genai.GenerateContentResponseUsageMetadata{PromptTokenCount: 100, CandidatesTokenCount: 100}, ModelVersion: "offline-fake"}, nil)
	}
}
