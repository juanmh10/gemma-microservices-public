package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/gemini"
	"google.golang.org/genai"
)

// Exercise the ADK tool loop: the first two turns read metrics and the last
// must be reserved for structured output, without adding a fourth model call.
func TestReadToolsCannotConsumeFinalReportTurn(t *testing.T) {
	inner := &readingModel{}
	r := &Reporter{Instruction: "Use verified metrics.", Model: inner}
	m := analysis.Metrics{RecordsExpected: 900, SourceRecords: 900, PerCause: map[string]benchmark.CauseMetrics{"price": {}}}
	n, usage, err := r.Generate(context.Background(), m, analysis.Decision{Status: "pass"})
	if err != nil || n.Validate(m) != nil || inner.calls != 3 || usage.Requests != 3 || !inner.finalForced {
		t.Fatalf("final report missing within budget: calls=%d usage=%+v error=%v", inner.calls, usage, err)
	}
}

type readingModel struct {
	calls       int
	finalForced bool
}

func (*readingModel) Name() string { return analysis.ModelID }
func (m *readingModel) GenerateContent(ctx context.Context, req *model.LLMRequest, stream bool) iter.Seq2[*model.LLMResponse, error] {
	return func(yield func(*model.LLMResponse, error) bool) {
		m.calls++
		if m.calls == 3 {
			cfg := req.Config.ToolConfig
			m.finalForced = cfg != nil && cfg.FunctionCallingConfig != nil && cfg.FunctionCallingConfig.Mode == genai.FunctionCallingConfigModeAny && reflect.DeepEqual(cfg.FunctionCallingConfig.AllowedFunctionNames, []string{"set_model_response"})
			if m.finalForced {
				for response, err := range (&fakeModel{}).GenerateContent(ctx, req, stream) {
					if !yield(response, err) {
						return
					}
				}
				return
			}
		}
		yield(&model.LLMResponse{Content: &genai.Content{Role: "model", Parts: []*genai.Part{{FunctionCall: &genai.FunctionCall{Name: "get_cause_metrics", Args: map[string]any{"code": "price"}}}}}, UsageMetadata: &genai.GenerateContentResponseUsageMetadata{PromptTokenCount: 100, CandidatesTokenCount: 20}}, nil)
	}
}

func TestFakeExercisesADKStructuredTool(t *testing.T) {
	r := &Reporter{Instruction: "Report only verified metrics.", Fake: true}
	m := analysis.Metrics{RecordsExpected: 300, RecordsInvalid: 3}
	n, u, err := r.Generate(context.Background(), m, analysis.Decision{Status: "not_evaluated"})
	if err != nil {
		t.Fatal(err)
	}
	if n.Validate(m) != nil || u.Requests != 1 || u.ModelVersion != "offline-fake" {
		t.Fatalf("ADK result: %+v %+v", n, u)
	}
}

func TestNarrativeRejectionsHaveSafeCategories(t *testing.T) {
	for _, tc := range []struct{ code, ref, category string }{
		{"invalid_code", "records_invalid", "finding_bounds"},
		{"coverage", "metrics.records_invalid", "metric_reference"},
	} {
		t.Run(tc.category, func(t *testing.T) {
			r := &Reporter{Instruction: "Use verified metrics.", Model: &invalidNarrativeModel{code: tc.code, ref: tc.ref}}
			_, usage, err := r.Generate(context.Background(), analysis.Metrics{RecordsExpected: 900}, analysis.Decision{Status: "pass"})
			var diagnostic analysis.NarrationError
			if !errors.As(err, &diagnostic) || diagnostic.Category != tc.category || usage.Requests != 1 {
				t.Fatalf("unclassified invalid narrative: category=%s usage=%+v", diagnostic.Category, usage)
			}
		})
	}
}

type invalidNarrativeModel struct{ code, ref string }

func (*invalidNarrativeModel) Name() string { return analysis.ModelID }
func (m *invalidNarrativeModel) GenerateContent(ctx context.Context, req *model.LLMRequest, stream bool) iter.Seq2[*model.LLMResponse, error] {
	return func(yield func(*model.LLMResponse, error) bool) {
		for response, err := range (&fakeModel{}).GenerateContent(ctx, req, stream) {
			finding := response.Content.Parts[0].FunctionCall.Args["findings"].([]any)[0].(map[string]any)
			finding["code"] = m.code
			finding["metric_refs"] = []any{m.ref}
			if !yield(response, err) {
				return
			}
		}
	}
}
func TestVertexConfigUsesIdentityAndNoRetries(t *testing.T) {
	c := vertexConfig()
	if c.APIKey != "" || c.Backend != genai.BackendVertexAI || c.Project != analysis.Project || c.Location != "us" || *c.HTTPOptions.RetryOptions.Attempts != 1 {
		t.Fatal("unsafe Vertex config")
	}
}

type failingModel struct{ calls int }

func (*failingModel) Name() string { return analysis.ModelID }
func (m *failingModel) GenerateContent(context.Context, *model.LLMRequest, bool) iter.Seq2[*model.LLMResponse, error] {
	return func(yield func(*model.LLMResponse, error) bool) {
		m.calls++
		yield(nil, errors.New("temporary failure"))
	}
}
func TestNoRetryOnFailure(t *testing.T) {
	inner := &failingModel{}
	m := &boundedModel{inner: inner}
	for _, err := range m.GenerateContent(context.Background(), &model.LLMRequest{Model: analysis.ModelID}, false) {
		if err == nil {
			t.Fatal("failure lost")
		}
	}
	if inner.calls != 1 || m.usage.Requests != 1 {
		t.Fatal("model failure retried")
	}
}
func TestBudgetStopsBeforeRequest(t *testing.T) {
	inner := &failingModel{}
	m := &boundedModel{inner: inner, usage: analysis.Usage{Requests: 3}}
	for range m.GenerateContent(context.Background(), &model.LLMRequest{Model: analysis.ModelID}, false) {
	}
	if inner.calls != 0 {
		t.Fatal("budget allowed another request")
	}
}

// Test the real Gen AI/ADK adapter's retry settings through an in-memory HTTP
// transport, not a fake LLM; no network socket or credential is used.
func TestSDKDoesNotRetry429(t *testing.T) {
	transport := &rateLimitedTransport{}
	cfg := vertexConfig()
	cfg.HTTPClient = &http.Client{Transport: transport}
	llm, err := gemini.NewModel(context.Background(), analysis.ModelID, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sawError := false
	request := &model.LLMRequest{Model: analysis.ModelID, Contents: []*genai.Content{genai.NewContentFromText("Synthetic metrics", genai.RoleUser)}}
	for _, err := range llm.GenerateContent(context.Background(), request, false) {
		sawError = sawError || err != nil
	}
	if !sawError || transport.calls != 1 || transport.host != "aiplatform.us.rep.googleapis.com" {
		t.Fatalf("SDK retried or lost failure: calls=%d", transport.calls)
	}
}

type rateLimitedTransport struct {
	calls int
	host  string
}

func TestSDKTransmitsSupportedNarrativeSchema(t *testing.T) {
	transport := &schemaCaptureTransport{}
	cfg := vertexConfig()
	cfg.HTTPClient = &http.Client{Transport: transport}
	llm, err := gemini.NewModel(context.Background(), analysis.ModelID, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := &Reporter{Instruction: "Use verified metrics.", Model: llm}
	_, usage, runErr := r.Generate(context.Background(), analysis.Metrics{RecordsExpected: 900}, analysis.Decision{Status: "not_evaluated"})
	var diagnostic analysis.NarrationError
	if !errors.As(runErr, &diagnostic) || diagnostic.Category != "provider_http" || diagnostic.HTTPStatus != 429 || diagnostic.ProviderStatus != "RESOURCE_EXHAUSTED" || usage.Requests != 1 {
		t.Fatal("SDK failure lost safe HTTP diagnostics or retried")
	}
	if transport.calls != 1 {
		t.Fatal("unexpected provider call count")
	}
	calling := transport.body["toolConfig"].(map[string]any)["functionCallingConfig"].(map[string]any)
	if calling["mode"] != "ANY" || !reflect.DeepEqual(calling["allowedFunctionNames"], []any{"get_cause_metrics", "get_evidence", "set_model_response"}) {
		t.Fatal("an early response can bypass the structured tool")
	}
	found := false
	for _, item := range transport.body["tools"].([]any) {
		for _, value := range item.(map[string]any)["functionDeclarations"].([]any) {
			declaration := value.(map[string]any)
			if declaration["name"] != "set_model_response" {
				continue
			}
			schema := declaration["parametersJsonSchema"].(map[string]any)
			properties := schema["properties"].(map[string]any)
			finding := properties["findings"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
			refs := finding["metric_refs"].(map[string]any)["items"].(map[string]any)["enum"].([]any)
			encoded, _ := json.Marshal(refs)
			if !strings.Contains(string(encoded), `"records_invalid"`) || strings.Contains(string(encoded), `"cause_f1"`) {
				t.Fatal("provider did not receive authoritative reference and shape constraints")
			}
			var check func(any)
			check = func(value any) {
				switch node := value.(type) {
				case map[string]any:
					for key, child := range node {
						switch key {
						case "minLength", "maxLength", "pattern", "minItems", "maxItems":
							t.Fatalf("unsupported Vertex schema field: %s", key)
						}
						check(child)
					}
				case []any:
					for _, child := range node {
						check(child)
					}
				}
			}
			check(schema)
			found = true
		}
	}
	if !found {
		t.Fatal("structured output tool missing")
	}
}

type schemaCaptureTransport struct {
	calls int
	body  map[string]any
}

func (s *schemaCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	s.calls++
	if err := json.NewDecoder(request.Body).Decode(&s.body); err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: 429, Status: "429 Too Many Requests", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}`)), Request: request}, nil
}

func (r *rateLimitedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	r.calls++
	r.host = request.URL.Host
	if request.URL.Host != "aiplatform.us.rep.googleapis.com" {
		return nil, errors.New("unexpected Vertex endpoint")
	}
	return &http.Response{StatusCode: 429, Status: "429 Too Many Requests", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":429,"message":"synthetic rate limit","status":"RESOURCE_EXHAUSTED"}}`)), Request: request}, nil
}

func TestProviderFailureNeverExposesPayload(t *testing.T) {
	for _, tc := range []struct {
		err      error
		category string
		status   int
	}{
		{genai.APIError{Code: 400, Status: "INVALID_ARGUMENT", Message: "PRIVATE_MARKER", Details: []map[string]any{{"private": "PRIVATE_MARKER"}}}, "provider_http", 400},
		{genai.APIError{Code: 503, Status: "PRIVATE_MARKER", Message: "PRIVATE_MARKER"}, "provider_http", 503},
		{errors.New("PRIVATE_MARKER"), "provider_transport", 0},
		{context.DeadlineExceeded, "model_deadline", 0},
	} {
		d := providerFailure(tc.err)
		raw, _ := json.Marshal(d)
		if d.Category != tc.category || d.HTTPStatus != tc.status || strings.Contains(string(raw), "PRIVATE_MARKER") || strings.Contains(d.Error(), "PRIVATE_MARKER") || !d.SafeProviderDiagnostic() {
			t.Fatal("unsafe provider diagnostic")
		}
	}
}

func TestSDKStructuredResponseCompletesADKWithoutAnotherCall(t *testing.T) {
	transport := &successfulTransport{}
	cfg := vertexConfig()
	cfg.HTTPClient = &http.Client{Transport: transport}
	llm, err := gemini.NewModel(context.Background(), analysis.ModelID, cfg)
	if err != nil {
		t.Fatal(err)
	}
	n, usage, err := (&Reporter{Instruction: "Use verified metrics.", Model: llm}).Generate(context.Background(), analysis.Metrics{RecordsExpected: 900}, analysis.Decision{Status: "not_evaluated"})
	if err != nil || n.Validate(analysis.Metrics{RecordsExpected: 900}) != nil || transport.calls != 1 || usage.Requests != 1 || usage.InputTokens != 100 || usage.OutputTokens != 100 {
		t.Fatalf("SDK/ADK final-response integration failed: calls=%d usage=%+v err=%v", transport.calls, usage, err)
	}
}

type successfulTransport struct{ calls int }

func (s *successfulTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	s.calls++
	var body map[string]any
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		return nil, err
	}
	// A real SDK response fixture, not an LLM interface mock.
	response := `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"set_model_response","args":{"summary":"Synthetic 900-record report","findings":[{"code":"coverage","observation":"Coverage is provided by verified metrics.","metric_refs":["records_expected"]}],"recommendations":[],"limitations":["Local SDK fixture; not live provider validation."]}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":100},"modelVersion":"sdk-fixture"}`
	return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: request}, nil
}
