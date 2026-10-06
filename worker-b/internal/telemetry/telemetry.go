// Package telemetry emits bounded, payload-free timing and resource observations.
package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

const Version = "pipeline-observation-v1"
const MaxEvents = 128

type Logger struct {
	out io.Writer
	mu  sync.Mutex
}
type budget struct {
	mu sync.Mutex
	n  int
}
type trace struct {
	logger          *Logger
	budget          *budget
	id, event, mode string
	published       *time.Time
}
type key struct{}

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var eventPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var components = map[string]bool{"analysis": true, "storage": true, "index": true, "messaging": true, "api": true, "pipeline": true}
var stages = map[string]bool{"run": true, "evaluation": true, "model_generation": true, "model_rpc": true, "read": true, "create": true, "query": true, "publication": true, "publish_rpc": true, "notification": true, "email": true, "email_rpc": true, "request": true, "launch": true, "observe": true, "lifecycle": true, "initialization": true}

func New(out io.Writer) *Logger { return &Logger{out: out} }
func (l *Logger) Context(ctx context.Context, id string) context.Context {
	return Bind(context.WithValue(ctx, key{}, &trace{logger: l, budget: &budget{}}), id, "")
}
func Bind(ctx context.Context, id, event string) context.Context {
	t, _ := ctx.Value(key{}).(*trace)
	if t == nil {
		return ctx
	}
	next := *t
	next.id, next.event = "", ""
	if idPattern.MatchString(id) {
		next.id = id
	}
	if eventPattern.MatchString(event) {
		next.event = event
	}
	return context.WithValue(ctx, key{}, &next)
}

func Mode(ctx context.Context, mode string) context.Context {
	t, _ := ctx.Value(key{}).(*trace)
	if t == nil {
		return ctx
	}
	next := *t
	next.mode = ""
	if mode == "fake" || mode == "vertex" {
		next.mode = mode
	}
	return context.WithValue(ctx, key{}, &next)
}

// Published records parsed broker metadata, never the raw envelope string.
func Published(ctx context.Context, value string) context.Context {
	t, _ := ctx.Value(key{}).(*trace)
	if t == nil {
		return ctx
	}
	stamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return ctx
	}
	next := *t
	stamp = stamp.UTC()
	next.published = &stamp
	return context.WithValue(ctx, key{}, &next)
}

type Event struct {
	SourcePublishedAt   *time.Time `json:"source_published_at,omitempty"`
	Version             string     `json:"schema_version"`
	Component           string     `json:"component"`
	Stage               string     `json:"stage"`
	AnalysisID          string     `json:"analysis_id,omitempty"`
	EventID             string     `json:"event_id,omitempty"`
	ModelMode           string     `json:"model_mode,omitempty"`
	JobStage            string     `json:"job_stage,omitempty"`
	ExecutionID         string     `json:"execution_id,omitempty"`
	RemoteCreatedAt     *time.Time `json:"remote_created_at,omitempty"`
	RemoteStartedAt     *time.Time `json:"remote_started_at,omitempty"`
	RemoteCompletedAt   *time.Time `json:"remote_completed_at,omitempty"`
	StartedAt           time.Time  `json:"started_at"`
	CompletedAt         time.Time  `json:"completed_at"`
	ElapsedUS           int64      `json:"elapsed_us"`
	Status              string     `json:"status"`
	Sequence            int        `json:"sequence"`
	EventLimitReached   bool       `json:"event_limit_reached,omitempty"`
	Bytes               int64      `json:"bytes,omitempty"`
	InputTokens         int        `json:"input_tokens,omitempty"`
	OutputTokens        int        `json:"output_tokens,omitempty"`
	ProcessCPUUS        *int64     `json:"process_cpu_delta_us,omitempty"`
	ProcessPeakRSSBytes *int64     `json:"process_peak_rss_bytes,omitempty"`
	CgroupMemoryBytes   *int64     `json:"cgroup_memory_current_bytes,omitempty"`
	CgroupPeakBytes     *int64     `json:"cgroup_memory_peak_bytes,omitempty"`
}
type Span struct {
	trace *trace
	event Event
	start time.Time
	cpu   *int64
	once  sync.Once
}

func Begin(ctx context.Context, component, stage string) *Span {
	t, _ := ctx.Value(key{}).(*trace)
	s := &Span{}
	if t == nil || t.logger == nil || t.logger.out == nil || !components[component] || !stages[stage] {
		return s
	}
	s.trace, s.start = t, time.Now()
	s.cpu, _, _, _ = resources()
	s.event = Event{Version: Version, Component: component, Stage: stage, AnalysisID: t.id, EventID: t.event, SourcePublishedAt: t.published, ModelMode: t.mode, StartedAt: s.start.UTC()}
	return s
}

var executionPattern = regexp.MustCompile(`^projects/your-gcp-project-id/locations/us-central1/jobs/poc-gemma-(worker-a-rtx6000|analysis|index|publisher)/executions/([a-z0-9-]{1,80})$`)

func (s *Span) Target(stage string) {
	switch stage {
	case "worker-a", "analysis", "index", "publish":
		s.event.JobStage = stage
	}
}
func (s *Span) Execution(name string) {
	if m := executionPattern.FindStringSubmatch(name); m != nil {
		s.event.ExecutionID = m[2]
	}
}
func (s *Span) Remote(created, started, completed time.Time) {
	if !created.IsZero() {
		v := created.UTC()
		s.event.RemoteCreatedAt = &v
	}
	if !started.IsZero() {
		v := started.UTC()
		s.event.RemoteStartedAt = &v
	}
	if !completed.IsZero() {
		v := completed.UTC()
		s.event.RemoteCompletedAt = &v
	}
}
func Lifecycle(ctx context.Context, stage, execution, created, started, completed string, failed bool) {
	parse := func(v string) time.Time { t, _ := time.Parse(time.RFC3339Nano, v); return t }
	s := Begin(ctx, "pipeline", "lifecycle")
	s.Target(stage)
	s.Execution(execution)
	s.Remote(parse(created), parse(started), parse(completed))
	s.End(failed)
}

func (s *Span) Bytes(n int64) {
	if n >= 0 {
		s.event.Bytes = n
	}
}
func (s *Span) Tokens(input, output int) {
	if input >= 0 && input <= 24000 && output >= 0 && output <= 6000 {
		s.event.InputTokens, s.event.OutputTokens = input, output
	}
}
func (s *Span) Expected(status string) {
	if status == "missing" || status == "exists" {
		s.event.Status = status
	}
}
func (s *Span) End(failed bool) {
	s.once.Do(func() {
		if s.trace == nil {
			return
		}
		end := time.Now()
		s.event.CompletedAt, s.event.ElapsedUS = end.UTC(), end.Sub(s.start).Microseconds()
		if s.event.Status == "" {
			s.event.Status = "ok"
			if failed {
				s.event.Status = "error"
			}
		}
		cpu, rss, current, peak := resources()
		if cpu != nil && s.cpu != nil && *cpu >= *s.cpu {
			delta := *cpu - *s.cpu
			s.event.ProcessCPUUS = &delta
		}
		s.event.ProcessPeakRSSBytes, s.event.CgroupMemoryBytes, s.event.CgroupPeakBytes = rss, current, peak
		b := s.trace.budget
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.n >= MaxEvents {
			return
		}
		b.n++
		s.event.Sequence = b.n
		s.event.EventLimitReached = b.n == MaxEvents
		raw, err := json.Marshal(s.event)
		if err != nil {
			return
		}
		s.trace.logger.mu.Lock()
		defer s.trace.logger.mu.Unlock()
		// Logging failure never changes a pipeline result or retries a side effect.
		_, _ = s.trace.logger.out.Write(append(raw, '\n'))
	})
}

type response struct {
	http.ResponseWriter
	status int
}

func (r *response) Unwrap() http.ResponseWriter { return r.ResponseWriter }
func (r *response) WriteHeader(n int) {
	if r.status == 0 {
		r.status = n
	}
	r.ResponseWriter.WriteHeader(n)
}
func (r *response) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = 200
	}
	return r.ResponseWriter.Write(b)
}

// HTTP creates a separate bounded trace per request without logging paths/headers.
func HTTP(l *Logger, component string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := ""
		if component == "api" && r.Method == "GET" {
			parts := strings.Split(r.URL.Path, "/")
			if (len(parts) == 3 || (len(parts) == 4 && parts[3] == "report")) && parts[1] == "analyses" {
				id = parts[2]
			}
		}
		ctx := l.Context(r.Context(), id)
		s := Begin(ctx, component, "request")
		rw := &response{ResponseWriter: w}
		completed := false
		defer func() { s.End(!completed || rw.status >= 400) }()
		next.ServeHTTP(rw, r.WithContext(ctx))
		completed = true
	})
}
