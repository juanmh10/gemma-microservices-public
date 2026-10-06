package pipeline

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

func example(t *testing.T) Plan {
	t.Helper()
	raw, err := os.ReadFile("testdata/plan-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var p Plan
	if err := analysis.Decode(raw, &p); err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}
func analyzePlan(t *testing.T) Plan {
	t.Helper()
	p := example(t)
	raw, err := os.ReadFile("testdata/request-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var req analysis.Request
	if err := analysis.Decode(raw, &req); err != nil {
		t.Fatal(err)
	}
	req.AnalysisID = "coordinator-test-01"
	p.AnalysisID = req.AnalysisID
	p.AnalysisMode = "analyze"
	p.ExpectedReportSHA256 = ""
	p.Request = &req
	p.AnalysisImage = "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/analysis@sha256:" + strings.Repeat("a", 64)
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}

type memoryJournal struct {
	state  State
	writes int
	failAt int
}

func (j *memoryJournal) Load() (State, error) {
	var s State
	if j.state.Version != "" {
		_ = analysis.Decode(analysis.JSON(j.state), &s)
	}
	return s, nil
}
func (j *memoryJournal) Save(s State) error {
	j.writes++
	if j.writes == j.failAt {
		return errors.New("disk failure")
	}
	return analysis.Decode(analysis.JSON(s), &j.state)
}

type fakeBackend struct {
	snapshot     Snapshot
	calls        int
	starts       []string
	waits        []string
	prepared     int
	startErr     string
	waitErr      string
	preflightErr bool
	pendingEmail bool
}

func (b *fakeBackend) Preflight(context.Context, Plan) error {
	b.calls++
	if b.preflightErr {
		return errors.New("drift")
	}
	return nil
}
func (b *fakeBackend) Snapshot(context.Context, Plan) (Snapshot, error) {
	b.calls++
	return b.snapshot, nil
}
func (b *fakeBackend) PrepareRequest(context.Context, Plan) error {
	b.calls++
	b.prepared++
	return nil
}
func (b *fakeBackend) Start(_ context.Context, p Plan, s string) (Handle, error) {
	b.calls++
	b.starts = append(b.starts, s)
	if b.startErr == s {
		return Handle{}, errors.New("lost acknowledgment")
	}
	return Handle{Operation: Root + "/operations/test-" + s}, nil
}
func (b *fakeBackend) Wait(_ context.Context, p Plan, s string, h Handle, save func(Handle) error) error {
	b.calls++
	b.waits = append(b.waits, s)
	if h.Execution == "" {
		h.Execution = p.Job(s) + "/executions/test-" + s
		if err := save(h); err != nil {
			return err
		}
	}
	if b.waitErr == s {
		return errors.New("observation interrupted")
	}
	switch s {
	case "analysis":
		b.snapshot.Completed = true
	case "index":
		b.snapshot.Indexed = true
	case "publish":
		b.snapshot.Publication = "published"
		b.snapshot.Notification = "delivered"
		if !b.pendingEmail {
			b.snapshot.Email = "accepted"
		}
	}
	return nil
}
func runner(b *fakeBackend, j Journal) Runner {
	return Runner{Backend: b, Journal: j, PollInterval: time.Millisecond, NotificationTimeout: 10 * time.Millisecond}
}
func ready() Snapshot {
	return Snapshot{Completed: true, Indexed: true, Publication: "published", Notification: "delivered", Email: "accepted"}
}

func TestApprovalAndBoundsBeforeAccess(t *testing.T) {
	p := example(t)
	for _, test := range []struct {
		approval string
		timeout  time.Duration
	}{{"", 0}, {strings.Repeat("b", 64), 0}, {p.Digest(), 121 * time.Second}} {
		b := &fakeBackend{}
		j := &memoryJournal{}
		r := runner(b, j)
		r.NotificationTimeout = test.timeout
		if _, err := r.Execute(context.Background(), p, test.approval); err == nil {
			t.Fatal("unapproved/unbounded execution accepted")
		}
		if b.calls != 0 || j.writes != 0 {
			t.Fatal("access before approval/bounds validation")
		}
	}
	changed := p
	changed.Republish = true
	if Authorized(changed, true, p.Digest()) == nil {
		t.Fatal("changed plan inherited approval")
	}
}
func TestAnalyzeAndCompletedResume(t *testing.T) {
	p := analyzePlan(t)
	b := &fakeBackend{}
	j := &memoryJournal{}
	r := runner(b, j)
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.starts, []string{"analysis", "index", "publish"}) || b.prepared != 1 {
		t.Fatalf("unexpected stages: %v", b.starts)
	}
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	if len(b.starts) != 3 || len(b.waits) != 3 || b.prepared != 1 {
		t.Fatal("completed resume launched or waited again")
	}
}
func TestUnknownAcknowledgmentNeverRelaunched(t *testing.T) {
	p := example(t)
	b := &fakeBackend{snapshot: ready(), startErr: "index"}
	b.snapshot.Indexed = false
	j := &memoryJournal{}
	r := runner(b, j)
	for range 2 {
		if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil {
			t.Fatal("ambiguous launch succeeded")
		}
	}
	if !reflect.DeepEqual(b.starts, []string{"index"}) || !j.state.Stages["index"].Requested || j.state.Stages["index"].Handle != (Handle{}) {
		t.Fatal("ambiguous launch was retried or forgotten")
	}
}
func TestKnownExecutionResumedWithoutNewLaunch(t *testing.T) {
	p := example(t)
	b := &fakeBackend{snapshot: ready(), waitErr: "index"}
	b.snapshot.Indexed = false
	j := &memoryJournal{}
	r := runner(b, j)
	if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil {
		t.Fatal("interrupted observation succeeded")
	}
	if j.state.Stages["index"].Handle.Execution == "" {
		t.Fatal("execution identity not checkpointed")
	}
	b.waitErr = ""
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.starts, []string{"index"}) || len(b.waits) != 2 {
		t.Fatal("resume launched again")
	}
}
func TestIndexFailureDoesNotBlockPublication(t *testing.T) {
	p := example(t)
	b := &fakeBackend{snapshot: Snapshot{Completed: true}, waitErr: "index"}
	j := &memoryJournal{}
	r := runner(b, j)
	s, err := r.Execute(context.Background(), p, p.Digest())
	if err == nil || s.Email != "accepted" || !reflect.DeepEqual(b.starts, []string{"index", "publish"}) {
		t.Fatalf("independent publication failed: %+v %v", s, err)
	}
}
func TestNotificationResumeDoesNotRepublish(t *testing.T) {
	p := example(t)
	b := &fakeBackend{snapshot: ready(), pendingEmail: true}
	b.snapshot.Publication = ""
	b.snapshot.Notification = ""
	b.snapshot.Email = ""
	j := &memoryJournal{}
	r := runner(b, j)
	if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil {
		t.Fatal("pending notification accepted")
	}
	b.snapshot.Email = "accepted"
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.starts, []string{"publish"}) {
		t.Fatal("notification resume republished")
	}
}
func TestPublicationRecoveryIsExplicit(t *testing.T) {
	p := example(t)
	b := &fakeBackend{snapshot: ready()}
	b.snapshot.Publication = "pending"
	j := &memoryJournal{}
	r := runner(b, j)
	if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil || len(b.starts) != 0 {
		t.Fatal("pending intent was replayed")
	}
	p.Republish = true
	j = &memoryJournal{}
	r = runner(b, j)
	for range 2 {
		if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(b.starts, []string{"publish"}) {
		t.Fatal("explicit replay repeated on resume")
	}
}
func TestPreflightJournalAndPersistenceFailuresPreventLaunch(t *testing.T) {
	p := example(t)
	t.Run("preflight", func(t *testing.T) {
		b := &fakeBackend{preflightErr: true}
		r := runner(b, &memoryJournal{})
		if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil || len(b.starts) != 0 {
			t.Fatal("drift did not block launch")
		}
	})
	t.Run("journal mismatch", func(t *testing.T) {
		b := &fakeBackend{}
		j := &memoryJournal{state: State{Version: Version, PlanSHA256: strings.Repeat("0", 64), Stages: map[string]Stage{}}}
		r := runner(b, j)
		if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil || b.calls != 0 {
			t.Fatal("mismatched journal accessed backend")
		}
	})
	t.Run("persist before mutation", func(t *testing.T) {
		b := &fakeBackend{snapshot: Snapshot{Completed: true}}
		j := &memoryJournal{failAt: 2}
		r := runner(b, j)
		_, _ = r.Execute(context.Background(), p, p.Digest())
		if len(b.starts) != 0 {
			t.Fatal("mutation preceded durable request checkpoint")
		}
	})
	t.Run("unrelated execution", func(t *testing.T) {
		b := &fakeBackend{}
		j := &memoryJournal{state: State{Version: Version, PlanSHA256: p.Digest(), Stages: map[string]Stage{"index": {Requested: true, Handle: Handle{Execution: p.Job("publish") + "/executions/foreign"}}}}}
		r := runner(b, j)
		if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil || b.calls != 0 {
			t.Fatal("foreign execution accepted")
		}
	})
	t.Run("claimed analysis", func(t *testing.T) {
		p := analyzePlan(t)
		b := &fakeBackend{snapshot: Snapshot{ClaimPresent: true}}
		r := runner(b, &memoryJournal{})
		if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil || len(b.starts) != 0 || b.prepared != 0 {
			t.Fatal("claimed analysis relaunched")
		}
	})
}
func TestFileJournalExclusiveAndDurable(t *testing.T) {
	p := example(t)
	dir := t.TempDir()
	j, err := OpenJournal(dir, p)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err := OpenJournal(dir, p); err == nil {
		t.Fatal("second operator admitted")
	}
	s := State{Version: Version, PlanSHA256: p.Digest(), Stages: map[string]Stage{"publish": {Requested: true, Handle: Handle{Operation: Root + "/operations/test"}}}}
	if err := j.Save(s); err != nil {
		t.Fatal(err)
	}
	got, err := j.Load()
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatal("durable checkpoint differs")
	}
	st, err := os.Stat(j.path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("journal permissions")
	}
	if err := os.WriteFile(j.path, append(analysis.JSON(s), []byte(" {}")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Load(); err == nil {
		t.Fatal("trailing journal JSON accepted")
	}
}

func TestFixedPlanScopeAndMetadataOnlySources(t *testing.T) {
	for name, change := range map[string]func(*Plan){
		"project":         func(p *Plan) { p.Project = "foreign-project" },
		"region":          func(p *Plan) { p.Region = "europe-west1" },
		"mutable image":   func(p *Plan) { p.DataImage = "mutable:latest" },
		"fake model":      func(p *Plan) { p.Request.ModelMode = "fake" },
		"source metadata": func(p *Plan) { p.Request.Marker.URI += "/private@address" },
	} {
		t.Run(name, func(t *testing.T) {
			p := analyzePlan(t)
			change(&p)
			b := &fakeBackend{}
			j := &memoryJournal{}
			r := runner(b, j)
			if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil || b.calls != 0 || j.writes != 0 {
				t.Fatal("invalid scope reached backend")
			}
		})
	}
}

type gpuBackend struct {
	fakeBackend
	sourceReady bool
	resolves    int
}

func (b *gpuBackend) ResolveSource(_ context.Context, p Plan) (*analysis.Request, bool, error) {
	b.resolves++
	if !b.sourceReady {
		return nil, false, nil
	}
	request := *p.Request
	request.Marker = analysis.ObjectRef{URI: p.WorkerTaskPrefix() + "/_SUCCESS.json", SHA256: strings.Repeat("d", 64)}
	return &request, true, nil
}
func (b *gpuBackend) Wait(ctx context.Context, p Plan, s string, h Handle, save func(Handle) error) error {
	if s == "worker-a" {
		if h.RequestedAt == "" {
			return errors.New("missing GPU timestamp")
		}
		if err := b.fakeBackend.Wait(ctx, p, s, h, save); err != nil {
			return err
		}
		b.sourceReady = true
		return nil
	}
	return b.fakeBackend.Wait(ctx, p, s, h, save)
}
func fullPlan(t *testing.T) Plan {
	p := analyzePlan(t)
	p.Request.SourceImage = CachedWorkerImage
	p.WorkerA = &WorkerA{CanaryID: "pipeline-test-01", Image: CachedWorkerImage, GitSHA: CachedWorkerRevision}
	if p.Validate() != nil {
		t.Fatal("invalid full fixture")
	}
	return p
}
func TestFullPipelineResumesAfterDownstreamFailureWithoutGPU(t *testing.T) {
	p := fullPlan(t)
	b := &gpuBackend{}
	b.waitErr = "index"
	j := &memoryJournal{}
	r := Runner{Backend: b, Journal: j, PollInterval: time.Millisecond, NotificationTimeout: 10 * time.Millisecond}
	if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil {
		t.Fatal("index failure hidden")
	}
	if !reflect.DeepEqual(b.starts, []string{"worker-a", "analysis", "index", "publish"}) || !j.state.Stages["worker-a"].Done || j.state.Stages["worker-a"].Handle.RequestedAt == "" {
		t.Fatalf("full flow incomplete: %v", b.starts)
	}
	b.waitErr = ""
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	if len(b.starts) != 4 || b.prepared != 1 {
		t.Fatal("downstream recovery relaunched GPU/model")
	}
}
func TestGPUUnknownLaunchNeverRetries(t *testing.T) {
	p := fullPlan(t)
	b := &gpuBackend{}
	b.startErr = "worker-a"
	j := &memoryJournal{}
	r := Runner{Backend: b, Journal: j}
	for range 2 {
		if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil {
			t.Fatal("unknown launch accepted")
		}
	}
	if !reflect.DeepEqual(b.starts, []string{"worker-a"}) || j.state.Stages["worker-a"].Handle.RequestedAt == "" {
		t.Fatal("unknown GPU launch lost or retried")
	}
}
