package pipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"time"
)

type Snapshot struct {
	SourceRecovered bool   `json:"source_recovered,omitempty"`
	Completed       bool   `json:"completed"`
	ClaimPresent    bool   `json:"claim_present"`
	ReportSHA256    string `json:"report_sha256,omitempty"`
	Indexed         bool   `json:"indexed"`
	Publication     string `json:"publication_status"`
	Notification    string `json:"notification_status"`
	Email           string `json:"email_status"`
}

type Backend interface {
	Preflight(context.Context, Plan) error
	Snapshot(context.Context, Plan) (Snapshot, error)
	PrepareRequest(context.Context, Plan) error
	Start(context.Context, Plan, string) (Handle, error)
	Wait(context.Context, Plan, string, Handle, func(Handle) error) error
}

type Runner struct {
	Backend             Backend
	Journal             Journal
	PollInterval        time.Duration
	NotificationTimeout time.Duration
}

// Execute has no auto-relaunch path. A persisted unknown launch blocks that
// branch; a known operation/execution resumes observation of the same launch.
func (r *Runner) Execute(ctx context.Context, p Plan, approved string) (Snapshot, error) {
	if err := Authorized(p, true, approved); err != nil {
		return Snapshot{}, err
	}
	if r.Backend == nil || r.Journal == nil {
		return Snapshot{}, errors.New("pipeline dependencies unavailable")
	}
	if r.NotificationTimeout > 120*time.Second {
		return Snapshot{}, errors.New("notification wait exceeds bounded limit")
	}
	state, err := r.Journal.Load()
	if err != nil {
		return Snapshot{}, err
	}
	if state.Version == "" {
		state = State{Version: Version, PlanSHA256: p.Digest(), Stages: map[string]Stage{}}
	}
	if state.Version != Version || state.PlanSHA256 != p.Digest() || state.Stages == nil {
		return Snapshot{}, errors.New("journal differs from approved plan")
	}
	if state.PreparedRequest != nil && p.Preparation == nil {
		return Snapshot{}, errors.New("unexpected preparation journal")
	}
	for stage, record := range state.Stages {
		if record.Handle.SourceRecovered && stage != "worker-a" {
			return Snapshot{}, errors.New("source recovery only applies to Worker A")
		}
		if stage == "worker-a" && record.Requested {
			if _, err := time.Parse(time.RFC3339Nano, record.Handle.RequestedAt); err != nil {
				return Snapshot{}, errors.New("GPU launch timestamp unavailable; do not relaunch")
			}
		}
		if (!record.Requested && (record.Handle.Operation != "" || record.Handle.Execution != "")) || (stage == "analysis" && p.AnalysisMode != "analyze") || p.Job(stage) == "" || (record.Handle.Operation != "" && !operationValid(record.Handle.Operation)) || (record.Handle.Execution != "" && !executionValid(p.Job(stage), record.Handle.Execution)) {
			return Snapshot{}, errors.New("invalid journal identity")
		}
	}
	if err = r.Journal.Save(state); err != nil {
		return Snapshot{}, err
	}
	if err = r.Backend.Preflight(ctx, p); err != nil {
		return Snapshot{}, errors.New("pipeline preflight failed; no job launched")
	}
	var snapshot Snapshot
	if p.WorkerA == nil {
		snapshot, err = r.Backend.Snapshot(ctx, p)
		if err != nil {
			return snapshot, errors.New("analysis artifacts unavailable")
		}
	}
	persistenceFailed := false
	saveState := func() error {
		err := r.Journal.Save(state)
		if err != nil {
			persistenceFailed = true
		}
		return err
	}
	ensure := func(stage string, ready bool, force bool) error {
		record := state.Stages[stage]
		if record.Done {
			if !ready {
				return errors.New("completed stage artifact unavailable")
			}
			return nil
		}
		if !record.Requested && ready && !force {
			record.Done = true
			state.Stages[stage] = record
			return saveState()
		}
		if !record.Requested {
			record.Requested = true
			if stage == "worker-a" {
				record.Handle.RequestedAt = time.Now().UTC().Format(time.RFC3339Nano)
			}
			state.Stages[stage] = record
			if err := saveState(); err != nil {
				return err
			}
			phase := telemetry.Begin(ctx, "pipeline", "launch")
			phase.Target(stage)
			handle, err := r.Backend.Start(ctx, p, stage)
			phase.Execution(handle.Execution)
			phase.End(err != nil)
			if err != nil {
				return errors.New("launch ambiguous; inspect executions, never relaunch automatically")
			}
			handle.RequestedAt = record.Handle.RequestedAt
			record.Handle = handle
			state.Stages[stage] = record
			if err = saveState(); err != nil {
				return err
			}
		}
		if record.Handle.Operation == "" && record.Handle.Execution == "" {
			return errors.New("launch identity unknown; manual reconciliation required")
		}
		phase := telemetry.Begin(ctx, "pipeline", "observe")
		phase.Target(stage)
		phase.Execution(record.Handle.Execution)
		err := r.Backend.Wait(ctx, p, stage, record.Handle, func(h Handle) error {
			phase.Execution(h.Execution)
			record.Handle = h
			state.Stages[stage] = record
			return saveState()
		})
		phase.End(err != nil)
		if err != nil {
			return fmt.Errorf("job not confirmed successful; resume its recorded identity: %w", err)
		}
		record.Done = true
		state.Stages[stage] = record
		return saveState()
	}
	if p.Preparation != nil {
		prep, ok := r.Backend.(PreparationBackend)
		if !ok {
			return snapshot, errors.New("preparation backend unavailable")
		}
		request, ready, err := prep.ResolvePreparation(ctx, p)
		if err != nil {
			return snapshot, errors.New("preparation validation failed")
		}
		if !state.Stages["prepare"].Requested && ready {
			return snapshot, errors.New("fresh workload cannot reuse prepared outputs")
		}
		if err = ensure("prepare", ready, false); err != nil {
			return snapshot, err
		}
		request, ready, err = prep.ResolvePreparation(ctx, p)
		if err != nil || !ready {
			return snapshot, errors.New("preparation completed outputs unavailable; no GPU launch")
		}
		if state.PreparedRequest != nil && !preparedEqual(state.PreparedRequest, request) {
			return snapshot, errors.New("prepared outputs differ from journal")
		}
		state.PreparedRequest = request
		if err = saveState(); err != nil {
			return snapshot, err
		}
		p.Request = request
		if !state.Stages["worker-a"].Requested {
			if err := prep.VerifyFreshWorker(ctx, p); err != nil {
				return snapshot, err
			}
		}
	}
	if p.WorkerA != nil {
		source, ok := r.Backend.(SourceBackend)
		if !ok {
			return snapshot, errors.New("Worker A source backend unavailable")
		}
		request, ready, err := source.ResolveSource(ctx, p)
		if err != nil {
			return snapshot, errors.New("Worker A source validation failed")
		}
		if err = ensure("worker-a", ready, false); err != nil {
			return snapshot, err
		}
		request, ready, err = source.ResolveSource(ctx, p)
		if err != nil || !ready {
			return snapshot, errors.New("Worker A completed source unavailable; never retry GPU")
		}
		p.Request = request
		snapshot, err = r.Backend.Snapshot(ctx, p)
		if err != nil {
			return snapshot, errors.New("resolved analysis status unavailable")
		}
	}
	if !snapshot.Completed && p.AnalysisMode == "reuse" {
		return snapshot, errors.New("reuse requires a validated completed report")
	}
	if !snapshot.Completed && p.AnalysisMode == "analyze" {
		if snapshot.ClaimPresent && !state.Stages["analysis"].Requested {
			return snapshot, errors.New("existing analysis claim requires manual reconciliation")
		}
		if err = r.Backend.PrepareRequest(ctx, p); err != nil {
			return snapshot, errors.New("immutable analysis request unavailable")
		}
	}
	if p.AnalysisMode == "analyze" {
		if err = ensure("analysis", snapshot.Completed, false); err != nil {
			return snapshot, err
		}
		snapshot, err = r.Backend.Snapshot(ctx, p)
		if err != nil || !snapshot.Completed {
			return snapshot, errors.New("analysis completed artifact unavailable")
		}
	}
	// Index failure is recorded, but publication remains independent.
	indexErr := ensure("index", snapshot.Indexed, false)
	if persistenceFailed {
		return snapshot, errors.New("journal persistence failed; no further launch authorized")
	}
	snapshot, err = r.Backend.Snapshot(ctx, p)
	if err != nil {
		return snapshot, errors.New("stage status unavailable")
	}
	if snapshot.Publication == "pending" && !p.Republish && !state.Stages["publish"].Requested {
		return snapshot, errors.New("publication intent pending; explicitly reviewed republish plan required")
	}
	if snapshot.Publication == "unavailable" {
		return snapshot, errors.New("publication artifact unavailable")
	}
	if err = ensure("publish", snapshot.Publication == "published", p.Republish); err != nil {
		return snapshot, err
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	timeout := r.NotificationTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		snapshot, err = r.Backend.Snapshot(waitCtx, p)
		snapshot.SourceRecovered = state.Stages["worker-a"].Handle.SourceRecovered
		if err != nil {
			return snapshot, errors.New("notification status unavailable")
		}
		if snapshot.Email == "rejected" || snapshot.Email == "unavailable" || snapshot.Notification == "unavailable" {
			return snapshot, errors.New("notification requires reconciliation")
		}
		if snapshot.Publication == "published" && snapshot.Notification == "delivered" && (!p.RequireEmail || snapshot.Email == "accepted") {
			if indexErr != nil || !snapshot.Indexed {
				return snapshot, errors.New("notification completed; indexing still requires reconciliation")
			}
			return snapshot, nil
		}
		if err = pause(waitCtx, interval); err != nil {
			return snapshot, errors.New("notification remains pending; resume observation without republish")
		}
	}
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
