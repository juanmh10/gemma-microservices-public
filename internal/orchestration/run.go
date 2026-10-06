// Package orchestration runs one bounded canary without changing Terraform state.
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	run "google.golang.org/api/run/v2"
)

const JobName = "projects/your-gcp-project-id/locations/us-central1/jobs/poc-gemma-worker-a-rtx6000"

type Monitor struct {
	Service        *run.Service
	PollInterval   time.Duration
	CancelAfter    time.Duration
	CleanupTimeout time.Duration
}

func (m Monitor) Wait(ctx context.Context, manifestURI, resultsPrefix string) (*run.GoogleCloudRunV2Execution, error) {
	if m.PollInterval <= 0 {
		m.PollInterval = 15 * time.Second
	}
	if m.CancelAfter <= 0 {
		m.CancelAfter = 14 * time.Minute
	}
	if m.CancelAfter > 14*time.Minute {
		return nil, errors.New("canary cancellation guard exceeds 14 minutes")
	}
	if m.CleanupTimeout <= 0 {
		m.CleanupTimeout = time.Minute
	}
	if m.CleanupTimeout > time.Minute {
		return nil, errors.New("cleanup exceeds the 15-minute budget")
	}
	if !strings.HasPrefix(manifestURI, "gs://your-gcp-project-id-prepared/runs/") || !strings.HasSuffix(manifestURI, "/manifest.json") || !strings.HasPrefix(resultsPrefix, "gs://your-gcp-project-id-results/canary/") {
		return nil, errors.New("canary inputs must use the project prepared and isolated results buckets")
	}
	jobs := m.Service.Projects.Locations.Jobs
	checkCtx, stopCheck := context.WithTimeout(ctx, 30*time.Second)
	defer stopCheck()
	job, err := jobs.Get(JobName).Context(checkCtx).Do()
	if err != nil {
		return nil, err
	}
	if job.Template == nil || job.Template.Template == nil || job.Template.TaskCount != 1 || job.Template.Parallelism != 1 {
		return nil, errors.New("Terraform must configure exactly one task and parallelism one")
	}
	task := job.Template.Template
	if task.MaxRetries != 0 || task.Timeout != "900s" || len(task.Containers) != 1 {
		return nil, errors.New("Terraform must configure zero retries and a 900-second timeout")
	}
	container := task.Containers[0]
	if !strings.Contains(container.Image, "@sha256:") || container.Resources == nil || container.Resources.Limits["nvidia.com/gpu"] != "1" || container.Resources.Limits["cpu"] != "20" || container.Resources.Limits["memory"] != "80Gi" {
		return nil, errors.New("expected one GPU, 20 vCPUs, 80 GiB, and an immutable image")
	}
	env := map[string]string{}
	for _, item := range container.Env {
		env[item.Name] = item.Value
	}
	if env["MANIFEST_URI"] != manifestURI || env["RESULTS_PREFIX"] != resultsPrefix || env["SOFT_DEADLINE_SECONDS"] != "600" || env["TASK_TIMEOUT_SECONDS"] != "900" {
		return nil, errors.New("deployed inputs or deadlines differ from the requested canary")
	}
	started := time.Now()
	runCtx, stopRun := context.WithDeadline(ctx, started.Add(m.CancelAfter))
	defer stopRun()
	// Never retry this mutation: a lost response can mean an execution was created.
	operation, err := jobs.Run(JobName, &run.GoogleCloudRunV2RunJobRequest{Etag: job.Etag}).Context(runCtx).Do()
	if err != nil {
		return nil, fmt.Errorf("run request failed; inspect executions before any further launch: %w", err)
	}
	var execution run.GoogleCloudRunV2Execution
	for {
		for _, raw := range []json.RawMessage{json.RawMessage(operation.Metadata), json.RawMessage(operation.Response)} {
			var candidate run.GoogleCloudRunV2Execution
			if json.Unmarshal(raw, &candidate) == nil && strings.HasPrefix(candidate.Name, JobName+"/executions/") {
				execution = candidate
			}
		}
		if execution.Name != "" {
			break
		}
		if operation.Done || time.Since(started) >= m.CancelAfter || ctx.Err() != nil {
			return nil, errors.New("execution identity unavailable; inspect job executions immediately, do not retry")
		}
		if err := pause(ctx, m.PollInterval); err != nil {
			return nil, err
		}
		operation, err = m.Service.Projects.Locations.Operations.Get(operation.Name).Context(runCtx).Do()
		if err != nil {
			return nil, fmt.Errorf("execution discovery failed; inspect job executions: %w", err)
		}
	}
	for {
		if ctx.Err() != nil || time.Since(started) >= m.CancelAfter {
			return m.cancelAndConfirm(execution.Name)
		}
		current, err := jobs.Executions.Get(execution.Name).Context(runCtx).Do()
		if err != nil {
			return m.cancelAndConfirm(execution.Name)
		}
		if current.CompletionTime != "" && current.RunningCount == 0 {
			// A second independent read verifies that no task remains active.
			confirmed, err := jobs.Executions.Get(execution.Name).Context(runCtx).Do()
			if err != nil {
				return nil, err
			}
			if confirmed.CompletionTime == "" || confirmed.RunningCount != 0 || confirmed.SucceededCount+confirmed.FailedCount+confirmed.CancelledCount != 1 {
				return nil, errors.New("execution termination could not be confirmed")
			}
			if confirmed.SucceededCount != 1 || confirmed.RetriedCount != 0 {
				return confirmed, errors.New("canary failed, cancelled, or retried")
			}
			return confirmed, nil
		}
		if err := pause(ctx, m.PollInterval); err != nil {
			return m.cancelAndConfirm(execution.Name)
		}
	}
}

func (m Monitor) cancelAndConfirm(name string) (*run.GoogleCloudRunV2Execution, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.CleanupTimeout)
	defer cancel()
	_, cancelErr := m.Service.Projects.Locations.Jobs.Executions.Cancel(name, &run.GoogleCloudRunV2CancelExecutionRequest{}).Context(ctx).Do()
	for {
		execution, err := m.Service.Projects.Locations.Jobs.Executions.Get(name).Context(ctx).Do()
		if err == nil && execution.CompletionTime != "" && execution.RunningCount == 0 && execution.SucceededCount+execution.FailedCount+execution.CancelledCount == 1 {
			confirmed, err := m.Service.Projects.Locations.Jobs.Executions.Get(name).Context(ctx).Do()
			if err == nil && confirmed.CompletionTime != "" && confirmed.RunningCount == 0 {
				return confirmed, errors.New("canary stopped by the monitor; termination confirmed")
			}
		}
		if err := pause(ctx, m.PollInterval); err != nil {
			return nil, fmt.Errorf("termination unconfirmed after cancellation (cancel error: %v): %w", cancelErr, err)
		}
	}
}

func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
