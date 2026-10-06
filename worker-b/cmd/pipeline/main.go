// Command pipeline reviews a local plan and, only after approval, coordinates bounded pipeline Jobs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/pipeline"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("pipeline", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	filename := flags.String("plan", "", "local versioned plan file")
	execute := flags.Bool("execute", false, "execute only after explicit operator approval")
	approved := flags.String("approved-plan-sha256", "", "digest of the explicitly approved plan")
	workspace := flags.String("state-dir", "results/pipeline", "private operator journal workspace; preserve across resume")
	if flags.Parse(args) != nil || *filename == "" || flags.NArg() != 0 {
		return errors.New("a local plan file and supported flags are required")
	}
	raw, err := (&analysis.Objects{}).Read(context.Background(), *filename, 1<<20)
	if err != nil {
		return errors.New("local plan read failed")
	}
	var p pipeline.Plan
	if analysis.Decode(raw, &p) != nil || p.Validate() != nil {
		return errors.New("invalid pipeline plan")
	}
	if !*execute {
		_, err = fmt.Fprintf(out, "plan_sha256=%s execution=blocked analysis_mode=%s analysis_id=%s\n", p.Digest(), p.AnalysisMode, p.AnalysisID)
		if err != nil {
			return err
		}
		_, err = out.Write(analysis.JSON(p))
		return err
	}
	if err = pipeline.Authorized(p, *execute, *approved); err != nil {
		return err
	}
	journal, err := pipeline.OpenJournal(*workspace, p)
	if err != nil {
		return err
	}
	defer journal.Close()
	if err = os.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", analysis.Project); err != nil {
		return errors.New("quota configuration unavailable")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = telemetry.New(os.Stderr).Context(ctx, p.AnalysisID)
	observationBudget := 30 * time.Minute
	if p.WorkerA != nil {
		observationBudget = 45 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, observationBudget)
	defer cancel()
	store := &analysis.Objects{Remote: true}
	defer store.Close()
	phase := telemetry.Begin(ctx, "pipeline", "initialization")
	backend, err := pipeline.NewCloud(ctx, store)
	phase.End(err != nil)
	if err != nil {
		return err
	}
	runner := pipeline.Runner{Backend: backend, Journal: journal}
	status, err := runner.Execute(ctx, p, *approved)
	if _, writeErr := out.Write(analysis.JSON(status)); writeErr != nil {
		return writeErr
	}
	return err
}
