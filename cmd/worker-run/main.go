// Command worker-run executes the Terraform-configured one-task RTX canary.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/juanmh10/gemma-microservices/internal/orchestration"
	run "google.golang.org/api/run/v2"
)

func main() {
	manifest := flag.String("manifest", "", "exact deployed prepared manifest URI")
	results := flag.String("results-prefix", "", "exact deployed isolated canary results prefix")
	execute := flag.Bool("execute", false, "explicitly authorize one billable GPU execution")
	flag.Parse()
	if !*execute || *manifest == "" || *results == "" {
		fmt.Fprintln(os.Stderr, "--execute, --manifest, and --results-prefix are required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service, err := run.NewService(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	execution, err := (orchestration.Monitor{Service: service}).Wait(ctx, *manifest, *results)
	if execution != nil {
		fmt.Printf("execution=%s completed=%s succeeded=%d failed=%d cancelled=%d retried=%d running=%d\n", execution.Name, execution.CompletionTime, execution.SucceededCount, execution.FailedCount, execution.CancelledCount, execution.RetriedCount, execution.RunningCount)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
