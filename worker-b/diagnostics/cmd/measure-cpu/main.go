// Command measure-cpu runs bounded local fake-ADK measurements, never cloud jobs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/diagnostics/internal/localmeasure"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	request := flag.String("request", "", "local retained-artifact fake analysis request")
	baseline := flag.String("baseline", "", "validated local metrics.json")
	output := flag.String("output", "", "new directory under an existing local parent")
	count := flag.Int("samples", 3, "sequential samples (1..10)")
	flag.Parse()
	if *request == "" || *baseline == "" || *output == "" || flag.NArg() != 0 {
		return fmt.Errorf("request, baseline and output are required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	raw, err := (&analysis.Objects{}).Read(ctx, *request, 1<<20)
	if err != nil {
		return fmt.Errorf("local request unavailable")
	}
	var req analysis.Request
	if analysis.Decode(raw, &req) != nil {
		return fmt.Errorf("invalid request contract")
	}
	report, err := localmeasure.Run(ctx, req, *baseline, *output, *count)
	if err != nil {
		return err
	}
	fmt.Printf("status=%s samples=%d concurrency=1 model_mode=fake median_us=%.1f\n", report.Status, len(report.Samples), report.Wall.Median)
	return nil
}
