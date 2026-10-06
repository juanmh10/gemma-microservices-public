// Command analysis evaluates retained Worker A outputs without invoking GPU jobs.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"os"
	"os/signal"
	"syscall"
	"time"

	prompt "github.com/juanmh10/gemma-microservices/prompts/worker-b"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/agent"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	opts, err := parseOptions(os.Args[1:], os.Getenv("ANALYSIS_REQUEST_URI"))
	if err != nil {
		return err
	}
	request, remote, modelMode := &opts.request, &opts.remote, &opts.model
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := telemetry.New(os.Stderr)
	ctx, cancel := context.WithTimeout(logger.Context(signalCtx, ""), 480*time.Second)
	defer cancel()
	store := &analysis.Objects{Remote: *remote}
	defer store.Close()
	if *remote && (!isRequestURI(*request)) {
		return fmt.Errorf("remote request must use the project analysis-requests prefix")
	}
	raw, err := store.Read(ctx, *request, 1<<20)
	if err != nil {
		return fmt.Errorf("request read failed")
	}
	var req analysis.Request
	if analysis.Decode(raw, &req) != nil {
		return fmt.Errorf("request contract invalid")
	}
	instruction := prompt.ForVersion(req.PromptVersion)
	narrator := &agent.Reporter{Instruction: instruction, Fake: *modelMode == "fake"}
	cfg := analysis.Config{Remote: *remote, ImageDigest: os.Getenv("IMAGE_DIGEST"), SourceRevision: os.Getenv("SOURCE_REVISION"), SourceSHA256: os.Getenv("SOURCE_SHA256"), PromptSHA256: analysis.Digest([]byte(instruction)), ModelMode: *modelMode}
	status, err := analysis.Run(ctx, store, req, narrator, cfg)
	if err != nil {
		return err
	}
	fmt.Printf("analysis_id=%s status=%s model_mode=%s\n", req.AnalysisID, status, *modelMode)
	return nil
}

type options struct {
	request, model string
	remote         bool
}

// Model selection must be explicit before reading inputs or initializing providers.
func parseOptions(args []string, requestURI string) (options, error) {
	var opts options
	flags := flag.NewFlagSet("analysis", flag.ContinueOnError)
	flags.StringVar(&opts.request, "request", requestURI, "immutable request file or allowlisted GCS URI")
	flags.BoolVar(&opts.remote, "remote", false, "read/write GCS; separate from model selection")
	flags.StringVar(&opts.model, "model", "", "required: fake for offline checks or vertex for authorized model calls")
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	if flags.NArg() != 0 || opts.request == "" || (opts.model != "fake" && opts.model != "vertex") {
		return opts, fmt.Errorf("request and explicit supported model mode are required")
	}
	if opts.remote && opts.model != "vertex" {
		return opts, fmt.Errorf("remote execution requires explicit Vertex mode")
	}
	return opts, nil
}

func isRequestURI(uri string) bool {
	const prefix = "gs://your-gcp-project-id-results/analysis-requests/"
	if len(uri) <= len(prefix) || uri[:len(prefix)] != prefix {
		return false
	}
	for _, c := range uri[len(prefix):] {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
