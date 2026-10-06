// Command data indexes completed reports or serves authenticated read-only APIs.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analytics"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/messaging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "index", "index or api")
	id := flag.String("analysis-id", os.Getenv("INDEX_ANALYSIS_ID"), "completed analysis identifier")
	attempt := flag.String("attempt", "initial", "stable indexing attempt; reuse to reconcile ambiguous submission")
	remote := flag.Bool("remote", false, "explicit GCS/BigQuery access")
	prefix := flag.String("prefix", analytics.Prefix, "local analysis prefix; remote prefix fixed")
	indexPrefix := flag.String("index-prefix", analytics.IndexPrefix, "local index prefix; remote prefix fixed")
	port := flag.String("port", os.Getenv("PORT"), "API port")
	flag.Parse()
	if *mode != "index" && *mode != "api" {
		return fmt.Errorf("unsupported mode")
	}
	if !*remote {
		return fmt.Errorf("local verification uses analytics tests and an injected warehouse; remote access requires --remote")
	}
	if *prefix != analytics.Prefix || *indexPrefix != analytics.IndexPrefix || os.Getenv("GOOGLE_CLOUD_PROJECT") != analysis.Project {
		return fmt.Errorf("fixed project and artifact prefixes required")
	}
	// The storage SDK builds its own HTTP transport; an explicit WithQuotaProject
	// option conflicts with that transport. Scope ADC quota through its supported
	// environment override instead of adding incompatible client options.
	if err := os.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", analysis.Project); err != nil {
		return fmt.Errorf("quota project configuration failed")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := telemetry.New(os.Stderr)
	ctx = logger.Context(ctx, *id)
	store := &analysis.Objects{Remote: true}
	defer store.Close()
	phase := telemetry.Begin(ctx, "index", "initialization")
	warehouse, err := analytics.NewBigQuery(ctx)
	phase.End(err != nil)
	if err != nil {
		return fmt.Errorf("BigQuery ADC initialization failed")
	}
	if *mode == "index" {
		deadline, cancel := context.WithTimeout(ctx, 240*time.Second)
		defer cancel()
		job, err := analytics.Index(deadline, store, warehouse, *prefix, *indexPrefix, *id, *attempt)
		if err != nil {
			return err
		}
		fmt.Printf("analysis_id=%s indexing_status=indexed job_id=%s\n", *id, job)
		return nil
	}
	if *port == "" {
		*port = "8080"
	}
	n, err := strconv.Atoi(*port)
	if err != nil || n < 1024 || n > 65535 {
		return fmt.Errorf("invalid API port")
	}
	api := &analytics.API{Store: store, Warehouse: warehouse, Prefix: *prefix, IndexPrefix: *indexPrefix}
	notifications := &messaging.Engine{Store: store, PublicationPrefix: messaging.PublicationPrefix, NotificationPrefix: messaging.NotificationPrefix}
	api.NotificationStatus = notifications.Status
	server := &http.Server{Addr: ":" + *port, Handler: telemetry.HTTP(logger, "api", api), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Println("mode=api ready=true")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("API server failed")
	}
	return nil
}
