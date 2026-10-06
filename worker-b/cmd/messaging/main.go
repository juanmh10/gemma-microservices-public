// Command messaging publishes existing analyses or receives authenticated push.
package main

import (
	"context"
	"errors"
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
	mode := flag.String("mode", "publish", "publish or notify")
	remote := flag.Bool("remote", false, "explicit fixed-project cloud access")
	id := flag.String("analysis-id", os.Getenv("PUBLISH_ANALYSIS_ID"), "completed analysis ID")
	replay := flag.Bool("republish", false, "explicit replay of the same immutable event")
	flag.Parse()
	if !*remote || os.Getenv("GOOGLE_CLOUD_PROJECT") != analysis.Project || (*mode != "publish" && *mode != "notify") {
		return errors.New("explicit remote mode and fixed project required")
	}
	if err := os.Setenv("GOOGLE_CLOUD_QUOTA_PROJECT", analysis.Project); err != nil {
		return errors.New("quota configuration failed")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := telemetry.New(os.Stderr)
	ctx = logger.Context(ctx, *id)
	store := &analysis.Objects{Remote: true}
	defer store.Close()
	engine := &messaging.Engine{Store: store, PublicationPrefix: messaging.PublicationPrefix, NotificationPrefix: messaging.NotificationPrefix}
	engine.Load = func(ctx context.Context, id string) (analysis.Report, string, error) {
		r, hash, err := analytics.Load(ctx, store, analytics.Prefix, id)
		if errors.Is(err, analytics.ErrInvalid) {
			err = messaging.ErrInvalid
		}
		return r, hash, err
	}
	if *mode == "publish" {
		if !analytics.IDValid(*id) {
			return errors.New("invalid analysis ID")
		}
		deadline, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		phase := telemetry.Begin(deadline, "messaging", "initialization")
		pub, err := messaging.NewPubSub(deadline)
		phase.End(err != nil)
		if err != nil {
			return errors.New("Pub/Sub initialization failed")
		}
		e, err := engine.Publish(deadline, pub, *id, *replay)
		if err != nil {
			return errors.New("publication pending or invalid; inspect receipts before explicit replay")
		}
		fmt.Printf("analysis_id=%s stage=publication status=published event_id=%s\n", e.AnalysisID, e.EventID)
		return nil
	}
	if os.Getenv("EMAIL_ENABLED") == "true" {
		sender, err := messaging.NewResend(os.Getenv("RESEND_API_KEY"))
		if err != nil {
			return err
		}
		engine.Email, err = messaging.NewEmailDelivery(sender, os.Getenv("EMAIL_FROM"), os.Getenv("EMAIL_TO"), os.Getenv("EMAIL_SENDER_DOMAIN"))
		if err != nil {
			return err
		}
	} else if value := os.Getenv("EMAIL_ENABLED"); value != "" && value != "false" {
		return errors.New("invalid email adapter configuration")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1024 || n > 65535 {
		return errors.New("invalid port")
	}
	server := &http.Server{Addr: ":" + port, Handler: telemetry.HTTP(logger, "messaging", &messaging.Push{Engine: engine}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() {
		<-ctx.Done()
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(deadline)
	}()
	fmt.Println("mode=notify ready=true")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return errors.New("notifier server failed")
	}
	return nil
}
