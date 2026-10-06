// Command benchmark-report evaluates completed Worker A outputs locally.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/juanmh10/gemma-microservices/internal/gcs"
)

func main() {
	truthPath := flag.String("truth", "", "local isolated labels.parquet")
	l4Dir := flag.String("l4", "", "optional local L4 output directory")
	rtxDir := flag.String("rtx", "", "optional local RTX output directory")
	tasks := flag.Int("tasks", 3, "three full-pilot tasks or one canary task")
	taskIndex := flag.Int("task-index", 0, "manifest shard selected by a one-task canary")
	manifestPath := flag.String("manifest", "", "original manifest file; required for canaries")
	shardPath := flag.String("shard", "", "exact compressed canary shard; required for canaries")
	output := flag.String("output", "", "optional immutable report path or gs:// URI")
	flag.Parse()
	if strings.HasPrefix(*output, "gs://") && !strings.HasPrefix(*output, "gs://your-gcp-project-id-results/") {
		fmt.Fprintln(os.Stderr, "remote reports must use the project results bucket")
		os.Exit(2)
	}
	if *truthPath == "" || (*l4Dir == "" && *rtxDir == "") {
		fmt.Fprintln(os.Stderr, "--truth and at least one of --l4 or --rtx are required")
		os.Exit(2)
	}
	truth, err := benchmark.ReadTruth(*truthPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	options := benchmark.Options{Tasks: *tasks, TaskIndex: *taskIndex, ManifestPath: *manifestPath, ShardPath: *shardPath}
	reports := map[string]benchmark.Report{}
	for profile, dir := range map[string]string{"l4": *l4Dir, "rtx6000": *rtxDir} {
		if dir == "" {
			continue
		}
		report, err := benchmark.Evaluate(dir, profile, truth, options)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		reports[profile] = report
	}
	if len(reports) == 2 {
		l4, rtx := reports["l4"], reports["rtx6000"]
		if l4.RunID != rtx.RunID || l4.ManifestSHA256 != rtx.ManifestSHA256 || l4.PromptVersion != rtx.PromptVersion || l4.PromptSHA256 != rtx.PromptSHA256 || l4.DomainSHA256 != rtx.DomainSHA256 || l4.DomainVersion != rtx.DomainVersion {
			fmt.Fprintln(os.Stderr, "profiles used different runs, manifests, or prompts")
			os.Exit(1)
		}
	}
	if *output != "" {
		payload, err := json.MarshalIndent(reports, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		store := gcs.New()
		defer store.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := store.Put(ctx, *output, bytes.NewReader(append(payload, '\n'))); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("report=%s\n", *output)
		return
	}
	if err := json.NewEncoder(os.Stdout).Encode(reports); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
