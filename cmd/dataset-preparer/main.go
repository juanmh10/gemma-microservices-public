package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/juanmh10/gemma-microservices/internal/gcs"
	"github.com/juanmh10/gemma-microservices/internal/preparation"
)

func main() {
	source := flag.String("source", os.Getenv("SOURCE_URI"), "raw dataset JSONL path or gs:// URI")
	metadata := flag.String("metadata", os.Getenv("METADATA_URI"), "raw metadata JSONL path or gs:// URI")
	prepared := flag.String("prepared-prefix", os.Getenv("PREPARED_PREFIX"), "prepared output prefix")
	groundTruth := flag.String("ground-truth-prefix", os.Getenv("GROUND_TRUTH_PREFIX"), "ground truth output prefix")
	runID := flag.String("run-id", os.Getenv("RUN_ID"), "unique run identifier")
	limit := flag.Int("limit", 0, "optional small-batch record limit; zero means all")
	shards := flag.Int("shards", 3, "number of balanced shards")
	promptVersion := flag.String("prompt-version", "worker-a-v1", "versioned worker prompt contract")
	domain := flag.String("domain", "", "versioned domain JSON; required for prompt v3")
	canonical := flag.Bool("canonical", false, "source is canonical dialogue-only JSONL; metadata holds references")
	datasetVersion := flag.String("dataset-version", "gym-sales-v1", "versioned dataset identity")
	flag.Parse()
	store := gcs.New()
	defer store.Close()
	var err error
	if *promptVersion == "worker-a-v3" {
		err = preparation.RunConfigured(context.Background(), store, *source, *metadata, *prepared, *groundTruth, *runID, *datasetVersion, *domain, *canonical, *limit, *shards)
	} else if *canonical || *domain != "" {
		err = fmt.Errorf("canonical/domain options require prompt v3")
	} else {
		err = preparation.Run(context.Background(), store, *source, *metadata, *prepared, *groundTruth, *runID, *promptVersion, *limit, *shards)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
