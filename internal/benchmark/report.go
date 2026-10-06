// Package benchmark evaluates persisted predictions against isolated ground truth.
package benchmark

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

type TruthRow struct {
	RecordID     string `parquet:"record_id"`
	Outcome      string `parquet:"outcome"`
	MetadataJSON string `parquet:"metadata_json"`
}

type Cause struct {
	Code          string  `parquet:"code"`
	Confidence    float32 `parquet:"confidence"`
	EvidenceTurns []int32 `parquet:"evidence_turns,list"`
}

type PredictionRow struct {
	ConversationID string  `parquet:"conversation_id"`
	Status         string  `parquet:"status"`
	Outcome        string  `parquet:"outcome"`
	Causes         []Cause `parquet:"causes,list"`
	InputTokens    int32   `parquet:"input_tokens"`
	OutputTokens   int32   `parquet:"output_tokens"`
	DurationMS     int64   `parquet:"duration_ms"`
}

type Marker struct {
	RunID            string `json:"run_id"`
	TaskIndex        int    `json:"task_index"`
	GPUProfile       string `json:"gpu_profile"`
	Status           string `json:"status"`
	ManifestSHA256   string `json:"manifest_sha256"`
	RecordsTotal     int    `json:"records_total"`
	RecordsProcessed int    `json:"records_processed"`
	DurationMS       int64  `json:"duration_ms"`
	ModelLoadMS      int64  `json:"model_load_ms"`
	ModelRevision    string `json:"model_revision"`
	Quantization     string `json:"quantization"`
	VLLMVersion      string `json:"vllm_version"`
	DomainVersion    string `json:"domain_version,omitempty"`
	DomainSHA256     string `json:"domain_sha256,omitempty"`
	PromptVersion    string `json:"prompt_version"`
	PromptSHA256     string `json:"prompt_sha256"`
	ImageDigest      string `json:"image_digest"`
	MemoryPeakBytes  *int64 `json:"memory_peak_bytes"`
}

type Options struct {
	Tasks        int
	TaskIndex    int
	ManifestPath string
	ShardPath    string
}

type CauseMetrics struct {
	TruePositive  int     `json:"true_positive"`
	FalsePositive int     `json:"false_positive"`
	FalseNegative int     `json:"false_negative"`
	Precision     float64 `json:"precision"`
	Recall        float64 `json:"recall"`
	F1            float64 `json:"f1"`
}

type Reference struct {
	Outcome string
	Causes  map[string]bool
}

type Report struct {
	RunID               string                  `json:"run_id"`
	GPUProfile          string                  `json:"gpu_profile"`
	ManifestSHA256      string                  `json:"manifest_sha256"`
	ModelRevision       string                  `json:"model_revision"`
	Quantization        string                  `json:"quantization"`
	VLLMVersion         string                  `json:"vllm_version"`
	Tasks               int                     `json:"tasks"`
	Records             int                     `json:"records"`
	SchemaValidRate     float64                 `json:"schema_valid_rate"`
	OutcomeAccuracy     float64                 `json:"outcome_accuracy"`
	OutcomeMacroF1      float64                 `json:"outcome_macro_f1"`
	CausePrecision      float64                 `json:"cause_precision"`
	CauseRecall         float64                 `json:"cause_recall"`
	CauseF1             float64                 `json:"cause_f1"`
	TaskDurationMS      int64                   `json:"task_duration_ms_sum"`
	TaskWallMS          int64                   `json:"task_wall_ms_max"`
	ModelLoadMS         int64                   `json:"model_load_ms_sum"`
	InputTokens         int64                   `json:"input_tokens_sum"`
	OutputTokens        int64                   `json:"output_tokens_sum"`
	CorrectlyInferred   int                     `json:"correct_outcomes"`
	Scope               string                  `json:"scope"`
	SourceRecords       int                     `json:"source_records"`
	DomainVersion       string                  `json:"domain_version,omitempty"`
	DomainSHA256        string                  `json:"domain_sha256,omitempty"`
	PromptVersion       string                  `json:"prompt_version"`
	PromptSHA256        string                  `json:"prompt_sha256"`
	ImageDigest         string                  `json:"image_digest"`
	MemoryPeakBytes     *int64                  `json:"memory_peak_bytes"`
	InferenceDurationMS int64                   `json:"inference_duration_ms_sum"`
	InferenceP50MS      int64                   `json:"inference_p50_ms"`
	InferenceP95MS      int64                   `json:"inference_p95_ms"`
	InferenceMaxMS      int64                   `json:"inference_max_ms"`
	PerCause            map[string]CauseMetrics `json:"per_cause"`
}

func ratio(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d)
}

func ReadTruth(path string) (map[string]Reference, error) {
	rows, err := parquet.ReadFile[TruthRow](path)
	if err != nil {
		return nil, err
	}
	truth := make(map[string]Reference, len(rows))
	for _, row := range rows {
		var meta struct {
			HiddenObjectionIDs []string  `json:"hidden_objection_ids"`
			CauseCodes         *[]string `json:"cause_codes"`
		}
		if err := json.Unmarshal([]byte(row.MetadataJSON), &meta); err != nil {
			return nil, fmt.Errorf("truth %s: %w", row.RecordID, err)
		}
		if row.RecordID == "" || truth[row.RecordID].Outcome != "" {
			return nil, fmt.Errorf("empty or duplicate truth ID %q", row.RecordID)
		}
		codes := meta.HiddenObjectionIDs
		if meta.CauseCodes != nil {
			if meta.HiddenObjectionIDs != nil {
				return nil, errors.New("ambiguous reference cause fields")
			}
			codes = *meta.CauseCodes
		}
		causes := make(map[string]bool, len(codes))
		for _, code := range codes {
			if code == "" || causes[code] {
				return nil, errors.New("empty or duplicate reference cause")
			}
			causes[code] = true
		}
		outcome := strings.ToLower(row.Outcome)
		if outcome != "success" && outcome != "failure" {
			return nil, fmt.Errorf("unexpected truth outcome for %s", row.RecordID)
		}
		truth[row.RecordID] = Reference{Outcome: outcome, Causes: causes}
	}
	if len(truth) == 0 {
		return nil, errors.New("ground truth is empty")
	}
	return truth, nil
}

func Evaluate(dir, profile string, truth map[string]Reference, options Options) (Report, error) {
	if options.Tasks == 0 {
		options.Tasks = 3
	}
	if options.Tasks != 1 && options.Tasks != 3 {
		return Report{}, errors.New("use one canary task or three full-pilot tasks")
	}
	sourceRecords := len(truth)
	expectedManifestHash := ""
	expectedRunID, expectedPrompt, expectedDomainHash, expectedDomainVersion := "", "", "", ""
	if options.ManifestPath != "" {
		manifestBytes, err := os.ReadFile(options.ManifestPath)
		if err != nil {
			return Report{}, err
		}
		var manifest struct {
			DomainSHA256  string `json:"domain_sha256"`
			DomainVersion string `json:"domain_version"`
			RunID         string `json:"run_id"`
			PromptVersion string `json:"prompt_version"`
		}
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			return Report{}, err
		}
		expectedRunID, expectedPrompt = manifest.RunID, manifest.PromptVersion
		expectedDomainHash = manifest.DomainSHA256
		expectedDomainVersion = manifest.DomainVersion
		if expectedPrompt == "worker-a-v3" && (expectedDomainVersion == "" || len(expectedDomainHash) != 64) {
			return Report{}, errors.New("manifest domain identity is missing")
		}
		if expectedRunID == "" || expectedPrompt == "" {
			return Report{}, errors.New("manifest identity is missing")
		}
		digest := sha256.Sum256(manifestBytes)
		expectedManifestHash = hex.EncodeToString(digest[:])
	}
	if options.Tasks == 1 {
		var err error
		truth, err = selectCanaryTruth(truth, options)
		if err != nil {
			return Report{}, err
		}
	}
	var chunks []string
	var markers []Marker
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		switch {
		case strings.HasPrefix(entry.Name(), "chunk-") && strings.HasSuffix(entry.Name(), ".parquet"):
			chunks = append(chunks, path)
		case entry.Name() == "_SUCCESS.json":
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var item Marker
			if err := json.Unmarshal(data, &item); err != nil {
				return err
			}
			markers = append(markers, item)
		case entry.Name() == "_ABORTED.json":
			return fmt.Errorf("aborted task marker: %s", path)
		}
		return nil
	})
	if err != nil {
		return Report{}, err
	}
	if len(markers) != options.Tasks || len(chunks) == 0 {
		return Report{}, fmt.Errorf("%s needs %d success markers and at least one chunk", profile, options.Tasks)
	}
	sort.Strings(chunks)
	report := Report{GPUProfile: profile, Tasks: len(markers), Records: len(truth), SourceRecords: sourceRecords, Scope: "full-pilot", PerCause: map[string]CauseMetrics{}}
	if options.Tasks == 1 {
		report.Scope = "single-shard-canary"
	}
	seenTasks := map[int]bool{}
	markedRecords := 0
	for _, item := range markers {
		if item.GPUProfile != profile || item.Status != "success" || item.TaskIndex < 0 || item.TaskIndex >= 3 || seenTasks[item.TaskIndex] || (options.Tasks == 1 && item.TaskIndex != options.TaskIndex) {
			return Report{}, fmt.Errorf("invalid %s task marker", profile)
		}
		if item.ModelRevision == "" || item.Quantization == "" || item.VLLMVersion == "" || item.PromptVersion == "" || item.PromptSHA256 == "" || item.ImageDigest == "" {
			return Report{}, errors.New("task marker is missing model, engine, prompt, or image provenance")
		}
		seenTasks[item.TaskIndex] = true
		if report.RunID == "" {
			report.RunID, report.ManifestSHA256 = item.RunID, item.ManifestSHA256
			report.ModelRevision, report.Quantization, report.VLLMVersion = item.ModelRevision, item.Quantization, item.VLLMVersion
			report.PromptVersion, report.PromptSHA256, report.ImageDigest = item.PromptVersion, item.PromptSHA256, item.ImageDigest
			report.DomainVersion, report.DomainSHA256 = item.DomainVersion, item.DomainSHA256
		}
		if item.DomainSHA256 != report.DomainSHA256 || item.DomainVersion != report.DomainVersion || (expectedDomainHash != "" && (item.DomainSHA256 != expectedDomainHash || item.DomainVersion != expectedDomainVersion)) || (item.PromptVersion == "worker-a-v3" && (item.DomainVersion == "" || len(item.DomainSHA256) != 64)) || item.RunID != report.RunID || item.ManifestSHA256 != report.ManifestSHA256 || item.ModelRevision != report.ModelRevision || item.Quantization != report.Quantization || item.VLLMVersion != report.VLLMVersion || item.RecordsProcessed != item.RecordsTotal || item.PromptVersion != report.PromptVersion || item.PromptSHA256 != report.PromptSHA256 || item.ImageDigest != report.ImageDigest || (expectedManifestHash != "" && (item.ManifestSHA256 != expectedManifestHash || item.RunID != expectedRunID || item.PromptVersion != expectedPrompt)) {
			return Report{}, fmt.Errorf("inconsistent %s task markers", profile)
		}
		markedRecords += item.RecordsProcessed
		if item.MemoryPeakBytes != nil && (report.MemoryPeakBytes == nil || *item.MemoryPeakBytes > *report.MemoryPeakBytes) {
			peak := *item.MemoryPeakBytes
			report.MemoryPeakBytes = &peak
		}
		report.TaskDurationMS += item.DurationMS
		if item.DurationMS > report.TaskWallMS {
			report.TaskWallMS = item.DurationMS
		}
		report.ModelLoadMS += item.ModelLoadMS
	}
	if markedRecords != len(truth) {
		return Report{}, errors.New("marker record counts do not match expected coverage")
	}
	if report.RunID == "" || report.ManifestSHA256 == "" {
		return Report{}, fmt.Errorf("missing %s run identity", profile)
	}
	seen := map[string]bool{}
	valid, correct, truePositive, falsePositive, falseNegative := 0, 0, 0, 0, 0
	outcomeTP := map[string]int{"success": 0, "failure": 0}
	outcomeFP := map[string]int{"success": 0, "failure": 0}
	outcomeFN := map[string]int{"success": 0, "failure": 0}
	var latencies []int64
	for _, path := range chunks {
		rows, err := parquet.ReadFile[PredictionRow](path)
		if err != nil {
			return Report{}, fmt.Errorf("%s: %w", path, err)
		}
		for _, row := range rows {
			ref, ok := truth[row.ConversationID]
			if !ok || seen[row.ConversationID] {
				return Report{}, fmt.Errorf("unknown or duplicate prediction %q", row.ConversationID)
			}
			seen[row.ConversationID] = true
			report.InputTokens += int64(row.InputTokens)
			report.OutputTokens += int64(row.OutputTokens)
			if row.Status == "ok" {
				latencies = append(latencies, row.DurationMS)
				report.InferenceDurationMS += row.DurationMS
			}
			if row.Status != "ok" {
				falseNegative += len(ref.Causes)
				for code := range ref.Causes {
					metric := report.PerCause[code]
					metric.FalseNegative++
					report.PerCause[code] = metric
				}
				outcomeFN[ref.Outcome]++
				continue
			}
			valid++
			if row.Outcome == ref.Outcome {
				correct++
				outcomeTP[ref.Outcome]++
			} else {
				outcomeFP[row.Outcome]++
				outcomeFN[ref.Outcome]++
			}
			predicted := map[string]bool{}
			for _, item := range row.Causes {
				predicted[item.Code] = true
			}
			for code := range predicted {
				metric := report.PerCause[code]
				if ref.Causes[code] {
					truePositive++
					metric.TruePositive++
				} else {
					falsePositive++
					metric.FalsePositive++
				}
				report.PerCause[code] = metric
			}
			for code := range ref.Causes {
				if !predicted[code] {
					falseNegative++
					metric := report.PerCause[code]
					metric.FalseNegative++
					report.PerCause[code] = metric
				}
			}
		}
	}
	if len(seen) != len(truth) {
		return Report{}, fmt.Errorf("%s has %d predictions for %d truth records", profile, len(seen), len(truth))
	}
	report.CorrectlyInferred = correct
	report.SchemaValidRate = ratio(valid, len(truth))
	report.OutcomeAccuracy = ratio(correct, len(truth))
	for _, class := range []string{"success", "failure"} {
		report.OutcomeMacroF1 += ratio(2*outcomeTP[class], 2*outcomeTP[class]+outcomeFP[class]+outcomeFN[class]) / 2
	}
	report.CausePrecision = ratio(truePositive, truePositive+falsePositive)
	report.CauseRecall = ratio(truePositive, truePositive+falseNegative)
	if report.CausePrecision+report.CauseRecall > 0 {
		report.CauseF1 = 2 * report.CausePrecision * report.CauseRecall / (report.CausePrecision + report.CauseRecall)
	}
	for code, metric := range report.PerCause {
		metric.Precision = ratio(metric.TruePositive, metric.TruePositive+metric.FalsePositive)
		metric.Recall = ratio(metric.TruePositive, metric.TruePositive+metric.FalseNegative)
		metric.F1 = ratio(2*metric.TruePositive, 2*metric.TruePositive+metric.FalsePositive+metric.FalseNegative)
		report.PerCause[code] = metric
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		report.InferenceP50MS = latencies[(len(latencies)*50+99)/100-1]
		report.InferenceP95MS = latencies[(len(latencies)*95+99)/100-1]
		report.InferenceMaxMS = latencies[len(latencies)-1]
	}
	return report, nil
}

func selectCanaryTruth(truth map[string]Reference, options Options) (map[string]Reference, error) {
	if options.ManifestPath == "" || options.ShardPath == "" {
		return nil, errors.New("canary evaluation requires its original manifest and exact shard file")
	}
	data, err := os.ReadFile(options.ManifestPath)
	if err != nil {
		return nil, err
	}
	var manifest struct {
		Shards []struct {
			Index   int    `json:"index"`
			Records int    `json:"records"`
			SHA256  string `json:"sha256"`
		} `json:"shards"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if options.TaskIndex < 0 || options.TaskIndex >= len(manifest.Shards) || manifest.Shards[options.TaskIndex].Index != options.TaskIndex {
		return nil, errors.New("canary task index does not match manifest")
	}
	shard := manifest.Shards[options.TaskIndex]
	compressed, err := os.ReadFile(options.ShardPath)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(compressed)
	if hex.EncodeToString(digest[:]) != shard.SHA256 {
		return nil, errors.New("canary shard checksum mismatch")
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	scanner := bufio.NewScanner(decoder)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	selected := map[string]Reference{}
	for scanner.Scan() {
		var row struct {
			RecordID string `json:"record_id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, err
		}
		ref, ok := truth[row.RecordID]
		if !ok || selected[row.RecordID].Outcome != "" {
			return nil, errors.New("unknown or duplicate record in canary shard")
		}
		selected[row.RecordID] = ref
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(selected) != shard.Records {
		return nil, errors.New("canary shard count differs from manifest")
	}
	return selected, nil
}
