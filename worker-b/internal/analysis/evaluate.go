package analysis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

func Digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// Fingerprint identifies immutable evaluator inputs independently of analysis ID.
func Fingerprint(r Request) string {
	if len(r.AdditionalTasks) > 0 {
		primary := r
		primary.AdditionalTasks = nil
		return Digest(JSON(struct {
			Primary string
			Tasks   []TaskSource
		}{Fingerprint(primary), r.AdditionalTasks}))
	}
	// Analysis ID, rendering options and metric replay do not change source identity.
	data, _ := json.Marshal(struct {
		Manifest, Shard, Marker ObjectRef
		Chunks                  []ObjectRef
		Truth                   *ObjectRef
		SourceImage             string
	}{r.Manifest, r.Shard, r.Marker, r.Chunks, r.Truth, r.SourceImage})
	return Digest(data)
}
func readVerified(ctx context.Context, s Store, ref ObjectRef) ([]byte, error) {
	data, err := s.Read(ctx, ref.URI, MaxObjectBytes)
	if err != nil {
		return nil, err
	}
	if Digest(data) != ref.SHA256 {
		return nil, errors.New("source checksum mismatch")
	}
	return data, nil
}

type dialogue struct {
	RecordID string `json:"record_id"`
	Messages []struct {
		TurnID int32  `json:"turn_id"`
		Role   string `json:"role"`
		Text   string `json:"text"`
	} `json:"messages"`
}

type manifest struct {
	RunID         string `json:"run_id"`
	RecordsTotal  int    `json:"records_total"`
	PromptVersion string `json:"prompt_version"`
	Shards        []struct {
		Index   int    `json:"index"`
		Records int    `json:"records"`
		SHA256  string `json:"sha256"`
	} `json:"shards"`
}

func Evaluate(ctx context.Context, s Store, r Request) (Metrics, error) {
	if len(r.AdditionalTasks) > 0 {
		return evaluateBatch(ctx, s, r)
	}
	return evaluateSingle(ctx, s, r)
}
func evaluateSingle(ctx context.Context, s Store, r Request) (Metrics, error) {
	if r.Metrics != nil {
		data, err := readVerified(ctx, s, *r.Metrics)
		if err != nil {
			return Metrics{}, err
		}
		var m Metrics
		if Decode(data, &m) != nil || ValidateMetrics(m, r) != nil {
			return Metrics{}, errors.New("invalid metrics replay provenance")
		}
		return m, nil
	}
	refs := []ObjectRef{r.Manifest, r.Shard, r.Marker}
	refs = append(refs, r.Chunks...)
	if r.Truth != nil {
		refs = append(refs, *r.Truth)
	}
	dir, err := os.MkdirTemp("", "analysis-input-")
	if err != nil {
		return Metrics{}, err
	}
	defer os.RemoveAll(dir)
	payloads := make([][]byte, 0, len(refs))
	total := 0
	for _, ref := range refs {
		data, err := readVerified(ctx, s, ref)
		if err != nil {
			return Metrics{}, err
		}
		total += len(data)
		if total > MaxTotalBytes {
			return Metrics{}, errors.New("source package exceeds size bound")
		}
		if len(data) >= 4 && string(data[:4]) == "PAR1" {
			file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil || file.NumRows() < 1 || file.NumRows() > 900 {
				return Metrics{}, errors.New("Parquet row bound exceeded")
			}
		}
		payloads = append(payloads, data)
	}
	var mf manifest
	if json.Unmarshal(payloads[0], &mf) != nil || mf.RecordsTotal < 1 || mf.RecordsTotal > 900 || mf.RunID != r.SourceRunID || mf.PromptVersion != "worker-a-v2" || r.TaskIndex < 0 || r.TaskIndex >= len(mf.Shards) || mf.Shards[r.TaskIndex].Index != r.TaskIndex || mf.Shards[r.TaskIndex].Records <= 0 || mf.Shards[r.TaskIndex].Records > MaxRecords || mf.Shards[r.TaskIndex].SHA256 != r.Shard.SHA256 {
		return Metrics{}, errors.New("source manifest mismatch or unsupported scope")
	}
	var marker benchmark.Marker
	if json.Unmarshal(payloads[2], &marker) != nil || marker.ImageDigest != r.SourceImage {
		return Metrics{}, errors.New("source image provenance mismatch")
	}
	dec, err := zstd.NewReader(bytes.NewReader(payloads[1]), zstd.WithDecoderMaxMemory(MaxTotalBytes))
	if err != nil {
		return Metrics{}, err
	}
	defer dec.Close()
	scanner := bufio.NewScanner(io.LimitReader(dec, MaxTotalBytes+1))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	expected := map[string]benchmark.Reference{}
	dialogues := map[string]dialogue{}
	decompressed := 0
	for scanner.Scan() {
		decompressed += len(scanner.Bytes()) + 1
		if decompressed > MaxTotalBytes {
			return Metrics{}, errors.New("decompressed shard exceeds bound")
		}
		var d dialogue
		if json.Unmarshal(scanner.Bytes(), &d) != nil || d.RecordID == "" || len(d.Messages) == 0 {
			return Metrics{}, errors.New("invalid canonical dialogue")
		}
		if _, ok := expected[d.RecordID]; ok {
			return Metrics{}, errors.New("duplicate source dialogue")
		}
		if len(expected) >= MaxRecords {
			return Metrics{}, errors.New("record bound exceeded")
		}
		expected[d.RecordID] = benchmark.Reference{Outcome: "failure", Causes: map[string]bool{}}
		dialogues[d.RecordID] = d
	}
	if err = scanner.Err(); err != nil {
		return Metrics{}, err
	}
	if len(expected) != mf.Shards[r.TaskIndex].Records {
		return Metrics{}, errors.New("shard record coverage mismatch")
	}
	truth := expected
	if r.Truth != nil {
		path := filepath.Join(dir, "labels.parquet")
		if err = os.WriteFile(path, payloads[len(payloads)-1], 0600); err != nil {
			return Metrics{}, err
		}
		truth, err = benchmark.ReadTruth(path)
		if err != nil {
			return Metrics{}, err
		}
		if len(truth) != mf.RecordsTotal {
			return Metrics{}, errors.New("label source coverage mismatch")
		}
	}
	paths := []string{"manifest.json", "shard.jsonl.zst", "_SUCCESS.json"}
	for i, data := range payloads[:3] {
		if err = os.WriteFile(filepath.Join(dir, paths[i]), data, 0600); err != nil {
			return Metrics{}, err
		}
	}
	seen := map[string]bool{}
	valid := 0
	evidence := []Evidence{}
	for i, data := range payloads[3 : 3+len(r.Chunks)] {
		path := filepath.Join(dir, fmt.Sprintf("chunk-%06d.parquet", i))
		if err = os.WriteFile(path, data, 0600); err != nil {
			return Metrics{}, err
		}
		rows, err := parquet.ReadFile[benchmark.PredictionRow](path)
		if err != nil {
			return Metrics{}, err
		}
		for _, row := range rows {
			if err = ctx.Err(); err != nil {
				return Metrics{}, err
			}
			d, ok := dialogues[row.ConversationID]
			if !ok || seen[row.ConversationID] {
				return Metrics{}, errors.New("unknown or duplicate prediction")
			}
			seen[row.ConversationID] = true
			if row.Status != "ok" {
				continue
			}
			valid++
			if row.Outcome != "success" && row.Outcome != "failure" {
				return Metrics{}, errors.New("invalid accepted prediction outcome")
			}
			turns := map[int32]bool{}
			for _, msg := range d.Messages {
				if msg.Role == "customer" {
					turns[msg.TurnID] = true
				}
			}
			codes := map[string]bool{}
			for _, cause := range row.Causes {
				if cause.Code == "" || codes[cause.Code] || math.IsNaN(float64(cause.Confidence)) || cause.Confidence < 0 || cause.Confidence > 1 || len(cause.EvidenceTurns) == 0 {
					return Metrics{}, errors.New("invalid accepted cause")
				}
				codes[cause.Code] = true
				ids := map[int32]bool{}
				for _, id := range cause.EvidenceTurns {
					if !turns[id] || ids[id] {
						return Metrics{}, errors.New("invalid accepted evidence reference")
					}
					ids[id] = true
				}
			}
			if len(evidence) < 10 && len(row.Causes) > 0 {
				cause := row.Causes[0]
				for _, msg := range d.Messages {
					if msg.TurnID == cause.EvidenceTurns[0] {
						if len(msg.Text) < 1 || len(msg.Text) > 1024 {
							return Metrics{}, errors.New("selected evidence exceeds text bound")
						}
						evidence = append(evidence, Evidence{RecordID: row.ConversationID, CauseCode: cause.Code, TurnID: msg.TurnID, Text: msg.Text})
						break
					}
				}
			}
		}
	}
	report, err := benchmark.Evaluate(dir, "rtx6000", truth, benchmark.Options{Tasks: 1, TaskIndex: r.TaskIndex, ManifestPath: filepath.Join(dir, "manifest.json"), ShardPath: filepath.Join(dir, "shard.jsonl.zst")})
	if err != nil {
		return Metrics{}, err
	}
	m := Metrics{Evidence: evidence, SchemaVersion: SchemaVersion, EvaluatorVersion: EvaluatorVersion, SourceRunID: r.SourceRunID, ManifestSHA256: r.Manifest.SHA256, SourceImage: r.SourceImage, InputFingerprint: Fingerprint(r), RecordsExpected: report.Records, SourceRecords: mf.RecordsTotal, RecordsValid: valid, RecordsInvalid: report.Records - valid, SchemaValidRate: report.SchemaValidRate, PerCause: map[string]benchmark.CauseMetrics{}}
	if r.Truth != nil {
		m.CorrectOutcomes = &report.CorrectlyInferred
		m.OutcomeAccuracy = &report.OutcomeAccuracy
		m.CausePrecision = &report.CausePrecision
		m.CauseRecall = &report.CauseRecall
		m.CauseF1 = &report.CauseF1
		m.PerCause = report.PerCause
	}
	return m, ValidateMetrics(m, r)
}

// ValidateMetrics verifies coverage, provenance and nullable supervised scores.
func ValidateMetrics(m Metrics, r Request) error {
	if r.SchemaVersion == BatchSchemaVersion && (m.RecordsExpected != 900 || m.SourceRecords != 900) {
		return errors.New("batch metrics must cover all 900 records")
	}

	if m.Evidence == nil || len(m.Evidence) > 10 {
		return errors.New("invalid evidence sample")
	}
	seenEvidence := map[string]bool{}
	for _, e := range m.Evidence {
		if e.RecordID == "" || seenEvidence[e.RecordID] || e.CauseCode == "" || e.TurnID < 1 || len(e.Text) == 0 || len(e.Text) > 1024 {
			return errors.New("invalid evidence sample identity or bound")
		}
		seenEvidence[e.RecordID] = true
	}

	if m.SchemaVersion != r.SchemaVersion || m.EvaluatorVersion != EvaluatorVersion || m.SourceRunID != r.SourceRunID || m.ManifestSHA256 != r.Manifest.SHA256 || m.SourceImage != r.SourceImage || m.InputFingerprint != Fingerprint(r) || m.RecordsExpected < 1 || m.RecordsExpected > r.RecordLimit() || m.SourceRecords < m.RecordsExpected || m.RecordsValid < 0 || m.RecordsInvalid < 0 || m.RecordsValid+m.RecordsInvalid != m.RecordsExpected || m.SchemaValidRate != float64(m.RecordsValid)/float64(m.RecordsExpected) {
		return errors.New("metrics identity or coverage mismatch")
	}
	values := []*float64{m.OutcomeAccuracy, m.CausePrecision, m.CauseRecall, m.CauseF1}
	for _, v := range values {
		if (r.Truth != nil) != (v != nil) || (v != nil && (math.IsNaN(*v) || *v < 0 || *v > 1)) {
			return errors.New("metrics supervision mismatch")
		}
	}
	if r.Truth == nil && (m.CorrectOutcomes != nil || len(m.PerCause) > 0) {
		return errors.New("unavailable supervised metrics must be null")
	}
	if r.Truth != nil && (m.CorrectOutcomes == nil || *m.CorrectOutcomes < 0 || *m.CorrectOutcomes > m.RecordsValid || *m.OutcomeAccuracy != float64(*m.CorrectOutcomes)/float64(m.RecordsExpected)) {
		return errors.New("outcome metrics mismatch")
	}
	return nil
}
