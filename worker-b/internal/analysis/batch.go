package analysis

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/klauspost/compress/zstd"
)

type batchStore struct {
	Store
	seen  map[string]bool
	bytes int
}

func (s *batchStore) Read(ctx context.Context, uri string, limit int64) ([]byte, error) {
	raw, err := s.Store.Read(ctx, uri, limit)
	if err == nil && !s.seen[uri] {
		s.seen[uri] = true
		s.bytes += len(raw)
		if s.bytes > MaxTotalBytes {
			return nil, errors.New("batch package exceeds size bound")
		}
	}
	return raw, err
}
func evaluateBatch(ctx context.Context, store Store, r Request) (Metrics, error) {
	if r.TaskCount() != 3 || r.SchemaVersion != BatchSchemaVersion || r.Truth == nil {
		return Metrics{}, errors.New("three supervised tasks required")
	}
	s := &batchStore{Store: store, seen: map[string]bool{}}
	out := Metrics{SchemaVersion: BatchSchemaVersion, EvaluatorVersion: EvaluatorVersion, SourceRunID: r.SourceRunID, ManifestSHA256: r.Manifest.SHA256, SourceImage: r.SourceImage, InputFingerprint: Fingerprint(r), PerCause: map[string]benchmark.CauseMetrics{}, Evidence: []Evidence{}}
	raw, err := readVerified(ctx, s, r.Manifest)
	if err != nil {
		return Metrics{}, err
	}
	var mf manifest
	if json.Unmarshal(raw, &mf) != nil || mf.RecordsTotal != 900 || len(mf.Shards) != 3 {
		return Metrics{}, errors.New("full manifest must contain three shards and 900 records")
	}
	var identity any
	ids := map[string]bool{}
	correct := 0
	for _, task := range r.Sources() {
		raw, err := readVerified(ctx, s, task.Marker)
		if err != nil {
			return Metrics{}, err
		}
		var marker benchmark.Marker
		if json.Unmarshal(raw, &marker) != nil {
			return Metrics{}, errors.New("invalid batch marker")
		}
		current := struct{ Run, Image, Manifest, Model, Quantization, Engine, Prompt, PromptHash string }{marker.RunID, marker.ImageDigest, marker.ManifestSHA256, marker.ModelRevision, marker.Quantization, marker.VLLMVersion, marker.PromptVersion, marker.PromptSHA256}
		if identity == nil {
			identity = current
		} else if !reflect.DeepEqual(identity, current) {
			return Metrics{}, errors.New("batch marker provenance differs")
		}

		shard, err := readVerified(ctx, s, task.Shard)
		if err != nil {
			return Metrics{}, err
		}
		dec, err := zstd.NewReader(bytes.NewReader(shard), zstd.WithDecoderMaxMemory(MaxTotalBytes))
		if err != nil {
			return Metrics{}, err
		}
		scan := bufio.NewScanner(io.LimitReader(dec, MaxTotalBytes+1))
		scan.Buffer(make([]byte, 4096), 1<<20)
		decompressed := 0
		for scan.Scan() {
			decompressed += len(scan.Bytes()) + 1
			var d dialogue
			if decompressed > MaxTotalBytes || json.Unmarshal(scan.Bytes(), &d) != nil || d.RecordID == "" || ids[d.RecordID] || len(ids) >= 900 {
				dec.Close()
				return Metrics{}, errors.New("batch shard identities or size invalid")
			}
			ids[d.RecordID] = true
		}
		err = scan.Err()
		dec.Close()
		if err != nil {
			return Metrics{}, err
		}
		single := r
		single.SchemaVersion = SchemaVersion
		single.AdditionalTasks = nil
		single.TaskIndex = task.TaskIndex
		single.Shard = task.Shard
		single.Marker = task.Marker
		single.Chunks = task.Chunks
		m, err := evaluateSingle(ctx, s, single)
		if err != nil {
			return Metrics{}, err
		}
		if m.RecordsExpected != 300 || m.SourceRecords != 900 {
			return Metrics{}, errors.New("batch requires complete three 300-record shards")
		}
		out.RecordsExpected += m.RecordsExpected
		out.RecordsValid += m.RecordsValid
		out.RecordsInvalid += m.RecordsInvalid
		correct += *m.CorrectOutcomes
		for _, e := range m.Evidence {
			if len(out.Evidence) < 10 {
				out.Evidence = append(out.Evidence, e)
			}
		}
		for code, v := range m.PerCause {
			a := out.PerCause[code]
			a.TruePositive += v.TruePositive
			a.FalsePositive += v.FalsePositive
			a.FalseNegative += v.FalseNegative
			out.PerCause[code] = a
		}
	}
	if len(ids) != 900 {
		return Metrics{}, errors.New("batch population coverage incomplete")
	}
	out.SourceRecords = 900
	out.CorrectOutcomes = &correct
	out.SchemaValidRate = float64(out.RecordsValid) / 900
	accuracy := float64(correct) / 900
	out.OutcomeAccuracy = &accuracy
	tp, fp, fn := 0, 0, 0
	ratio := func(n, d int) float64 {
		if d == 0 {
			return 0
		}
		return float64(n) / float64(d)
	}
	for code, v := range out.PerCause {
		tp += v.TruePositive
		fp += v.FalsePositive
		fn += v.FalseNegative
		v.Precision = ratio(v.TruePositive, v.TruePositive+v.FalsePositive)
		v.Recall = ratio(v.TruePositive, v.TruePositive+v.FalseNegative)
		v.F1 = ratio(2*v.TruePositive, 2*v.TruePositive+v.FalsePositive+v.FalseNegative)
		out.PerCause[code] = v
	}
	precision, recall, f1 := ratio(tp, tp+fp), ratio(tp, tp+fn), ratio(2*tp, 2*tp+fp+fn)
	out.CausePrecision = &precision
	out.CauseRecall = &recall
	out.CauseF1 = &f1
	return out, ValidateMetrics(out, r)
}
