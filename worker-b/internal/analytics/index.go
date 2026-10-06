package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
)

type Receipt struct {
	SchemaVersion string `json:"schema_version"`
	AnalysisID    string `json:"analysis_id"`
	ReportSHA256  string `json:"report_sha256"`
	JobID         string `json:"job_id"`
	Status        string `json:"status"`
}

func indexJobID(id, hash, attempt string) string {
	return "index_v1_" + analysis.Digest([]byte(id+":"+hash+":"+attempt))
}

func receiptPath(prefix, id, hash string) string {
	return prefix + "/" + id + "/index-v1/" + hash + "/_SUCCESS.json"
}

// Index is manually serialized during this milestone. Attempts use deterministic
// job IDs; operators must resolve a previous attempt before selecting a new one.
func Index(ctx context.Context, s analysis.Store, w Warehouse, prefix, indexPrefix, id, attempt string) (jobID string, retErr error) {
	ctx = telemetry.Bind(ctx, id, "")
	span := telemetry.Begin(ctx, "index", "run")
	defer func() { span.End(retErr != nil) }()
	if !IDValid(attempt) {
		return "", ErrInvalid
	}
	r, hash, err := Load(ctx, s, prefix, id)
	if err != nil {
		return "", err
	}
	base := indexPrefix + "/" + id + "/index-v1/" + hash
	pending := Receipt{"index-v1", id, hash, indexJobID(id, hash, attempt), "pending"}
	err = s.Create(ctx, base+"/attempts/"+attempt+"/_PENDING.json", analysis.JSON(pending))
	if errors.Is(err, analysis.ErrExists) {
		raw, e := s.Read(ctx, base+"/attempts/"+attempt+"/_PENDING.json", 1<<20)
		var old Receipt
		if e != nil || analysis.Decode(raw, &old) != nil || old != pending {
			return "", errors.New("index attempt conflict")
		}
	} else if err != nil {
		return "", errors.New("index pending write failed")
	}
	job, err := w.Index(ctx, attempt, r, hash)
	if err != nil {
		return "", errors.New("index remains pending; reconcile the same attempt")
	}
	receipt := Receipt{"index-v1", id, hash, job, "indexed"}
	name := receiptPath(indexPrefix, id, hash)
	err = s.Create(ctx, name, analysis.JSON(receipt))
	if errors.Is(err, analysis.ErrExists) {
		raw, e := s.Read(ctx, name, 1<<20)
		var old Receipt
		if e != nil || json.Unmarshal(raw, &old) != nil || old.SchemaVersion != "index-v1" || old.AnalysisID != id || old.ReportSHA256 != hash || old.Status != "indexed" {
			return "", errors.New("index receipt conflict")
		}
	} else if err != nil {
		return "", errors.New("index committed; receipt recovery required")
	}
	return job, nil
}
