package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	"github.com/parquet-go/parquet-go"
)

// Preparation pins raw inputs before future output hashes exist. Zero hashes
// are prospective references only; verified preparation replaces them before GPU launch.
type Preparation struct {
	Image    string             `json:"image"`
	GitSHA   string             `json:"git_sha"`
	Source   analysis.ObjectRef `json:"source"`
	Metadata analysis.ObjectRef `json:"metadata"`
}

const prospectiveHash = "0000000000000000000000000000000000000000000000000000000000000000"

type PreparationBackend interface {
	ResolvePreparation(context.Context, Plan) (*analysis.Request, bool, error)
	VerifyFreshWorker(context.Context, Plan) error
}

func (p Plan) validatePreparation() error {
	q := p.Preparation
	if p.WorkerA == nil || p.Request == nil || p.Request.Truth == nil || p.Request.TaskCount() != 3 || p.Request.SchemaVersion != analysis.BatchSchemaVersion || p.Request.Metrics != nil || p.Request.SourceRunID != p.WorkerA.CanaryID || !imageValid(q.Image, "dataset-preparer") || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(q.GitSHA) {
		return errors.New("fresh preparation requires one full 900-record scope")
	}
	for _, ref := range []analysis.ObjectRef{q.Source, q.Metadata} {
		if !regexp.MustCompile(`^gs://your-gcp-project-id-raw/[a-zA-Z0-9/_-]+\.[a-zA-Z0-9]+$`).MatchString(ref.URI) || !digestPattern.MatchString(ref.SHA256) || ref.SHA256 == prospectiveHash {
			return errors.New("raw inputs must have pinned hashes")
		}
	}
	if q.Source.URI == q.Metadata.URI {
		return errors.New("source and metadata must be distinct")
	}
	run := p.Request.SourceRunID
	if p.Request.Manifest.URI != "gs://your-gcp-project-id-prepared/runs/"+run+"/manifest.json" || p.Request.Manifest.SHA256 != prospectiveHash || p.Request.Truth.URI != "gs://your-gcp-project-id-ground-truth/runs/"+run+"/labels.parquet" || p.Request.Truth.SHA256 != prospectiveHash {
		return errors.New("prepared outputs must be prospective and isolated")
	}
	for _, task := range p.Request.Sources() {
		if task.Shard.URI != fmt.Sprintf("gs://your-gcp-project-id-prepared/runs/%s/shards/shard-%05d.jsonl.zst", run, task.TaskIndex) || task.Shard.SHA256 != prospectiveHash || task.Marker.URI != p.WorkerTask(task.TaskIndex)+"/_SUCCESS.json" || task.Marker.SHA256 != prospectiveHash || len(task.Chunks) != 1 || task.Chunks[0].URI != p.WorkerTask(task.TaskIndex)+"/chunk-000000.parquet" || task.Chunks[0].SHA256 != prospectiveHash {
			return errors.New("fresh outputs cannot reference retained artifacts")
		}
	}
	return nil
}
func (c *Cloud) verifyRaw(ctx context.Context, p Plan) error {
	for _, ref := range []analysis.ObjectRef{p.Preparation.Source, p.Preparation.Metadata} {
		raw, err := c.Store.Read(ctx, ref.URI, 256<<20)
		if err != nil || analysis.Digest(raw) != ref.SHA256 {
			return errors.New("raw source checksum mismatch")
		}
	}
	return nil
}
func (c *Cloud) VerifyFreshWorker(ctx context.Context, p Plan) error {
	// Reject an occupied analysis identity before paying for a new GPU launch.
	refs := []string{p.RequestURI()}
	for _, name := range []string{"request.json", "metrics.json", "report.json", "_SUCCESS.json", "_FAILED.json"} {
		refs = append(refs, p.Request.OutputPrefix+"/"+p.AnalysisID+"/"+name)
	}
	for _, uri := range refs {
		_, err := c.Store.Read(ctx, uri, analysis.MaxObjectBytes)
		if !errors.Is(err, analysis.ErrMissing) {
			return errors.New("fresh analysis output namespace is not empty")
		}
	}
	for _, task := range p.Request.Sources() {
		// Worker A resumes contiguous chunks beginning at zero; any existing chunk
		// or completion rejects this new workload before a GPU launch.
		for i := -1; i < 17; i++ {
			uri := p.WorkerTask(task.TaskIndex) + "/_SUCCESS.json"
			if i >= 0 {
				uri = fmt.Sprintf("%s/chunk-%06d.parquet", p.WorkerTask(task.TaskIndex), i)
			}
			_, err := c.Store.Read(ctx, uri, analysis.MaxObjectBytes)
			if !errors.Is(err, analysis.ErrMissing) {
				return errors.New("fresh Worker A output namespace is not empty")
			}
		}
	}
	return nil
}
func (c *Cloud) ResolvePreparation(ctx context.Context, p Plan) (*analysis.Request, bool, error) {
	if p.Preparation == nil || p.validatePreparation() != nil {
		return nil, false, errors.New("invalid preparation scope")
	}
	refs := []analysis.ObjectRef{p.Request.Manifest, *p.Request.Truth}
	for _, task := range p.Request.Sources() {
		refs = append(refs, task.Shard)
	}
	raw := make([][]byte, len(refs))
	missing := 0
	for i, ref := range refs {
		b, err := c.Store.Read(ctx, ref.URI, analysis.MaxObjectBytes)
		if errors.Is(err, analysis.ErrMissing) {
			missing++
			continue
		}
		if err != nil {
			return nil, false, errors.New("preparation output unavailable")
		}
		raw[i] = b
	}
	if missing == len(refs) {
		base := strings.TrimSuffix(p.Request.Manifest.URI, "manifest.json")
		for _, name := range []string{"manifest.sha256", "run.json"} {
			_, err := c.Store.Read(ctx, base+name, 1<<20)
			if !errors.Is(err, analysis.ErrMissing) {
				return nil, false, errors.New("partial preparation metadata already exists")
			}
		}
		return nil, false, nil
	}
	if missing != 0 {
		return nil, false, errors.New("partial preparation cannot launch GPU")
	}
	var mf struct {
		RunID   string `json:"run_id"`
		Schema  string `json:"schema_version"`
		Prompt  string `json:"prompt_version"`
		Dataset string `json:"dataset_version"`
		Records int    `json:"records_total"`
		Shards  []struct {
			Index   int    `json:"index"`
			URI     string `json:"uri"`
			Records int    `json:"records"`
			Hash    string `json:"sha256"`
		} `json:"shards"`
	}
	if json.Unmarshal(raw[0], &mf) != nil || mf.RunID != p.Request.SourceRunID || mf.Prompt != "worker-a-v2" || mf.Dataset != "gym-sales-v1" || mf.Schema != "sales-analysis-v1" || mf.Records != 900 || len(mf.Shards) != 3 {
		return nil, false, errors.New("preparation manifest differs from approved workload")
	}
	hash := analysis.Digest(raw[0])
	digest, err := c.Store.Read(ctx, strings.TrimSuffix(refs[0].URI, "manifest.json")+"manifest.sha256", 128)
	if err != nil || strings.TrimSpace(string(digest)) != hash {
		return nil, false, errors.New("preparation manifest digest missing or mismatched")
	}
	metadata, err := c.Store.Read(ctx, strings.TrimSuffix(refs[0].URI, "manifest.json")+"run.json", 1<<20)
	var run struct {
		RunID       string `json:"run_id"`
		GitSHA      string `json:"git_sha"`
		ManifestSHA string `json:"manifest_sha256"`
	}
	if err != nil || json.Unmarshal(metadata, &run) != nil || run.RunID != mf.RunID || run.GitSHA != p.Preparation.GitSHA || run.ManifestSHA != hash {
		return nil, false, errors.New("preparation provenance differs")
	}
	truth, err := parquet.OpenFile(bytes.NewReader(raw[1]), int64(len(raw[1])))
	if err != nil || truth.NumRows() != 900 {
		return nil, false, errors.New("fresh truth must cover 900 records")
	}
	request := *p.Request
	request.AdditionalTasks = append([]analysis.TaskSource(nil), p.Request.AdditionalTasks...)
	request.Manifest.SHA256 = hash
	v := *request.Truth
	v.SHA256 = analysis.Digest(raw[1])
	request.Truth = &v
	for i, shard := range mf.Shards {
		if shard.Index != i || shard.Records != 300 || shard.URI != refs[i+2].URI || shard.Hash != analysis.Digest(raw[i+2]) {
			return nil, false, errors.New("prepared shard identity or checksum mismatch")
		}
		if i == 0 {
			request.Shard.SHA256 = shard.Hash
		} else {
			request.AdditionalTasks[i-1].Shard.SHA256 = shard.Hash
		}
	}
	if request.Validate(true) != nil {
		return nil, false, errors.New("resolved preparation request invalid")
	}
	if err := c.verifyRaw(ctx, p); err != nil {
		return nil, false, err
	}
	return &request, true, nil
}
func preparedEqual(a, b *analysis.Request) bool { return reflect.DeepEqual(a, b) }
