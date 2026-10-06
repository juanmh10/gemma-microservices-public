package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/juanmh10/gemma-microservices/internal/benchmark"
	"github.com/juanmh10/gemma-microservices/internal/gcs"
	"github.com/juanmh10/gemma-microservices/internal/preparation"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	run "google.golang.org/api/run/v2"
)

func freshPlan(t *testing.T) Plan {
	p := batchPlan(t)
	p.WorkerA.CanaryID = p.Request.SourceRunID
	p.Preparation = &Preparation{Image: "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/dataset-preparer@sha256:" + strings.Repeat("e", 64), GitSHA: strings.Repeat("f", 40), Source: analysis.ObjectRef{URI: "gs://your-gcp-project-id-raw/fixture/source.jsonl", SHA256: strings.Repeat("a", 64)}, Metadata: analysis.ObjectRef{URI: "gs://your-gcp-project-id-raw/fixture/metadata.jsonl", SHA256: strings.Repeat("b", 64)}}
	ref := func(uri string) analysis.ObjectRef { return analysis.ObjectRef{URI: uri, SHA256: prospectiveHash} }
	p.Request.Manifest = ref("gs://your-gcp-project-id-prepared/runs/" + p.Request.SourceRunID + "/manifest.json")
	truth := ref("gs://your-gcp-project-id-ground-truth/runs/" + p.Request.SourceRunID + "/labels.parquet")
	p.Request.Truth = &truth
	p.Request.AdditionalTasks = nil
	for i := range 3 {
		task := analysis.TaskSource{TaskIndex: i, Shard: ref(fmt.Sprintf("gs://your-gcp-project-id-prepared/runs/%s/shards/shard-%05d.jsonl.zst", p.Request.SourceRunID, i)), Marker: ref(p.WorkerTask(i) + "/_SUCCESS.json"), Chunks: []analysis.ObjectRef{ref(p.WorkerTask(i) + "/chunk-000000.parquet")}}
		if i == 0 {
			p.Request.TaskIndex = 0
			p.Request.Shard = task.Shard
			p.Request.Marker = task.Marker
			p.Request.Chunks = task.Chunks
		} else {
			p.Request.AdditionalTasks = append(p.Request.AdditionalTasks, task)
		}
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}

// Production preparation generates the 900-record fixture; only local test paths
// are translated into the approved remote namespace.
func preparationFixture(t *testing.T, p *Plan) (*artifactStore, map[string][]byte) {
	t.Helper()
	root := t.TempDir()
	var source, metadata bytes.Buffer
	for i := range 900 {
		_ = json.NewEncoder(&source).Encode(map[string]any{"sample_id": i, "outcome": "failure", "dialogue": []map[string]string{{"role": "customer", "text": "Synthetic fixture"}}})
		_ = json.NewEncoder(&metadata).Encode(map[string]any{"sample_id": i, "hidden_objection_ids": []string{"price"}})
	}
	sourcePath, metadataPath := filepath.Join(root, "source.jsonl"), filepath.Join(root, "metadata.jsonl")
	for path, raw := range map[string][]byte{sourcePath: source.Bytes(), metadataPath: metadata.Bytes()} {
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("GIT_SHA", p.Preparation.GitSHA)
	store := gcs.New()
	defer store.Close()
	prepared, truth := filepath.Join(root, "prepared"), filepath.Join(root, "truth")
	if err := preparation.Run(context.Background(), store, sourcePath, metadataPath, prepared, truth, p.Request.SourceRunID, "worker-a-v2", 900, 3); err != nil {
		t.Fatal(err)
	}
	read := func(path string) []byte {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	raw := read(filepath.Join(prepared, p.Request.SourceRunID, "manifest.json"))
	var mf map[string]any
	if err := json.Unmarshal(raw, &mf); err != nil {
		t.Fatal(err)
	}
	outputs := map[string][]byte{p.Request.Truth.URI: read(filepath.Join(truth, p.Request.SourceRunID, "labels.parquet"))}
	for i, task := range p.Request.Sources() {
		mf["shards"].([]any)[i].(map[string]any)["uri"] = task.Shard.URI
		outputs[task.Shard.URI] = read(filepath.Join(prepared, p.Request.SourceRunID, "shards", fmt.Sprintf("shard-%05d.jsonl.zst", i)))
	}
	outputs[p.Request.Manifest.URI] = analysis.JSON(mf)
	hash := analysis.Digest(outputs[p.Request.Manifest.URI])
	base := strings.TrimSuffix(p.Request.Manifest.URI, "manifest.json")
	outputs[base+"manifest.sha256"] = []byte(hash + "\n")
	outputs[base+"run.json"] = analysis.JSON(map[string]string{"run_id": p.Request.SourceRunID, "git_sha": p.Preparation.GitSHA, "manifest_sha256": hash})
	s := &artifactStore{files: map[string][]byte{p.Preparation.Source.URI: source.Bytes(), p.Preparation.Metadata.URI: metadata.Bytes()}}
	p.Preparation.Source.SHA256 = analysis.Digest(source.Bytes())
	p.Preparation.Metadata.SHA256 = analysis.Digest(metadata.Bytes())
	return s, outputs
}
func TestPreparationResolvesProductionArtifactsAndRejectsMutation(t *testing.T) {
	p := freshPlan(t)
	s, outputs := preparationFixture(t, &p)
	c := Cloud{Store: s}
	if _, ready, err := c.ResolvePreparation(context.Background(), p); err != nil || ready {
		t.Fatal("missing preparation passed", err)
	}
	for uri, raw := range outputs {
		s.files[uri] = raw
	}
	request, ready, err := c.ResolvePreparation(context.Background(), p)
	if err != nil || !ready || request.Manifest.SHA256 == prospectiveHash {
		t.Fatal("real preparation rejected", err)
	}
	if p.Request.Manifest.SHA256 != prospectiveHash {
		t.Fatal("approved plan mutated")
	}
	for _, uri := range []string{p.Request.Truth.URI, p.Request.AdditionalTasks[1].Shard.URI, p.Preparation.Source.URI, strings.TrimSuffix(p.Request.Manifest.URI, "manifest.json") + "run.json"} {
		original := s.files[uri]
		s.files[uri] = []byte("corrupt")
		if _, _, err := c.ResolvePreparation(context.Background(), p); err == nil {
			t.Fatal("mutation accepted", uri)
		}
		s.files[uri] = original
	}
	delete(s.files, p.Request.AdditionalTasks[1].Shard.URI)
	if _, _, err := c.ResolvePreparation(context.Background(), p); err == nil {
		t.Fatal("partial preparation accepted")
	}
}

type freshBackend struct {
	fakeBackend
	cloud   Cloud
	outputs map[string][]byte
}

func (b *freshBackend) ResolvePreparation(ctx context.Context, p Plan) (*analysis.Request, bool, error) {
	return b.cloud.ResolvePreparation(ctx, p)
}
func (b *freshBackend) VerifyFreshWorker(ctx context.Context, p Plan) error {
	return b.cloud.VerifyFreshWorker(ctx, p)
}
func (b *freshBackend) ResolveSource(ctx context.Context, p Plan) (*analysis.Request, bool, error) {
	return b.cloud.ResolveSource(ctx, p)
}
func (b *freshBackend) Wait(ctx context.Context, p Plan, stage string, h Handle, save func(Handle) error) error {
	if err := b.fakeBackend.Wait(ctx, p, stage, h, save); err != nil {
		return err
	}
	s := b.cloud.Store.(*artifactStore)
	switch stage {
	case "prepare":
		for uri, raw := range b.outputs {
			s.files[uri] = raw
		}
	case "worker-a":
		for i := range 3 {
			marker := benchmark.Marker{Status: "success", RunID: p.Request.SourceRunID, TaskIndex: i, GPUProfile: "rtx6000", ImageDigest: p.WorkerA.Image, ManifestSHA256: p.Request.Manifest.SHA256, RecordsTotal: 300, RecordsProcessed: 300, PromptVersion: "worker-a-v2", PromptSHA256: "17c5f97f389ab084f37498e8800763a9537dbc7ef39c0394edfeacc3b65254aa", ModelRevision: "c333706ed87619f80159b1f0c5685b71dfafeea8"}
			s.files[p.WorkerTask(i)+"/_SUCCESS.json"] = analysis.JSON(marker)
			s.files[p.WorkerTask(i)+"/chunk-000000.parquet"] = []byte("synthetic GPU output")
		}
	}
	return nil
}
func TestFreshCoordinatorPreparesThenLaunchesOnceAndReplays(t *testing.T) {
	p := freshPlan(t)
	s, outputs := preparationFixture(t, &p)
	b := &freshBackend{cloud: Cloud{Store: s}, outputs: outputs}
	j := &memoryJournal{}
	r := Runner{Backend: b, Journal: j, PollInterval: time.Millisecond}
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	want := []string{"prepare", "worker-a", "analysis", "index", "publish"}
	if !reflect.DeepEqual(b.starts, want) || j.state.PreparedRequest == nil {
		t.Fatal("automatic stages incomplete", b.starts)
	}
	if _, err := r.Execute(context.Background(), p, p.Digest()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.starts, want) {
		t.Fatal("completed replay relaunched", b.starts)
	}
}
func TestFreshCoordinatorStopsBeforeGPUOnPreparationFailureOrReuse(t *testing.T) {
	for _, scenario := range []string{"failed", "reused", "gpu-reused", "analysis-reused"} {
		t.Run(scenario, func(t *testing.T) {
			p := freshPlan(t)
			s, outputs := preparationFixture(t, &p)
			b := &freshBackend{cloud: Cloud{Store: s}, outputs: outputs}
			switch scenario {
			case "failed":
				b.waitErr = "prepare"
			case "reused":
				for uri, raw := range outputs {
					s.files[uri] = raw
				}
			case "analysis-reused":
				s.files[p.RequestURI()] = []byte("old analysis request")
			case "gpu-reused":
				s.files[p.WorkerTask(0)+"/chunk-000000.parquet"] = []byte("old")
			}
			r := Runner{Backend: b, Journal: &memoryJournal{}}
			if _, err := r.Execute(context.Background(), p, p.Digest()); err == nil {
				t.Fatal("fresh acceptance violation hidden")
			}
			for _, stage := range b.starts {
				if stage != "prepare" {
					t.Fatal("GPU/downstream launched", b.starts)
				}
			}
		})
	}
}
func TestPreparationSDKRejectsDriftAndNeverRetriesLaunch(t *testing.T) {
	for _, scenario := range []string{"success", "ambiguous", "wrong-source", "wrong-limit"} {
		t.Run(scenario, func(t *testing.T) {
			p := freshPlan(t)
			s, _ := preparationFixture(t, &p)
			j := jobFixture(p, "prepare")
			env := map[string]string{"SOURCE_URI": p.Preparation.Source.URI, "METADATA_URI": p.Preparation.Metadata.URI, "RUN_ID": p.Request.SourceRunID, "GIT_SHA": p.Preparation.GitSHA, "PREPARED_PREFIX": "gs://your-gcp-project-id-prepared/runs", "GROUND_TRUTH_PREFIX": "gs://your-gcp-project-id-ground-truth/runs"}
			if scenario == "wrong-source" {
				env["SOURCE_URI"] += "wrong"
			}
			container := j.Template.Template.Containers[0]
			container.Env = nil
			container.Args = p.Args("prepare")
			if scenario == "wrong-limit" {
				container.Args = []string{"--limit", "1800"}
			}
			for k, v := range env {
				container.Env = append(container.Env, &run.GoogleCloudRunV2EnvVar{Name: k, Value: v})
			}
			posts := 0
			c := testCloud(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					reply(w, j)
					return
				}
				posts++
				if scenario == "ambiguous" {
					w.WriteHeader(503)
					return
				}
				var req run.GoogleCloudRunV2RunJobRequest
				if json.NewDecoder(r.Body).Decode(&req) != nil || req.Overrides == nil || !reflect.DeepEqual(req.Overrides.ContainerOverrides[0].Args, p.Args("prepare")) {
					t.Error("unbounded preparation launch")
				}
				reply(w, &run.GoogleLongrunningOperation{Name: Root + "/operations/preparation"})
			})
			c.Store = s
			_, err := c.Start(context.Background(), p, "prepare")
			if (err != nil) != (scenario != "success") {
				t.Fatal("unexpected launch result", err)
			}
			want := 1
			if strings.HasPrefix(scenario, "wrong-") {
				want = 0
			}
			if posts != want {
				t.Fatal("unsafe launch count", posts)
			}
		})
	}
}
