package preparation

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/internal/gcs"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
)

type message struct {
	TurnID int    `json:"turn_id"`
	Role   string `json:"role"`
	Text   string `json:"text"`
}

type sourceMessage struct {
	Role string `json:"role"`
	Text string `json:"text"`
}

type sourceRow struct {
	SampleID          int             `json:"sample_id"`
	Outcome           string          `json:"outcome"`
	DecisionTurnIndex int             `json:"decision_turn_index"`
	Dialogue          []sourceMessage `json:"dialogue"`
}

type conversation struct {
	RecordID string    `json:"record_id"`
	Messages []message `json:"messages"`
}

type label struct {
	RecordID          string `parquet:"record_id,zstd"`
	Outcome           string `parquet:"outcome,zstd"`
	DecisionTurnIndex int32  `parquet:"decision_turn_index"`
	MetadataJSON      string `parquet:"metadata_json,zstd"`
}

type shardManifest struct {
	Index   int    `json:"index"`
	URI     string `json:"uri"`
	Records int    `json:"records"`
	SHA256  string `json:"sha256"`
}

type manifest struct {
	DomainVersion  string          `json:"domain_version,omitempty"`
	DomainSHA256   string          `json:"domain_sha256,omitempty"`
	DomainURI      string          `json:"domain_uri,omitempty"`
	RunID          string          `json:"run_id"`
	DatasetVersion string          `json:"dataset_version"`
	SchemaVersion  string          `json:"schema_version"`
	PromptVersion  string          `json:"prompt_version"`
	RecordsTotal   int             `json:"records_total"`
	Shards         []shardManifest `json:"shards"`
}

type shardFile struct {
	file    *os.File
	encoder *zstd.Encoder
	writer  *bufio.Writer
	count   int
}

func eachJSONLine(reader io.Reader, visit func([]byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	for scanner.Scan() {
		if err := visit(scanner.Bytes()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func putFile(ctx context.Context, store *gcs.Store, uri, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return store.Put(ctx, uri, file)
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func Run(ctx context.Context, store *gcs.Store, sourceURI, metadataURI, preparedPrefix, truthPrefix, runID, promptVersion string, limit, shardCount int) error {
	return run(ctx, store, sourceURI, metadataURI, preparedPrefix, truthPrefix, runID, promptVersion, limit, shardCount, "", "", false)
}

// RunConfigured binds a versioned domain to gym or canonical dialogue-only inputs.
func RunConfigured(ctx context.Context, store *gcs.Store, sourceURI, metadataURI, preparedPrefix, truthPrefix, runID, datasetVersion, domainURI string, canonical bool, limit, shardCount int) error {
	if domainURI == "" || datasetVersion == "" {
		return errors.New("domain and dataset version are required")
	}
	return run(ctx, store, sourceURI, metadataURI, preparedPrefix, truthPrefix, runID, "worker-a-v3", limit, shardCount, domainURI, datasetVersion, canonical)
}

func run(ctx context.Context, store *gcs.Store, sourceURI, metadataURI, preparedPrefix, truthPrefix, runID, promptVersion string, limit, shardCount int, domainURI, datasetVersion string, canonical bool) error {
	if sourceURI == "" || metadataURI == "" || preparedPrefix == "" || truthPrefix == "" || runID == "" {
		return errors.New("source, metadata, prepared-prefix, ground-truth-prefix, and run-id are required")
	}
	if promptVersion != "worker-a-v1" && promptVersion != "worker-a-v2" && promptVersion != "worker-a-v3" {
		return errors.New("unsupported prompt version")
	}
	if shardCount < 1 || limit < 0 || strings.Contains(runID, "/") {
		return errors.New("invalid shard count, limit, or run ID")
	}

	var domainRaw []byte
	var taskDomain Domain
	if promptVersion == "worker-a-v3" {
		reader, err := store.Open(ctx, domainURI)
		if err != nil {
			return err
		}
		domainRaw, err = io.ReadAll(io.LimitReader(reader, 65537))
		reader.Close()
		if err != nil {
			return err
		}
		taskDomain, err = parseDomain(domainRaw)
		if err != nil {
			return err
		}
	}
	if datasetVersion == "" {
		datasetVersion = "gym-sales-v1"
	}
	if strings.TrimSuffix(preparedPrefix, "/") == strings.TrimSuffix(truthPrefix, "/") {
		return errors.New("prepared and truth prefixes must be separate")
	}
	metadataReader, err := store.Open(ctx, metadataURI)
	if err != nil {
		return err
	}
	metadata := map[string]json.RawMessage{}
	err = eachJSONLine(metadataReader, func(line []byte) error {
		var row struct {
			SampleID int    `json:"sample_id"`
			RecordID string `json:"record_id"`
		}
		if err := json.Unmarshal(line, &row); err != nil {
			return err
		}
		key := fmt.Sprintf("conv_%06d", row.SampleID)
		if canonical {
			key = row.RecordID
			if key == "" {
				return errors.New("reference record ID is required")
			}
		}
		if _, exists := metadata[key]; exists {
			return fmt.Errorf("duplicate metadata ID %d", row.SampleID)
		}
		metadata[key] = append([]byte(nil), line...)
		return nil
	})
	metadataReader.Close()
	if err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", "gemma-preparer-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	shards := make([]*shardFile, shardCount)
	for i := range shards {
		file, err := os.Create(filepath.Join(tempDir, fmt.Sprintf("shard-%05d.jsonl.zst", i)))
		if err != nil {
			return err
		}
		encoder, err := zstd.NewWriter(file)
		if err != nil {
			file.Close()
			return err
		}
		shards[i] = &shardFile{file: file, encoder: encoder, writer: bufio.NewWriter(encoder)}
	}
	labels, err := os.Create(filepath.Join(tempDir, "labels.parquet"))
	if err != nil {
		return err
	}
	labelWriter := parquet.NewGenericWriter[label](labels)
	sourceReader, err := store.Open(ctx, sourceURI)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	count := 0
	err = eachJSONLine(sourceReader, func(line []byte) error {
		if limit > 0 && count >= limit {
			return nil
		}
		var row sourceRow
		if err := json.Unmarshal(line, &row); err != nil {
			return err
		}

		id := fmt.Sprintf("conv_%06d", row.SampleID)
		conv := conversation{RecordID: id, Messages: make([]message, 0, len(row.Dialogue))}
		if canonical {
			var input struct {
				RecordID string    `json:"record_id"`
				Messages []message `json:"messages"`
			}
			decoder := json.NewDecoder(strings.NewReader(string(line)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&input); err != nil {
				return err
			}
			id = input.RecordID
			conv = conversation{RecordID: id, Messages: input.Messages}
			if !regexp.MustCompile(`^conv_[0-9]{6,}$`).MatchString(id) {
				return errors.New("invalid canonical record ID")
			}
			var reference struct {
				Outcome    string   `json:"outcome"`
				CauseCodes []string `json:"cause_codes"`
			}
			if err := json.Unmarshal(metadata[id], &reference); err != nil {
				return err
			}
			if reference.Outcome != "success" && reference.Outcome != "failure" {
				return errors.New("invalid reference outcome")
			}
			var referenceFields map[string]json.RawMessage
			if err := json.Unmarshal(metadata[id], &referenceFields); err != nil {
				return err
			}
			if _, ok := referenceFields["cause_codes"]; !ok || reference.CauseCodes == nil {
				return errors.New("reference cause_codes array is required")
			}
			codes := map[string]bool{}
			for _, code := range reference.CauseCodes {
				if _, ok := taskDomain.Causes[code]; !ok || codes[code] {
					return errors.New("unknown or duplicate reference cause")
				}
				codes[code] = true
			}
			row.Outcome = reference.Outcome
		} else {
			for i, turn := range row.Dialogue {
				role := turn.Role
				if role == "sales" {
					role = "seller"
				}
				conv.Messages = append(conv.Messages, message{TurnID: i + 1, Role: role, Text: turn.Text})
			}
		}
		if seen[id] || len(metadata[id]) == 0 {
			return errors.New("duplicate or unmatched record ID")
		}
		seen[id] = true
		turns := map[int]bool{}
		for _, turn := range conv.Messages {
			if turn.TurnID < 1 || turns[turn.TurnID] || (turn.Role != "seller" && turn.Role != "customer") || strings.TrimSpace(turn.Text) == "" {
				return errors.New("invalid conversation turn")
			}
			turns[turn.TurnID] = true
		}
		if len(conv.Messages) == 0 {
			return fmt.Errorf("empty dialogue for %s", id)
		}
		shard := shards[count%shardCount]
		if err := json.NewEncoder(shard.writer).Encode(conv); err != nil {
			return err
		}
		shard.count++
		if _, err := labelWriter.Write([]label{{RecordID: id, Outcome: row.Outcome, DecisionTurnIndex: int32(row.DecisionTurnIndex), MetadataJSON: string(metadata[id])}}); err != nil {
			return err
		}
		count++
		return nil
	})
	sourceReader.Close()
	if err != nil {
		return err
	}
	if count < shardCount {
		return fmt.Errorf("%d records cannot fill %d shards", count, shardCount)
	}
	if err := labelWriter.Close(); err != nil {
		return err
	}
	if err := labels.Close(); err != nil {
		return err
	}
	preparedBase := strings.TrimSuffix(preparedPrefix, "/") + "/" + runID
	truthBase := strings.TrimSuffix(truthPrefix, "/") + "/" + runID
	schemaVersion := "sales-analysis-v1"
	if promptVersion == "worker-a-v3" {
		schemaVersion = "conversation-analysis-v1"
	}
	result := manifest{RunID: runID, DatasetVersion: datasetVersion, SchemaVersion: schemaVersion, PromptVersion: promptVersion, RecordsTotal: count, Shards: make([]shardManifest, 0, shardCount)}

	if promptVersion == "worker-a-v3" {
		hash := sha256.Sum256(domainRaw)
		result.DomainVersion = taskDomain.Version
		result.DomainSHA256 = hex.EncodeToString(hash[:])
		result.DomainURI = preparedBase + "/domain.json"
		if err := store.Put(ctx, result.DomainURI, strings.NewReader(string(domainRaw))); err != nil {
			return err
		}
	}
	for i, shard := range shards {
		if err := shard.writer.Flush(); err != nil {
			return err
		}
		if err := shard.encoder.Close(); err != nil {
			return err
		}
		if err := shard.file.Close(); err != nil {
			return err
		}
		path := shard.file.Name()
		hash, err := fileSHA256(path)
		if err != nil {
			return err
		}
		uri := fmt.Sprintf("%s/shards/shard-%05d.jsonl.zst", preparedBase, i)
		if err := putFile(ctx, store, uri, path); err != nil {
			return err
		}
		result.Shards = append(result.Shards, shardManifest{Index: i, URI: uri, Records: shard.count, SHA256: hash})
	}
	if err := putFile(ctx, store, truthBase+"/labels.parquet", labels.Name()); err != nil {
		return err
	}
	manifestBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	manifestBytes = append(manifestBytes, '\n')
	digest := sha256.Sum256(manifestBytes)
	if err := store.Put(ctx, preparedBase+"/manifest.sha256", strings.NewReader(hex.EncodeToString(digest[:])+"\n")); err != nil {
		return err
	}
	runMetadata, err := json.MarshalIndent(map[string]any{
		"run_id":          runID,
		"dataset_version": datasetVersion,
		"schema_version":  schemaVersion,
		"prompt_version":  promptVersion,
		"domain_version":  result.DomainVersion,
		"domain_sha256":   result.DomainSHA256,
		"manifest_sha256": hex.EncodeToString(digest[:]),
		"git_sha":         os.Getenv("GIT_SHA"),
		"created_at":      time.Now().UTC().Format(time.RFC3339),
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := store.Put(ctx, preparedBase+"/run.json", strings.NewReader(string(append(runMetadata, '\n')))); err != nil {
		return err
	}
	manifestURI := preparedBase + "/manifest.json"
	if err := store.Put(ctx, manifestURI, strings.NewReader(string(manifestBytes))); err != nil {
		return err
	}
	fmt.Printf("prepared %d records in %d shards; manifest=%s sha256=%x\n", count, shardCount, manifestURI, digest)
	return nil
}
