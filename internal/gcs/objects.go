package gcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
)

// Store supports local paths and gs:// URIs without changing global gcloud state.
type Store struct{ client *storage.Client }

func New() *Store { return &Store{} }

func (s *Store) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *Store) ensureClient(ctx context.Context) error {
	if s.client != nil {
		return nil
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func split(uri string) (bucket, object string, remote bool, err error) {
	if !strings.HasPrefix(uri, "gs://") {
		return "", "", false, nil
	}
	parts := strings.SplitN(strings.TrimPrefix(uri, "gs://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", true, fmt.Errorf("invalid GCS object URI: %q", uri)
	}
	return parts[0], parts[1], true, nil
}

func (s *Store) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	bucket, object, remote, err := split(uri)
	if err != nil {
		return nil, err
	}
	if remote {
		if err := s.ensureClient(ctx); err != nil {
			return nil, err
		}
		return s.client.Bucket(bucket).Object(object).NewReader(ctx)
	}
	return os.Open(uri)
}

func (s *Store) Put(ctx context.Context, uri string, source io.Reader) error {
	bucket, object, remote, err := split(uri)
	if err != nil {
		return err
	}
	if !remote {
		if err := os.MkdirAll(filepath.Dir(uri), 0o755); err != nil {
			return err
		}
		file, err := os.Create(uri)
		if err != nil {
			return err
		}
		if _, err := io.Copy(file, source); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	}
	if err := s.ensureClient(ctx); err != nil {
		return err
	}
	writer := s.client.Bucket(bucket).Object(object).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if _, err := io.Copy(writer, source); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}
