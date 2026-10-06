package analysis

import (
	"context"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

var ErrExists = errors.New("immutable object already exists")
var ErrMissing = errors.New("object not found")

type Store interface {
	Read(context.Context, string, int64) ([]byte, error)
	Create(context.Context, string, []byte) error
}

// Objects uses create-only writes, bounded reads and no storage retries.
type Objects struct {
	client *storage.Client
	Remote bool
}

func (s *Objects) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
func (s *Objects) object(ctx context.Context, uri string) (*storage.ObjectHandle, error) {
	if !s.Remote || !strings.HasPrefix(uri, "gs://") {
		return nil, errors.New("remote object required")
	}
	parts := strings.SplitN(strings.TrimPrefix(uri, "gs://"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, errors.New("invalid object URI")
	}
	if s.client == nil {
		c, err := storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
		s.client = c
	}
	return s.client.Bucket(parts[0]).Retryer(storage.WithPolicy(storage.RetryNever)).Object(parts[1]), nil
}
func (s *Objects) Read(ctx context.Context, uri string, limit int64) (result []byte, retErr error) {
	span := telemetry.Begin(ctx, "storage", "read")
	defer func() {
		span.Bytes(int64(len(result)))
		if errors.Is(retErr, ErrMissing) {
			span.Expected("missing")
		}
		if errors.Is(retErr, ErrExists) {
			span.Expected("exists")
		}
		span.End(retErr != nil)
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var reader io.ReadCloser
	var err error
	if s.Remote {
		var obj *storage.ObjectHandle
		obj, err = s.object(ctx, uri)
		if err == nil {
			reader, err = obj.NewReader(ctx)
		}
	} else {
		reader, err = os.Open(uri)
	}
	if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, os.ErrNotExist) {
		return nil, ErrMissing
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("object exceeds size bound")
	}
	return data, nil
}
func (s *Objects) Create(ctx context.Context, uri string, data []byte) (retErr error) {
	span := telemetry.Begin(ctx, "storage", "create")
	defer func() {
		span.Bytes(int64(len(data)))
		if errors.Is(retErr, ErrMissing) {
			span.Expected("missing")
		}
		if errors.Is(retErr, ErrExists) {
			span.Expected("exists")
		}
		span.End(retErr != nil)
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.Remote {
		if err := os.MkdirAll(filepath.Dir(uri), 0700); err != nil {
			return err
		}
		// Link publishes a fully written object atomically; partial writes are never visible.
		f, err := os.CreateTemp(filepath.Dir(uri), ".pending-")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		if _, err = f.Write(data); err != nil {
			f.Close()
			return err
		}
		if err = f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		err = os.Link(f.Name(), uri)
		if errors.Is(err, os.ErrExist) {
			return ErrExists
		}
		return err
	}
	obj, err := s.object(ctx, uri)
	if err != nil {
		return err
	}
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err = w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	err = w.Close()
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == 412 {
		return ErrExists
	}
	return err
}
