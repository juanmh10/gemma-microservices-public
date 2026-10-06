package pipeline

import (
	"context"
	"errors"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

type artifactStore struct {
	files map[string][]byte
	reads []string
}

func (s *artifactStore) Read(_ context.Context, uri string, _ int64) ([]byte, error) {
	s.reads = append(s.reads, uri)
	if raw, ok := s.files[uri]; ok {
		return raw, nil
	}
	return nil, analysis.ErrMissing
}
func (s *artifactStore) Create(context.Context, string, []byte) error {
	return errors.New("unexpected write")
}
