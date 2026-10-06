package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

type Handle struct {
	SourceRecovered bool   `json:"source_recovered,omitempty"`
	RequestedAt     string `json:"requested_at,omitempty"`
	Operation       string `json:"operation,omitempty"`
	Execution       string `json:"execution,omitempty"`
}
type Stage struct {
	Requested bool   `json:"requested"`
	Done      bool   `json:"done"`
	Handle    Handle `json:"handle"`
}
type State struct {
	PreparedRequest *analysis.Request `json:"prepared_request,omitempty"`
	Version         string            `json:"schema_version"`
	PlanSHA256      string            `json:"plan_sha256"`
	Stages          map[string]Stage  `json:"stages"`
}
type Journal interface {
	Load() (State, error)
	Save(State) error
}
type FileJournal struct{ path, lock string }

// The workspace lock serializes local operators across analyses. Never steal a
// stale lock: inspect cloud executions before operator-authorized removal.
func OpenJournal(dir string, p Plan) (*FileJournal, error) {
	if p.Validate() != nil {
		return nil, errors.New("invalid journal plan")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, errors.New("journal workspace unavailable")
	}
	lock := filepath.Join(dir, ".operator.lock")
	f, err := os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, errors.New("operator lock exists or cannot be created; do not launch")
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(lock)
		return nil, errors.New("operator lock unavailable")
	}
	return &FileJournal{path: filepath.Join(dir, p.AnalysisID+".json"), lock: lock}, nil
}
func (j *FileJournal) Close() error { return os.Remove(j.lock) }
func (j *FileJournal) Load() (State, error) {
	raw, err := (&analysis.Objects{}).Read(context.Background(), j.path, 64<<10)
	if errors.Is(err, analysis.ErrMissing) {
		return State{}, nil
	}
	if err != nil || len(raw) > 64<<10 {
		return State{}, errors.New("journal read unavailable")
	}
	var s State
	if analysis.Decode(raw, &s) != nil || s.Version != Version || !digestPattern.MatchString(s.PlanSHA256) || s.Stages == nil {
		return State{}, errors.New("invalid journal")
	}
	return s, nil
}
func (j *FileJournal) Save(s State) error {
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return errors.New("journal encoding failed")
	}
	dir := filepath.Dir(j.path)
	f, err := os.CreateTemp(dir, ".journal-")
	if err != nil {
		return errors.New("journal write unavailable")
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(raw); err != nil {
		f.Close()
		return errors.New("journal write failed")
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return errors.New("journal sync failed")
	}
	if err = f.Close(); err != nil {
		return errors.New("journal close failed")
	}
	if err = os.Rename(f.Name(), j.path); err != nil {
		return errors.New("journal commit failed")
	}
	d, err := os.Open(dir)
	if err != nil {
		return errors.New("journal directory unavailable")
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return errors.New("journal directory sync failed")
	}
	return nil
}
