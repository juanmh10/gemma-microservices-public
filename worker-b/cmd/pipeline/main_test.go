package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineReviewAndApprovalGate(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "no-credentials"))
	state := filepath.Join(t.TempDir(), "must-not-exist")
	var out bytes.Buffer
	if err := run([]string{"--plan", "../../internal/pipeline/testdata/plan-v1.json", "--state-dir", state}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "execution=blocked") || !strings.Contains(out.String(), "plan_sha256=") {
		t.Fatal("review did not show blocked plan digest")
	}
	if err := run([]string{"--plan", "../../internal/pipeline/testdata/plan-v1.json", "--execute", "--state-dir", state}, &out); err == nil {
		t.Fatal("execute without digest accepted")
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("offline/unapproved command created journal")
	}
}
