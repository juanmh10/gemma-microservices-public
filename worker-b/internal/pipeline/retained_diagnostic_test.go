//go:build diagnostics

package pipeline

import (
	"context"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/analytics"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/messaging"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotRetainedReportBinding(t *testing.T) {
	prefix := os.Getenv("RETAINED_REPORT_PREFIX")
	if prefix == "" {
		t.Skip("set RETAINED_REPORT_PREFIX to local retained reports")
	}
	if strings.Contains(prefix, "://") {
		t.Fatal("local retained fixture required")
	}
	p := example(t)
	s := &artifactStore{files: map[string][]byte{}}
	for _, name := range []string{"_SUCCESS.json", "report.json", "request.json", "metrics.json"} {
		raw, err := os.ReadFile(filepath.Join(prefix, p.AnalysisID, name))
		if err != nil {
			t.Fatal(err)
		}
		s.files[analytics.Prefix+"/"+p.AnalysisID+"/"+name] = raw
	}
	c := Cloud{Store: s}
	snapshot, err := c.Snapshot(context.Background(), p)
	if err != nil || !snapshot.Completed || snapshot.ReportSHA256 != p.ExpectedReportSHA256 || snapshot.Indexed {
		t.Fatalf("retained binding: %+v %v", snapshot, err)
	}
	p.ExpectedReportSHA256 = strings.Repeat("b", 64)
	if _, err := c.Snapshot(context.Background(), p); err == nil {
		t.Fatal("foreign report hash accepted")
	}
	for _, uri := range s.reads {
		if !strings.HasPrefix(uri, analytics.Prefix+"/") && !strings.HasPrefix(uri, analytics.IndexPrefix+"/") && !strings.HasPrefix(uri, messaging.PublicationPrefix+"/") && !strings.HasPrefix(uri, messaging.NotificationPrefix+"/") {
			t.Fatal("snapshot read outside downstream artifacts")
		}
	}
}
