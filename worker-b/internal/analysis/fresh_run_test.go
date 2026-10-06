package analysis

import "testing"

func TestFreshRunAllowlistRejectsOtherRunsTraversalAndOtherTasks(t *testing.T) {
	run := "pipeline-fresh-20261003-01"
	for _, bucket := range []string{"prepared", "ground-truth"} {
		uri := "gs://your-gcp-project-id-" + bucket + "/runs/" + run + "/manifest.json"
		if !allowedReadForRun(uri, run) {
			t.Fatal("fresh prepared reference rejected")
		}
		if allowedReadForRun(uri, "pipeline-other-01") {
			t.Fatal("another run accepted")
		}
	}
	prefix := "gs://your-gcp-project-id-results/canary/pipeline-fresh-20261003-01/runs/" + run + "/worker-a/rtx6000/"
	if !allowedReadForRun(prefix+"task-00000/_SUCCESS.json", run) {
		t.Fatal("fresh completion rejected")
	}
	for _, suffix := range []string{"task-00003/_SUCCESS.json", "task-00000/../_SUCCESS.json", "task-00000/_SUCCESS.json?token=x", "task-00000/unknown.parquet"} {
		if allowedReadForRun(prefix+suffix, run) {
			t.Fatal("unsafe fresh reference accepted")
		}
	}
	if allowedReadForRun("gs://your-gcp-project-id-prepared/runs/pilot-004/manifest.json", run) {
		t.Fatal("retained run accepted for fresh request")
	}
}
