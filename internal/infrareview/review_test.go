package infrareview

import "testing"

func fixture() (Plan, Plan) {
	a := Plan{Variables: map[string]Variable{"project_id": {Value: "your-gcp-project-id"}, "region": {Value: "us-central1"}}}
	b := Plan{Variables: map[string]Variable{"project_id": {Value: "your-gcp-project-id"}, "region": {Value: "us-central1"}}}
	a.PlannedValues.RootModule.Resources = []Resource{{Mode: "managed", Type: "google_storage_bucket", Values: map[string]any{"id": "your-gcp-project-id-models", "name": "your-gcp-project-id-models", "deletion_policy": "PREVENT", "force_destroy": false, "autoclass": []any{map[string]any{"enabled": true, "terminal_storage_class": "NEARLINE"}}}}}
	return a, b
}
func TestBlocksModelRemovalProtectionLossAndOverlappingOwnership(t *testing.T) {
	for _, tc := range []string{"delete", "force_destroy", "expire", "duplicate", "project"} {
		t.Run(tc, func(t *testing.T) {
			a, b := fixture()
			switch tc {
			case "delete":
				c := Change{Mode: "managed", Type: "google_storage_bucket", Address: "models"}
				c.Change.Actions = []string{"delete"}
				a.ResourceChanges = []Change{c}
			case "force_destroy":
				a.PlannedValues.RootModule.Resources[0].Values["force_destroy"] = true
			case "expire":
				a.PlannedValues.RootModule.Resources[0].Values["lifecycle_rule"] = []any{map[string]any{"age": 21}}
			case "duplicate":
				b.PlannedValues.RootModule.Resources = a.PlannedValues.RootModule.Resources
			case "project":
				b.Variables["project_id"] = Variable{Value: "other"}
			}
			if _, err := Review(a, b, false); err == nil {
				t.Fatal("unsafe consolidation accepted")
			}
		})
	}
}
func TestOnlySamePrincipalScopedIAMReplacementCanPass(t *testing.T) {
	a, b := fixture()
	c := Change{Mode: "managed", Type: "google_storage_bucket_iam_member", Address: "read"}
	c.Change.Actions = []string{"delete", "create"}
	c.Change.Before = map[string]any{"bucket": "b/prepared", "role": "roles/storage.objectViewer", "member": "analysis"}
	c.Change.After = map[string]any{"bucket": "prepared", "role": "roles/storage.objectViewer", "member": "analysis"}
	b.ResourceChanges = []Change{c}
	s, err := Review(a, b, false)
	if err != nil || s[1].Replace != 1 {
		t.Fatal("expected scoped replacement")
	}
	c.Change.After["member"] = "worker-a"
	b.ResourceChanges = []Change{c}
	if _, err := Review(a, b, false); err == nil {
		t.Fatal("changed principal accepted")
	}
}
func TestFullProfileRejectsHistoricalPartialDeployment(t *testing.T) {
	a, b := fixture()
	if _, err := Review(a, b, false); err != nil {
		t.Fatal(err)
	}
	if _, err := Review(a, b, true); err == nil {
		t.Fatal("paused jobs accepted for full stack")
	}
}
