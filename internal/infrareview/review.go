// Package infrareview validates a coordinated plan without merging Terraform states.
package infrareview

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type Variable struct {
	Value any `json:"value"`
}
type Resource struct {
	Address, Mode, Type string
	Values              map[string]any
}
type Module struct {
	Resources    []Resource
	ChildModules []Module `json:"child_modules"`
}
type Change struct {
	Address, Mode, Type string
	Change              struct {
		Actions       []string
		Before, After map[string]any
	}
}
type Plan struct {
	Errored       bool
	Variables     map[string]Variable
	PlannedValues struct {
		RootModule Module `json:"root_module"`
	} `json:"planned_values"`
	ResourceChanges []Change `json:"resource_changes"`
}
type Summary struct{ Create, Update, Delete, Replace int }

func resources(m Module) []Resource {
	out := append([]Resource(nil), m.Resources...)
	for _, c := range m.ChildModules {
		out = append(out, resources(c)...)
	}
	return out
}
func Parse(data []byte) (Plan, error) {
	var p Plan
	err := json.Unmarshal(data, &p)
	if err != nil {
		return p, errors.New("invalid Terraform plan JSON")
	}
	if p.Errored || p.Variables == nil || len(resources(p.PlannedValues.RootModule)) == 0 {
		return p, errors.New("incomplete or errored Terraform plan")
	}
	return p, nil
}
func (p Plan) Value(name string) any { return p.Variables[name].Value }
func Review(foundation, application Plan, full bool) ([2]Summary, error) {
	var totals [2]Summary
	owned := map[string]int{}
	protected := false
	for n, p := range []Plan{foundation, application} {
		if p.Value("project_id") != "your-gcp-project-id" || p.Value("region") != "us-central1" {
			return totals, errors.New("unexpected project or region")
		}
		for _, r := range resources(p.PlannedValues.RootModule) {
			if r.Mode != "managed" {
				continue
			}
			id, _ := r.Values["id"].(string)
			if id != "" {
				key := r.Type + "/" + id
				if prior, ok := owned[key]; ok && prior != n {
					return totals, errors.New("resource is owned by both states")
				}
				owned[key] = n
			}
			if r.Type == "google_storage_bucket" && r.Values["name"] == "your-gcp-project-id-models" {
				protected = n == 0 && r.Values["deletion_policy"] == "PREVENT" && r.Values["force_destroy"] == false
				ac, _ := r.Values["autoclass"].([]any)
				protected = protected && len(ac) == 1
				if protected {
					v, _ := ac[0].(map[string]any)
					protected = v["enabled"] == true && v["terminal_storage_class"] == "NEARLINE"
				}
				rules, _ := r.Values["lifecycle_rule"].([]any)
				protected = protected && len(rules) == 0
			}
		}
		for _, r := range p.ResourceChanges {
			if r.Mode != "managed" {
				continue
			}
			actions := map[string]bool{}
			for _, a := range r.Change.Actions {
				actions[a] = true
			}
			if actions["delete"] {
				// Scoped IAM condition changes are replacements; data/compute removal is excluded.
				b, a := r.Change.Before, r.Change.After
				if r.Type != "google_storage_bucket_iam_member" || !actions["create"] || b == nil || a == nil || bucket(b["bucket"]) != bucket(a["bucket"]) || b["role"] != a["role"] || b["member"] != a["member"] {
					return totals, fmt.Errorf("destructive change blocked: %s", r.Address)
				}
				totals[n].Replace++
			} else if actions["create"] {
				totals[n].Create++
			} else if actions["update"] {
				totals[n].Update++
			}
		}
	}
	if !protected {
		return totals, errors.New("protected models bucket is absent or protection differs")
	}
	if full {
		for _, k := range []string{"deploy_jobs", "deploy_preparer", "require_rtx_compile_cache"} {
			if foundation.Value(k) != true {
				return totals, fmt.Errorf("full stack requires %s", k)
			}
		}
		if foundation.Value("deploy_l4") != false || foundation.Value("export_rtx_compile_cache") != false || foundation.Value("pilot_records") != float64(900) || foundation.Value("preparer_shards") != float64(3) || foundation.Value("rtx_canary_tasks") != float64(3) {
			return totals, errors.New("full stack must use three bounded cached RTX tasks and three 300-record shards")
		}
		run, _ := foundation.Value("run_id").(string)
		canary, _ := foundation.Value("rtx_canary_id").(string)
		pattern := regexp.MustCompile(`^pipeline-[a-z0-9-]{3,30}$`)
		if !pattern.MatchString(run) || !pattern.MatchString(canary) || application.Value("prepared_run_id") != run || application.Value("worker_a_canary_id") != canary {
			return totals, errors.New("fresh run and canary must match across roots")
		}
		for _, k := range []string{"deploy_analysis", "deploy_data_compute", "deploy_messaging_queues", "deploy_messaging_compute", "enable_email", "provision_email_secret"} {
			if application.Value(k) != true {
				return totals, fmt.Errorf("full stack requires %s", k)
			}
		}
		if application.Value("vertex_location") != "us" {
			return totals, errors.New("unexpected Vertex configuration")
		}
	}
	return totals, nil
}

// The provider records bucket IAM state as b/<name> while configuration uses <name>.
func bucket(v any) string { s, _ := v.(string); return strings.TrimPrefix(s, "b/") }
