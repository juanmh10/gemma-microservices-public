package preparation

import (
	"encoding/json"
	"errors"
	"regexp"
)

type Domain struct {
	Version      string            `json:"version"`
	Task         string            `json:"task"`
	Outcomes     map[string]string `json:"outcomes"`
	Causes       map[string]string `json:"causes"`
	EvidenceRole string            `json:"evidence_role"`
}

func parseDomain(raw []byte) (Domain, error) {
	var d Domain
	var fields map[string]json.RawMessage
	if len(raw) > 65536 || json.Unmarshal(raw, &d) != nil || json.Unmarshal(raw, &fields) != nil || len(fields) != 5 {
		return d, errors.New("invalid domain configuration")
	}
	for _, k := range []string{"version", "task", "outcomes", "causes", "evidence_role"} {
		if _, ok := fields[k]; !ok {
			return d, errors.New("missing domain field")
		}
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{2,63}$`).MatchString(d.Version) || len([]rune(d.Task)) < 1 || len([]rune(d.Task)) > 4000 || (d.EvidenceRole != "customer" && d.EvidenceRole != "seller") || len(d.Outcomes) != 2 || d.Outcomes["success"] == "" || d.Outcomes["failure"] == "" || len(d.Causes) < 1 || len(d.Causes) > 64 {
		return d, errors.New("invalid domain definitions")
	}
	for k, v := range d.Causes {
		if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(k) || len([]rune(v)) < 1 || len([]rune(v)) > 2000 {
			return d, errors.New("invalid cause definition")
		}
	}
	for _, v := range d.Outcomes {
		if len([]rune(v)) > 2000 {
			return d, errors.New("invalid outcome definition")
		}
	}
	return d, nil
}
