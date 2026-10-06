package main

import "testing"

func TestModelSelectionRequiresExplicitModeBeforeProviderSetup(t *testing.T) {
	for _, args := range [][]string{nil, {"--remote"}, {"--model", "unsupported"}, {"--remote", "--model", "fake"}} {
		if _, err := parseOptions(args, "missing-request.json"); err == nil {
			t.Fatalf("implicit or invalid execution mode accepted: %v", args)
		}
	}
	for _, args := range [][]string{{"--model", "fake"}, {"--remote", "--model", "vertex"}} {
		if _, err := parseOptions(args, "missing-request.json"); err != nil {
			t.Fatalf("explicit model selection rejected: %v: %v", args, err)
		}
	}
}
