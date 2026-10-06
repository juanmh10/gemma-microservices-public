// Package workerb contains the versioned Worker B instruction.
package workerb

import _ "embed"

//go:embed v1.txt
var Instruction string

//go:embed v2.txt
var BatchInstruction string

// ForVersion preserves the retained single-shard instruction unchanged.
func ForVersion(version string) string {
	if version == "worker-b-v2" {
		return BatchInstruction
	}
	return Instruction
}
