package workerb

import (
	"strings"
	"testing"
)

func TestBatchInstructionUsesObservedCoverage(t *testing.T) {
	if ForVersion("worker-b-v1") != Instruction || ForVersion("worker-b-v2") != BatchInstruction || !strings.Contains(BatchInstruction, "records_expected and source_records") || strings.Contains(BatchInstruction, "Always disclose synthetic input and single-shard") {
		t.Fatal("batch coverage instruction or retained prompt selection changed")
	}
}
