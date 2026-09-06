package security_test

import (
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/security"
)

func TestMerkleTreeAndIntegrityVerification(t *testing.T) {
	now := time.Now()
	steps := []*model.StepExecution{
		{
			ID:         "step_1",
			StageID:    "validate_account",
			Status:     model.StepStatusCompleted,
			DurationMs: 12,
			Output:     map[string]interface{}{"balance": 5000.0},
			StartedAt:  now,
		},
		{
			ID:         "step_2",
			StageID:    "debit_account",
			Status:     model.StepStatusCompleted,
			DurationMs: 25,
			Output:     map[string]interface{}{"debited": 100.0},
			StartedAt:  now,
		},
	}

	root, leaves := security.BuildMerkleTree(steps)
	if root == "" || len(leaves) != 2 {
		t.Fatalf("expected valid root and 2 leaves, got root '%s', leaves: %d", root, len(leaves))
	}

	exec := &model.Execution{
		ID:         "exec_fintech_1001",
		Steps:      steps,
		MerkleRoot: root,
	}

	// 1. Verify valid chain
	valid, verifiedRoot, err := security.VerifyChainIntegrity(exec)
	if !valid || err != nil || verifiedRoot != root {
		t.Fatalf("expected valid Merkle integrity check, got valid=%v, root=%s, err=%v", valid, verifiedRoot, err)
	}

	// 2. Simulate fraudulent data tampering (e.g. attacker changes debit amount directly in DB)
	steps[1].Output["debited"] = 0.01 // Tampered!

	validAfterTamper, _, tamperErr := security.VerifyChainIntegrity(exec)
	if validAfterTamper || tamperErr == nil {
		t.Fatalf("expected tamper detection to flag tampered execution, got valid=%v, err=%v",
			validAfterTamper, tamperErr)
	}
}

func TestRollingMerkleTree(t *testing.T) {
	now := time.Now()
	s1 := &model.StepExecution{
		ID:         "step_1",
		StageID:    "validate_account",
		Status:     model.StepStatusCompleted,
		DurationMs: 12,
		Output:     map[string]interface{}{"balance": 5000.0},
		StartedAt:  now,
	}
	s2 := &model.StepExecution{
		ID:         "step_2",
		StageID:    "debit_account",
		Status:     model.StepStatusCompleted,
		DurationMs: 25,
		Output:     map[string]interface{}{"debited": 100.0},
		StartedAt:  now,
	}

	rolling := security.NewRollingMerkleTree()
	r1 := rolling.AppendStep(s1)
	if r1 == "" || s1.MerkleHash == "" {
		t.Fatalf("expected non-empty root and hash after first step")
	}

	r2 := rolling.AppendStep(s2)
	if r2 == "" || s2.MerkleHash == "" {
		t.Fatalf("expected non-empty root and hash after second step")
	}

	// Compare with batch BuildMerkleTree
	batchSteps := []*model.StepExecution{
		{
			ID:         "step_1",
			StageID:    "validate_account",
			Status:     model.StepStatusCompleted,
			DurationMs: 12,
			Output:     map[string]interface{}{"balance": 5000.0},
			StartedAt:  now,
		},
		{
			ID:         "step_2",
			StageID:    "debit_account",
			Status:     model.StepStatusCompleted,
			DurationMs: 25,
			Output:     map[string]interface{}{"debited": 100.0},
			StartedAt:  now,
		},
	}
	batchRoot, _ := security.BuildMerkleTree(batchSteps)

	if r2 != batchRoot {
		t.Fatalf("expected rolling root '%s' to match batch root '%s'", r2, batchRoot)
	}
}

