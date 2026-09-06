package engine_test

import (
	"context"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/security"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestCoreBanking16StageWorkflowExecution(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)

	// Load 16-stage banking settlement YAML
	yamlBytes, err := os.ReadFile("testdata/banking_settlement.yaml")
	if err != nil {
		t.Fatalf("failed to read banking workflow YAML: %v", err)
	}

	var wf model.Workflow
	if err := yaml.Unmarshal(yamlBytes, &wf); err != nil {
		t.Fatalf("failed to unmarshal banking workflow YAML: %v", err)
	}

	if len(wf.Stages) < 15 {
		t.Fatalf("expected at least 15 stages in banking workflow, got %d", len(wf.Stages))
	}

	if err := store.Workflows().Create(ctx, &wf); err != nil {
		t.Fatalf("failed to create banking workflow: %v", err)
	}

	// Banking transfer input payload
	input := map[string]interface{}{
		"tx_id":               "TX-PIX-2026-88192",
		"amount":              2500.00,
		"currency":            "USD",
		"source_account":      "ACC-BR-9948-12",
		"destination_account": "ACC-US-4819-88",
		"source_tax_id":       "123.456.789-00",
		"destination_tax_id":  "98-7654321",
		"destination_country": "USA",
	}

	exec, err := eng.Execute(ctx, wf.ID, input, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to execute core banking workflow: %v", err)
	}

	// Wait for asynchronous engine worker to complete all 16 stages
	var finalExec *model.Execution
	for i := 0; i < 100; i++ {
		time.Sleep(30 * time.Millisecond)
		finalExec, err = store.Executions().Get(ctx, exec.ID)
		if err == nil && (finalExec.Status == model.ExecutionStatusCompleted || finalExec.Status == model.ExecutionStatusFailed) {
			break
		}
	}

	if finalExec == nil || finalExec.Status != model.ExecutionStatusCompleted {
		t.Fatalf("expected banking execution COMPLETED, got %v (err: %v)", finalExec.Status, finalExec.Error)
	}

	for _, s := range finalExec.Steps {
		t.Logf("Executed Step: %s (%s) Output: %v", s.StageID, s.Status, s.Output)
	}

	// Verify step count
	if len(finalExec.Steps) < 10 {
		t.Fatalf("expected multiple executed steps along the banking pipeline, got %d", len(finalExec.Steps))
	}

	// Verify Merkle Root was cryptographically generated
	if finalExec.MerkleRoot == "" {
		t.Fatalf("expected non-empty MerkleRoot on completed execution")
	}

	valid, root, verifyErr := security.VerifyChainIntegrity(finalExec)
	if !valid || verifyErr != nil || root != finalExec.MerkleRoot {
		t.Fatalf("expected valid Merkle tree audit verification on banking execution, got valid=%v, err=%v", valid, verifyErr)
	}

	t.Logf("✔ Banking Workflow Successfully Executed %d stages with Merkle Root: %s", len(exec.Steps), exec.MerkleRoot)
}
