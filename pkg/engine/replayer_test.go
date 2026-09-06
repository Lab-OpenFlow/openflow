package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestDeterministicReplayRecovery(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)

	wf := &model.Workflow{
		ID:      "wf_replay_test",
		Name:    "Replay Test Workflow",
		Version: "1.0.0",
		Status:  model.WorkflowStatusActive,
		StartAt: "stage_1",
		Stages: []model.Stage{
			{
				ID:   "stage_1",
				Name: "Step 1",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"val1": "100",
					},
				},
				Next: []string{"stage_2"},
			},
			{
				ID:   "stage_2",
				Name: "Step 2",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"val2": "200",
					},
				},
				Next: []string{"stage_3"},
			},
			{
				ID:   "stage_3",
				Name: "Step 3",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"final": "'FINISHED_FROM_REPLAY'",
					},
				},
			},
		},
	}

	_ = store.Workflows().Create(ctx, wf)

	// Simulate an interrupted execution where Stage 1 and Stage 2 completed before crash
	now := time.Now()
	interruptedExec := &model.Execution{
		ID:           "exec_crashed_123",
		WorkflowID:   wf.ID,
		WorkflowName: wf.Name,
		TriggerType:  model.TriggerTypeManual,
		Status:       model.ExecutionStatusRunning,
		Input:        map[string]interface{}{"init": true},
		Variables:    make(map[string]interface{}),
		StartedAt:    now,
	}
	_ = store.Executions().Create(ctx, interruptedExec)

	step1 := &model.StepExecution{
		ID:          "step_1_id",
		StageID:     "stage_1",
		StageName:   "Step 1",
		StageType:   model.StageTypeTransform,
		Status:      model.StepStatusCompleted,
		Output:      map[string]interface{}{"val1": "100"},
		DurationMs:  15,
		CompletedAt: &now,
	}
	step2 := &model.StepExecution{
		ID:          "step_2_id",
		StageID:     "stage_2",
		StageName:   "Step 2",
		StageType:   model.StageTypeTransform,
		Status:      model.StepStatusCompleted,
		Output:      map[string]interface{}{"val2": "200"},
		DurationMs:  20,
		CompletedAt: &now,
	}

	_ = store.Executions().AppendStep(ctx, interruptedExec.ID, step1)
	_ = store.Executions().AppendStep(ctx, interruptedExec.ID, step2)

	// Fetch loaded execution containing persisted steps from store
	loadedExec, _ := store.Executions().Get(ctx, interruptedExec.ID)

	// Test ReconstructExecutionState
	replayState, err := engine.ReconstructExecutionState(wf, loadedExec)
	if err != nil {
		t.Fatalf("unexpected state reconstruction error: %v", err)
	}

	if replayState.NextStageID != "stage_3" {
		t.Fatalf("expected next stage to resume at 'stage_3', got '%s'", replayState.NextStageID)
	}
	if len(replayState.CompletedStages) != 2 {
		t.Fatalf("expected 2 completed stages replayed from history, got %d", len(replayState.CompletedStages))
	}

	// Trigger ResumeExecution
	resumed, err := eng.ResumeExecution(ctx, interruptedExec.ID)
	if err != nil {
		t.Fatalf("failed to resume execution: %v", err)
	}
	if resumed.ID != interruptedExec.ID {
		t.Fatalf("expected resumed execution ID '%s', got '%s'", interruptedExec.ID, resumed.ID)
	}

	// Wait for execution to finish stage 3
	time.Sleep(150 * time.Millisecond)

	finalExec, err := store.Executions().Get(ctx, interruptedExec.ID)
	if err != nil {
		t.Fatalf("failed to get final execution: %v", err)
	}

	if finalExec.Status != model.ExecutionStatusCompleted {
		t.Fatalf("expected execution status COMPLETED after resume, got %s (err: %s)", finalExec.Status, finalExec.Error)
	}

	// Verify that stage 3 was appended to steps
	var stage3Found bool
	for _, s := range finalExec.Steps {
		if s.StageID == "stage_3" {
			stage3Found = true
			if s.Status != model.StepStatusCompleted {
				t.Fatalf("expected stage 3 status COMPLETED, got %s", s.Status)
			}
		}
	}

	if !stage3Found {
		t.Fatalf("expected stage_3 to be executed and recorded in execution steps")
	}
}
