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

func TestRecoveryWatchdogSweep(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)
	watchdog := engine.NewRecoveryWatchdog(eng, store).WithStalledThreshold(1 * time.Minute)

	wf := &model.Workflow{
		ID:      "wf_watchdog_test",
		Name:    "Watchdog Test",
		StartAt: "stage_1",
		Stages: []model.Stage{
			{
				ID:   "stage_1",
				Name: "Step 1",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"rescued": true,
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, wf)

	// 1. Truly stalled execution: started 5m ago, no heartbeat
	stalledExec := &model.Execution{
		ID:        "exec_stalled_999",
		WorkflowID: wf.ID,
		Status:    model.ExecutionStatusRunning,
		Input:     map[string]interface{}{"initial": 1},
		Variables: make(map[string]interface{}),
		StartedAt: time.Now().Add(-5 * time.Minute),
	}
	_ = store.Executions().Create(ctx, stalledExec)

	// 2. Fresh active execution: started 10s ago with fresh heartbeat -> MUST NOT BE RECOVERED
	freshHeartbeat := time.Now().Add(-5 * time.Second)
	activeExec := &model.Execution{
		ID:          "exec_active_fresh",
		WorkflowID:  wf.ID,
		Status:      model.ExecutionStatusRunning,
		Input:       map[string]interface{}{"initial": 2},
		Variables:   make(map[string]interface{}),
		StartedAt:   time.Now().Add(-10 * time.Second),
		HeartbeatAt: &freshHeartbeat,
	}
	_ = store.Executions().Create(ctx, activeExec)

	recovered, err := watchdog.RecoverInterruptedExecutions(ctx)
	if err != nil {
		t.Fatalf("unexpected recovery error: %v", err)
	}

	if recovered != 1 {
		t.Fatalf("expected exactly 1 stalled execution to be recovered, got %d", recovered)
	}

	// Verify active execution was NOT touched or marked
	activeCheck, err := store.Executions().Get(ctx, "exec_active_fresh")
	if err != nil {
		t.Fatalf("failed to fetch active execution: %v", err)
	}
	if activeCheck.WorkerID != "" {
		t.Fatalf("expected active execution to have empty WorkerID, got '%s'", activeCheck.WorkerID)
	}
}

func TestRecoveryWatchdogMultiNodeNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)
	watchdogA := engine.NewRecoveryWatchdog(eng, store).WithWorkerID("pod-replica-alpha").WithStalledThreshold(30 * time.Second)
	watchdogB := engine.NewRecoveryWatchdog(eng, store).WithWorkerID("pod-replica-beta").WithStalledThreshold(30 * time.Second)

	wf := &model.Workflow{
		ID:      "wf_multi_node_test",
		Name:    "Multi Node Test",
		StartAt: "stage_1",
		Stages: []model.Stage{
			{
				ID:   "stage_1",
				Name: "Step 1",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{"done": true},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, wf)

	stalledExec := &model.Execution{
		ID:        "exec_stalled_collision",
		WorkflowID: wf.ID,
		Status:    model.ExecutionStatusRunning,
		Input:     map[string]interface{}{"key": "val"},
		Variables: make(map[string]interface{}),
		StartedAt: time.Now().Add(-2 * time.Minute),
	}
	_ = store.Executions().Create(ctx, stalledExec)

	// Replica A claims it first
	recoveredA, errA := watchdogA.RecoverInterruptedExecutions(ctx)
	if errA != nil {
		t.Fatalf("watchdogA recovery failed: %v", errA)
	}
	if recoveredA != 1 {
		t.Fatalf("expected watchdogA to claim 1 execution, got %d", recoveredA)
	}

	// Replica B runs immediately after; should claim 0 since A updated heartbeat
	recoveredB, errB := watchdogB.RecoverInterruptedExecutions(ctx)
	if errB != nil {
		t.Fatalf("watchdogB recovery failed: %v", errB)
	}
	if recoveredB != 0 {
		t.Fatalf("expected watchdogB to claim 0 executions (no collision), got %d", recoveredB)
	}
}
