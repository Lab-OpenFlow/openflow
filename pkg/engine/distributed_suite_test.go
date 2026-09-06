package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestDistributedSuite(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)

	// 1. Setup workflow with a worker_task stage
	wf := &model.Workflow{
		ID:      "wf_worker_pipeline",
		Name:    "External Worker Pipeline",
		Version: "v1",
		Status:  model.WorkflowStatusActive,
		StartAt: "stage_worker",
		Stages: []model.Stage{
			{
				ID:   "stage_worker",
				Name: "AI Fraud Model Worker",
				Type: model.StageTypeWorkerTask,
				Config: map[string]interface{}{
					"queue_name":        "ai-fraud-queue",
					"heartbeat_timeout": "500ms",
				},
				Next: []string{"stage_finish"},
			},
			{
				ID:   "stage_finish",
				Name: "Format Final Result",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"decision": "payload.model_decision",
						"score":    "payload.fraud_score",
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, wf)

	// 2. Trigger workflow execution -> Should suspend at stage_worker in WAITING_WORKER status
	exec, err := eng.Execute(ctx, wf.ID, map[string]interface{}{"amount": 50000}, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Inquire state
	liveExec, err := store.Executions().Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("Get execution failed: %v", err)
	}
	if liveExec.Status != model.ExecutionStatusWaitingWorker {
		t.Fatalf("expected status WAITING_WORKER, got: %s", liveExec.Status)
	}

	// 3. Test Real-time In-Flight Query
	queryRes, err := eng.Query(ctx, exec.ID, "input.amount")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if fmt.Sprintf("%v", queryRes) != "50000" {
		t.Fatalf("expected queried amount 50000, got: %v", queryRes)
	}

	// 4. Test Task Queue Polling by External Worker
	task, err := eng.TaskQueues().Poll(ctx, "ai-fraud-queue", "worker_node_1", 2*time.Second)
	if err != nil {
		t.Fatalf("Poll task failed: %v", err)
	}
	if task == nil || task.LockToken == "" {
		t.Fatalf("expected valid task with lock token, got: %+v", task)
	}

	// 5. Test Heartbeat
	err = eng.TaskQueues().Heartbeat(ctx, task.ID, "worker_node_1", task.LockToken)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// 6. Test Worker Completion -> Should resume and complete workflow
	output := map[string]interface{}{
		"model_decision": "APPROVED",
		"fraud_score":    0.04,
	}
	err = eng.TaskQueues().Complete(ctx, task.ID, "worker_node_1", task.LockToken, output)
	if err != nil {
		t.Fatalf("Complete task failed: %v", err)
	}

	// Wait for pipeline to finish
	time.Sleep(100 * time.Millisecond)

	completedExec, err := store.Executions().Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("Get completed execution failed: %v", err)
	}
	if completedExec.Status != model.ExecutionStatusCompleted {
		t.Fatalf("expected COMPLETED status, got: %s", completedExec.Status)
	}
	if completedExec.Output["decision"] != "APPROVED" {
		t.Fatalf("expected decision APPROVED, got: %v", completedExec.Output["decision"])
	}
}

func TestWorkerLeaseExpirationAndReclaim(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	eng := engine.NewEngine(store, registry)

	// Create task directly in queue
	task := &model.TaskItem{
		ID:               "task_dead_worker_test",
		QueueName:        "ocr-queue",
		WorkflowID:       "wf_ocr",
		ExecutionID:      "exec_ocr_1",
		StageID:          "ocr_stage",
		Input:            map[string]interface{}{"file": "invoice.pdf"},
		HeartbeatTimeout: 100 * time.Millisecond,
		MaxAttempts:      3,
	}
	_ = eng.TaskQueues().Enqueue(ctx, task)

	// Worker 1 acquires task
	polledTask1, err := eng.TaskQueues().Poll(ctx, "ocr-queue", "worker_1", 1*time.Second)
	if err != nil || polledTask1 == nil {
		t.Fatalf("Worker 1 failed to acquire: %v", err)
	}
	oldToken := polledTask1.LockToken

	// Worker 1 dies (sleep longer than heartbeat timeout so lease expires)
	time.Sleep(250 * time.Millisecond)

	// Reclaim expired leases
	reclaimed, err := store.TaskQueues().ReclaimExpiredLeases(ctx)
	if err != nil || reclaimed == 0 {
		t.Fatalf("expected at least 1 reclaimed lease, got %d, err: %v", reclaimed, err)
	}

	// Worker 2 acquires the reclaimed task
	polledTask2, err := eng.TaskQueues().Poll(ctx, "ocr-queue", "worker_2", 1*time.Second)
	if err != nil || polledTask2 == nil {
		t.Fatalf("Worker 2 failed to acquire: %v", err)
	}
	newToken := polledTask2.LockToken

	if oldToken == newToken {
		t.Fatal("expected new lock token for worker 2")
	}

	// Worker 1 tries to complete with stale/expired token -> Must be rejected!
	err = eng.TaskQueues().Complete(ctx, polledTask1.ID, "worker_1", oldToken, map[string]interface{}{"result": "stale"})
	if err == nil {
		t.Fatal("expected stale worker 1 completion to be rejected with ErrInvalidLockToken")
	}

	// Worker 2 completes with valid token -> Must succeed!
	err = eng.TaskQueues().Complete(ctx, polledTask2.ID, "worker_2", newToken, map[string]interface{}{"result": "fresh_ocr_done"})
	if err != nil {
		t.Fatalf("Worker 2 completion failed: %v", err)
	}
}

func TestCronSchedulerRegistration(t *testing.T) {
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	eng := engine.NewEngine(store, registry)

	// Register cron schedule
	err := eng.CronScheduler().RegisterSchedule("wf_daily_settlement", "0 2 * * *", model.OverlapPolicySkip)
	if err != nil {
		t.Fatalf("RegisterSchedule failed: %v", err)
	}

	// Invalid cron expression should error
	err = eng.CronScheduler().RegisterSchedule("wf_broken", "invalid_cron_syntax", model.OverlapPolicyAllowAll)
	if err == nil {
		t.Fatal("expected error for invalid cron syntax")
	}
}
