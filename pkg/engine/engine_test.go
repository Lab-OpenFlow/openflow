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

func setupTestEngine() (*engine.Engine, storage.Store) {
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)
	return eng, store
}

func TestWorkflowLinearExecution(t *testing.T) {
	eng, store := setupTestEngine()
	ctx := context.Background()

	wf := &model.Workflow{
		Version: "v1",
		ID:      "test-linear",
		Name:    "Linear Test Flow",
		Status:  model.WorkflowStatusActive,
		StartAt: "stage1",
		Stages: []model.Stage{
			{
				ID:   "stage1",
				Name: "Transform Step 1",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"doubled": "payload.count * 2",
					},
				},
				Next: []string{"stage2"},
			},
			{
				ID:   "stage2",
				Name: "Transform Step 2",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"quadrupled": "payload.doubled * 2",
						"status":     "'SUCCESS'",
					},
				},
			},
		},
	}

	if err := store.Workflows().Create(ctx, wf); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	input := map[string]interface{}{
		"count": 10,
	}

	exec, err := eng.Execute(ctx, wf.ID, input, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to execute workflow: %v", err)
	}

	// Wait for async execution to complete
	time.Sleep(200 * time.Millisecond)

	finalExec, err := store.Executions().Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("failed to get execution: %v", err)
	}

	if finalExec.Status != model.ExecutionStatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s (error: %s)", finalExec.Status, finalExec.Error)
	}

	if len(finalExec.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(finalExec.Steps))
	}

	if finalExec.Output["status"] != "SUCCESS" {
		t.Fatalf("expected status SUCCESS, got %v", finalExec.Output["status"])
	}
}

func TestWorkflowExclusiveXORGateway(t *testing.T) {
	eng, store := setupTestEngine()
	ctx := context.Background()

	wf := &model.Workflow{
		Version: "v1",
		ID:      "test-xor",
		Name:    "XOR Gateway Test Flow",
		Status:  model.WorkflowStatusActive,
		StartAt: "gateway",
		Stages: []model.Stage{
			{
				ID:   "gateway",
				Name: "Route by Score",
				Type: model.StageTypeExclusiveXOR,
				Branches: []model.Branch{
					{
						Condition: "payload.score >= 80",
						Target:    "high_score_step",
					},
					{
						Default: true,
						Target:  "low_score_step",
					},
				},
			},
			{
				ID:   "high_score_step",
				Name: "High Score Handler",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"result": "'APPROVED'",
					},
				},
			},
			{
				ID:   "low_score_step",
				Name: "Low Score Handler",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"result": "'REJECTED'",
					},
				},
			},
		},
	}

	_ = store.Workflows().Create(ctx, wf)

	// Test Branch 1: score >= 80 -> APPROVED
	exec1, _ := eng.Execute(ctx, wf.ID, map[string]interface{}{"score": 95}, model.TriggerTypeManual)
	for i := 0; i < 20; i++ { time.Sleep(30 * time.Millisecond) }
	res1, _ := store.Executions().Get(ctx, exec1.ID)
	if res1.Output["result"] != "APPROVED" {
		t.Fatalf("expected result APPROVED for score 95, got %v", res1.Output["result"])
	}

	// Test Branch 2: score < 80 -> REJECTED
	exec2, _ := eng.Execute(ctx, wf.ID, map[string]interface{}{"score": 40}, model.TriggerTypeManual)
	for i := 0; i < 20; i++ { time.Sleep(30 * time.Millisecond) }
	res2, _ := store.Executions().Get(ctx, exec2.ID)
	if res2.Output["result"] != "REJECTED" {
		t.Fatalf("expected result REJECTED for score 40, got %v", res2.Output["result"])
	}
}

func TestWorkflowSagaRollback(t *testing.T) {
	eng, store := setupTestEngine()
	ctx := context.Background()

	wf := &model.Workflow{
		Version: "v1",
		ID:      "test-saga",
		Name:    "Saga Rollback Flow",
		Status:  model.WorkflowStatusActive,
		StartAt: "stage_reserve",
		Stages: []model.Stage{
			{
				ID:   "stage_reserve",
				Name: "Reserve Funds",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"reserved": true,
						"amount":   "payload.amount",
					},
				},
				Compensation: &model.CompensationConfig{
					ID:   "stage_refund",
					Name: "Refund Funds Compensation",
					Type: model.StageTypeTransform,
					Config: map[string]interface{}{
						"mapping": map[string]interface{}{
							"refunded": true,
						},
					},
				},
				Next: []string{"stage_fail"},
			},
			{
				ID:   "stage_fail",
				Name: "Step That Fails",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					// Deliberately trigger syntax error in expression
					"expression": "invalid_syntax_variable_that_does_not_exist()",
				},
			},
		},
	}

	_ = store.Workflows().Create(ctx, wf)

	exec, err := eng.Execute(ctx, wf.ID, map[string]interface{}{"amount": 500}, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to execute workflow: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	finalExec, _ := store.Executions().Get(ctx, exec.ID)
	if finalExec.Status != model.ExecutionStatusCompensated {
		t.Fatalf("expected status COMPENSATED after saga rollback, got %s", finalExec.Status)
	}

	// Verify that compensation step was executed
	var compFound bool
	for _, step := range finalExec.Steps {
		if step.IsCompensation && step.CompensatesFor == "stage_reserve" {
			compFound = true
			if step.Status != model.StepStatusCompensated {
				t.Fatalf("expected compensation step status COMPENSATED, got %s", step.Status)
			}
		}
	}

	if !compFound {
		t.Fatalf("expected compensation step for stage_reserve to be recorded in execution steps")
	}
}

func TestWorkflowIdempotency(t *testing.T) {
	eng, store := setupTestEngine()
	ctx := context.Background()

	wf := &model.Workflow{
		ID:      "wf_idempotent",
		Name:    "Idempotent Workflow",
		Version: "1.0.0",
		Status:  model.WorkflowStatusActive,
		StartAt: "stage_transform",
		Stages: []model.Stage{
			{
				ID:   "stage_transform",
				Name: "Echo Transform",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"echo": "payload.msg",
					},
				},
			},
		},
	}

	_ = store.Workflows().Create(ctx, wf)

	input := map[string]interface{}{
		"msg":                "hello world",
		"__idempotency_key": "unique_key_12345",
	}

	// First execution
	exec1, err := eng.Execute(ctx, wf.ID, input, model.TriggerTypeKafka)
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	// Second execution with SAME idempotency key
	exec2, err := eng.Execute(ctx, wf.ID, input, model.TriggerTypeKafka)
	if err != nil {
		t.Fatalf("second execution failed: %v", err)
	}

	// Should return identical execution instance
	if exec1.ID != exec2.ID {
		t.Fatalf("expected same execution ID for idempotent requests, got %s vs %s", exec1.ID, exec2.ID)
	}
}

func TestEvaluatorCompilationCache(t *testing.T) {
	eval := engine.NewEvaluator()

	env := map[string]interface{}{
		"amount": 250,
		"status": "APPROVED",
	}

	// 1. Condition evaluation cache
	for i := 0; i < 10; i++ {
		res, err := eval.EvalCondition("amount > 100 && status == 'APPROVED'", env)
		if err != nil {
			t.Fatalf("unexpected eval error: %v", err)
		}
		if !res {
			t.Fatalf("expected condition to be true")
		}
	}

	// 2. Template interpolation cache
	for i := 0; i < 10; i++ {
		interpolated, err := eval.InterpolateString("Amount is {{.amount}} and status is {{.status}}", env)
		if err != nil {
			t.Fatalf("unexpected interpolation error: %v", err)
		}
		if interpolated != "Amount is 250 and status is APPROVED" {
			t.Fatalf("unexpected interpolated text: %s", interpolated)
		}
	}
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	cfg := engine.CircuitBreakerConfig{
		FailureThreshold:    3,
		CooldownDuration:    100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}

	cb := engine.NewCircuitBreaker("test-breaker", cfg)

	if cb.State() != engine.StateClosed {
		t.Fatalf("expected initial state CLOSED, got %s", cb.State())
	}

	// Record failures below threshold
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != engine.StateClosed {
		t.Fatalf("expected state CLOSED after 2 failures, got %s", cb.State())
	}

	// 3rd failure trips the breaker
	cb.RecordFailure()
	if cb.State() != engine.StateOpen {
		t.Fatalf("expected state OPEN after 3 failures, got %s", cb.State())
	}

	// Allow() should fail fast while open
	if err := cb.Allow(); err == nil {
		t.Fatalf("expected Allow() to return error when circuit is open")
	}

	// Wait for cooldown to elapse
	time.Sleep(120 * time.Millisecond)

	// Next Allow() transitions to HALF_OPEN
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected Allow() to succeed in half-open probe, got: %v", err)
	}
	if cb.State() != engine.StateHalfOpen {
		t.Fatalf("expected state HALF_OPEN, got %s", cb.State())
	}

	// Success in half-open resets to CLOSED
	cb.RecordSuccess()
	if cb.State() != engine.StateClosed {
		t.Fatalf("expected state CLOSED after recovery, got %s", cb.State())
	}
}

func TestWorkflowCycleDetection(t *testing.T) {
	eng, _ := setupTestEngine()

	// Workflow with a direct cycle: stageA -> stageB -> stageA
	wfWithCycle := &model.Workflow{
		ID:      "wf_cycle",
		Name:    "Cyclic Workflow",
		Version: "1.0.0",
		StartAt: "stageA",
		Stages: []model.Stage{
			{
				ID:   "stageA",
				Name: "Stage A",
				Type: model.StageTypeTransform,
				Next: []string{"stageB"},
			},
			{
				ID:   "stageB",
				Name: "Stage B",
				Type: model.StageTypeTransform,
				Next: []string{"stageA"}, // creates cycle
			},
		},
	}

	err := eng.ValidateWorkflow(wfWithCycle)
	if err == nil {
		t.Fatalf("expected ValidateWorkflow to fail with cycle detection error, got nil")
	}

	// Valid DAG without cycles
	validDAG := &model.Workflow{
		ID:      "wf_dag",
		Name:    "Valid DAG Workflow",
		Version: "1.0.0",
		StartAt: "stageA",
		Stages: []model.Stage{
			{
				ID:   "stageA",
				Name: "Stage A",
				Type: model.StageTypeTransform,
				Next: []string{"stageB"},
			},
			{
				ID:   "stageB",
				Name: "Stage B",
				Type: model.StageTypeTransform,
			},
		},
	}

	if err := eng.ValidateWorkflow(validDAG); err != nil {
		t.Fatalf("expected valid DAG to pass validation, got: %v", err)
	}
}

func TestDirectTreeInterpolation(t *testing.T) {
	eval := engine.NewEvaluator()

	env := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "Alice",
			"id":   12345,
		},
		"token": "secret_jwt_value_with_quotes_\"test\"",
	}

	input := map[string]interface{}{
		"auth_header": "Bearer {{.token}}",
		"nested": map[string]interface{}{
			"welcome": "Hello {{.user.name}}",
			"user_id": 999,
		},
		"tags": []interface{}{
			"user:{{.user.name}}",
			"static_tag",
		},
	}

	res, err := eval.InterpolateMap(input, env)
	if err != nil {
		t.Fatalf("unexpected interpolation error: %v", err)
	}

	if res["auth_header"] != "Bearer secret_jwt_value_with_quotes_\"test\"" {
		t.Fatalf("expected auth_header to preserve quotes and interpolate properly, got: %v", res["auth_header"])
	}

	nested := res["nested"].(map[string]interface{})
	if nested["welcome"] != "Hello Alice" {
		t.Fatalf("expected nested welcome to be 'Hello Alice', got: %v", nested["welcome"])
	}
	if nested["user_id"] != 999 {
		t.Fatalf("expected non-string primitive to be preserved, got: %v", nested["user_id"])
	}

	tags := res["tags"].([]interface{})
	if tags[0] != "user:Alice" || tags[1] != "static_tag" {
		t.Fatalf("expected slice items to interpolate properly, got: %v", tags)
	}
}

func TestFullJitterBackoff(t *testing.T) {
	policy := &model.RetryPolicy{
		MaxAttempts:     5,
		Backoff:         "exponential",
		InitialInterval: "100ms",
		MaxInterval:     "2s",
		Multiplier:      2.0,
	}

	// Calculate backoffs across attempts to verify non-zero, bounded sleep
	for attempt := 2; attempt <= 5; attempt++ {
		backoff := engine.CalculateBackoff(policy, attempt)
		if backoff <= 0 {
			t.Fatalf("expected positive backoff duration for attempt %d, got %v", attempt, backoff)
		}
		if backoff > 2*time.Second {
			t.Fatalf("expected backoff to not exceed MaxInterval 2s, got %v", backoff)
		}
	}
}

func TestFastPathPropertyResolver(t *testing.T) {
	eval := engine.NewEvaluator()

	env := map[string]interface{}{
		"payload": map[string]interface{}{
			"device_id": "sensor-saopaulo-04",
			"telemetry": map[string]interface{}{
				"temperature": 94.2,
				"active":      true,
			},
		},
		"variables": map[string]interface{}{
			"api_token": "bearer-xyz-123",
		},
	}

	// 1. Direct fast-path property access
	devID, err := eval.EvalExpression("payload.device_id", env)
	if err != nil || devID != "sensor-saopaulo-04" {
		t.Fatalf("expected 'sensor-saopaulo-04', got %v (err: %v)", devID, err)
	}

	temp, err := eval.EvalExpression("payload.telemetry.temperature", env)
	if err != nil || temp != 94.2 {
		t.Fatalf("expected 94.2, got %v (err: %v)", temp, err)
	}

	token, err := eval.EvalExpression("variables.api_token", env)
	if err != nil || token != "bearer-xyz-123" {
		t.Fatalf("expected 'bearer-xyz-123', got %v (err: %v)", token, err)
	}

	// 2. Computed expression fallback (non-property path)
	calc, err := eval.EvalExpression("payload.telemetry.temperature + 5.8", env)
	if err != nil || calc != 100.0 {
		t.Fatalf("expected computed 100.0, got %v (err: %v)", calc, err)
	}
}

func TestBloomFilter(t *testing.T) {
	bf := engine.NewBloomFilter(65536, 5)

	keys := []string{"key_1", "key_2", "order_abc", "tx_9999"}
	for _, k := range keys {
		if bf.Contains(k) {
			t.Fatalf("expected empty Bloom filter to not contain '%s'", k)
		}
		bf.Add(k)
		if !bf.Contains(k) {
			t.Fatalf("expected Bloom filter to contain '%s' after insertion (zero false negatives)", k)
		}
	}

	// Unseen key must report false with high confidence
	if bf.Contains("definitely_never_inserted_xyz_987654321") {
		t.Fatalf("unexpected false positive on distinct random key")
	}

	bf.Reset()
	for _, k := range keys {
		if bf.Contains(k) {
			t.Fatalf("expected reset filter to not contain '%s'", k)
		}
	}
}

func TestTaskScheduler(t *testing.T) {
	s := engine.NewTaskScheduler()
	defer s.Close()

	var executed bool
	task := s.Schedule("task_1", 50*time.Millisecond, func() {
		executed = true
	})

	if task.ID != "task_1" {
		t.Fatalf("expected task ID 'task_1', got '%s'", task.ID)
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)
	if !executed {
		t.Fatalf("expected scheduled task to execute within deadline")
	}

	// Test cancellation
	var cancelledTaskRan bool
	s.Schedule("task_cancel", 100*time.Millisecond, func() {
		cancelledTaskRan = true
	})
	s.Cancel("task_cancel")

	for i := 0; i < 20; i++ { time.Sleep(30 * time.Millisecond) }
	if cancelledTaskRan {
		t.Fatalf("expected cancelled task to not execute")
	}
}

func TestKahnWavePartitioning(t *testing.T) {
	// Diamond DAG:
	//       stageA (root)
	//       /    \
	//   stageB  stageC (wave 1)
	//       \    /
	//       stageD (wave 2)
	wf := &model.Workflow{
		ID:      "diamond_dag",
		Name:    "Diamond Workflow",
		StartAt: "stageA",
		Stages: []model.Stage{
			{ID: "stageA", Next: []string{"stageB", "stageC"}},
			{ID: "stageB", Next: []string{"stageD"}},
			{ID: "stageC", Next: []string{"stageD"}},
			{ID: "stageD"},
		},
	}

	waves, err := engine.PartitionWorkflowWaves(wf)
	if err != nil {
		t.Fatalf("unexpected partitioning error: %v", err)
	}

	if len(waves) != 3 {
		t.Fatalf("expected 3 execution waves, got %d", len(waves))
	}

	// Wave 0: stageA
	if len(waves[0]) != 1 || waves[0][0] != "stageA" {
		t.Fatalf("expected Wave 0 to be [stageA], got %v", waves[0])
	}

	// Wave 1: stageB and stageC in parallel
	if len(waves[1]) != 2 {
		t.Fatalf("expected Wave 1 to have 2 parallel stages, got %v", waves[1])
	}

	// Wave 2: stageD (join)
	if len(waves[2]) != 1 || waves[2][0] != "stageD" {
		t.Fatalf("expected Wave 2 to be [stageD], got %v", waves[2])
	}
}

func TestRotatingBloomFilter(t *testing.T) {
	rbf := engine.NewRotatingBloomFilter(65536, 5, 1*time.Hour)
	defer rbf.Stop()

	// 1. Generation 1 keys
	rbf.Add("key_gen1_a")
	rbf.Add("key_gen1_b")

	if !rbf.Contains("key_gen1_a") || !rbf.Contains("key_gen1_b") {
		t.Fatalf("expected gen1 keys to be present in active filter")
	}
	if rbf.Contains("unknown_key_xyz") {
		t.Fatalf("unexpected positive on unknown key")
	}

	// 2. Rotate to Generation 2
	rbf.Rotate()

	// Gen 1 keys MUST still be recognized from previous generation (temporal window)
	if !rbf.Contains("key_gen1_a") || !rbf.Contains("key_gen1_b") {
		t.Fatalf("expected gen1 keys to still be present after 1 rotation (in previous generation)")
	}

	// Add Gen 2 keys to new active filter
	rbf.Add("key_gen2_x")
	if !rbf.Contains("key_gen2_x") {
		t.Fatalf("expected gen2 key to be present")
	}

	// 3. Rotate to Generation 3 (evicts Gen 1, retains Gen 2)
	rbf.Rotate()

	if !rbf.Contains("key_gen2_x") {
		t.Fatalf("expected gen2 key to be present after second rotation")
	}
	// Gen 1 keys should now be evicted from memory to prevent saturation
	if rbf.Contains("key_gen1_a") {
		t.Fatalf("expected gen1 key to be evicted after 2 full rotations")
	}
}

func TestDAGWaveExecution(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)
	defer eng.Close()

	// Diamond DAG with real data transformation across parallel waves:
	//            stageA (init)
	//            /          \
	//   stageB (tax_calc)  stageC (discount_calc)  [WAVE 1 - PARALLEL]
	//            \          /
	//            stageD (finalize_order)           [WAVE 2 - JOIN]
	wf := &model.Workflow{
		ID:      "diamond_order_pipeline",
		Name:    "Diamond Order Pipeline",
		StartAt: "stageA",
		Stages: []model.Stage{
			{
				ID:   "stageA",
				Name: "Initialize Order",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"base_price": 100.0,
					},
				},
				Next: []string{"stageB", "stageC"},
			},
			{
				ID:   "stageB",
				Name: "Calculate Tax",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"tax_amount": 15.0,
					},
				},
				Next: []string{"stageD"},
			},
			{
				ID:   "stageC",
				Name: "Calculate Discount",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"discount_amount": 10.0,
					},
				},
				Next: []string{"stageD"},
			},
			{
				ID:   "stageD",
				Name: "Finalize Order",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"order_confirmed": true,
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, wf)

	input := map[string]interface{}{
		"order_id": "ORD-2026-WAVE-99",
	}

	exec, err := eng.Execute(ctx, wf.ID, input, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to trigger DAG wave execution: %v", err)
	}

	// Poll until completed
	deadline := time.Now().Add(5 * time.Second)
	var finalExec *model.Execution
	for time.Now().Before(deadline) {
		e, err := store.Executions().Get(ctx, exec.ID)
		if err == nil && (e.Status == model.ExecutionStatusCompleted || e.Status == model.ExecutionStatusFailed) {
			finalExec = e
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalExec == nil {
		t.Fatalf("execution timed out")
	}
	if finalExec.Status != model.ExecutionStatusCompleted {
		t.Fatalf("expected execution status COMPLETED, got %s (error: %s)", finalExec.Status, finalExec.Error)
	}

	// Validate all 4 stages executed
	if len(finalExec.Steps) != 4 {
		t.Fatalf("expected 4 steps executed in wave DAG, got %d", len(finalExec.Steps))
	}

	// Validate output has aggregated contributions from all waves
	output := finalExec.Output
	if output["order_confirmed"] != true {
		t.Fatalf("expected order_confirmed to be true")
	}
	if output["tax_amount"] != 15.0 {
		t.Fatalf("expected tax_amount 15.0 from stageB, got %v", output["tax_amount"])
	}
	if output["discount_amount"] != 10.0 {
		t.Fatalf("expected discount_amount 10.0 from stageC, got %v", output["discount_amount"])
	}
	if finalExec.MerkleRoot == "" {
		t.Fatalf("expected valid MerkleRoot on completed wave DAG execution")
	}
}




