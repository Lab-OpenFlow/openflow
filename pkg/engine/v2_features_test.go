package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestDurableTimers(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())
	eng := NewEngine(store, registry)

	timerSched := NewDurableTimerScheduler(eng, store)
	timerSched.Start()
	defer timerSched.Stop()

	wf := &model.Workflow{
		ID:      "wf_timer_test",
		Name:    "Durable Timer Test Workflow",
		StartAt: "delay_stage",
		Stages: []model.Stage{
			{
				ID:   "delay_stage",
				Name: "Persisted Delay",
				Type: model.StageTypeDelay,
				Config: map[string]interface{}{
					"duration": "100ms",
				},
				Next: []string{"final_stage"},
			},
			{
				ID:   "final_stage",
				Name: "Final Stage",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"status": "'DELAY_PASSED'",
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, wf)

	exec, err := eng.Execute(ctx, wf.ID, map[string]interface{}{"amount": 100}, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to start execution: %v", err)
	}

	// Execution should enter WAITING_TIMER
	time.Sleep(50 * time.Millisecond)
	execCheck, _ := store.Executions().Get(ctx, exec.ID)
	if execCheck.Status != model.ExecutionStatusWaitingTimer {
		t.Fatalf("expected status WAITING_TIMER, got %v", execCheck.Status)
	}

	// Wait for scheduler tick (1s + margin)
	time.Sleep(1500 * time.Millisecond)

	finalExec, _ := store.Executions().Get(ctx, exec.ID)
	if finalExec.Status != model.ExecutionStatusCompleted {
		for _, s := range finalExec.Steps {
			t.Logf("Step %s (%s) Error: %s", s.StageID, s.Status, s.Error)
		}
		t.Fatalf("expected execution completed after durable timer fired, got %v (error: %s)", finalExec.Status, finalExec.Error)
	}

	if finalExec.Output["status"] != "DELAY_PASSED" {
		t.Fatalf("expected output status DELAY_PASSED, got %v", finalExec.Output["status"])
	}
}

func TestSignalsAPI(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())
	eng := NewEngine(store, registry)

	wf := &model.Workflow{
		ID:      "wf_signal_test",
		Name:    "Signal Test Workflow",
		StartAt: "await_otp",
		Stages: []model.Stage{
			{
				ID:   "await_otp",
				Name: "Await Customer OTP Confirmation",
				Type: model.StageTypeWaitForSignal,
				Config: map[string]interface{}{
					"signal_name": "otp_confirmed",
					"timeout":     "5s",
				},
				Next: []string{"finalize_order"},
			},
			{
				ID:   "finalize_order",
				Name: "Finalize Order",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"otp_code": "payload.otp",
						"verified": true,
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, wf)

	exec, err := eng.Execute(ctx, wf.ID, map[string]interface{}{"order_id": "ORD-998"}, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	execCheck, _ := store.Executions().Get(ctx, exec.ID)
	if execCheck.Status != model.ExecutionStatusWaitingSignal {
		t.Fatalf("expected status WAITING_SIGNAL, got %v", execCheck.Status)
	}

	// Verify Signal Subscription exists
	sig, err := store.Signals().FindWaiting(ctx, exec.ID, "otp_confirmed")
	if err != nil || sig == nil {
		t.Fatalf("expected waiting signal subscription for 'otp_confirmed', got err: %v", err)
	}

	// Inject external signal
	err = eng.ResumeSignal(ctx, exec.ID, "otp_confirmed", map[string]interface{}{
		"otp": "994821",
	})
	if err != nil {
		t.Fatalf("failed to resume signal: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	finalExec, _ := store.Executions().Get(ctx, exec.ID)
	if finalExec.Status != model.ExecutionStatusCompleted {
		for _, s := range finalExec.Steps {
			t.Logf("Step %s (%s) Error: %s", s.StageID, s.Status, s.Error)
		}
		t.Fatalf("expected execution completed after signal resumed, got %v (error: %s)", finalExec.Status, finalExec.Error)
	}

	if finalExec.Output["otp_code"] != "994821" || finalExec.Output["verified"] != true {
		t.Fatalf("expected output with injected signal data, got %v", finalExec.Output)
	}
}

func TestChildWorkflows(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())
	eng := NewEngine(store, registry)

	// Child Workflow
	childWf := &model.Workflow{
		ID:      "kyc_child_wf",
		Name:    "KYC Verification Sub-Workflow",
		StartAt: "verify_doc",
		Stages: []model.Stage{
			{
				ID:   "verify_doc",
				Name: "Verify Tax ID",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"kyc_score": 98,
						"approved":  true,
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, childWf)

	// Parent Workflow
	parentWf := &model.Workflow{
		ID:      "onboarding_parent_wf",
		Name:    "Customer Onboarding Parent Workflow",
		StartAt: "invoke_kyc",
		Stages: []model.Stage{
			{
				ID:   "invoke_kyc",
				Name: "Invoke KYC Verification Child Workflow",
				Type: model.StageTypeChildWorkflow,
				Config: map[string]interface{}{
					"workflow_id": "kyc_child_wf",
					"input": map[string]interface{}{
						"tax_id": "payload.tax_id",
					},
				},
				Next: []string{"complete_onboarding"},
			},
			{
				ID:   "complete_onboarding",
				Name: "Complete Onboarding",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"status":    "'ONBOARDED'",
						"kyc_score": "payload.kyc_score",
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(ctx, parentWf)

	parentExec, err := eng.Execute(ctx, parentWf.ID, map[string]interface{}{"tax_id": "123.456.789-00"}, model.TriggerTypeManual)
	if err != nil {
		t.Fatalf("failed to start parent workflow: %v", err)
	}

	// Allow parent to spawn child and child to complete
	time.Sleep(500 * time.Millisecond)

	finalParent, _ := store.Executions().Get(ctx, parentExec.ID)
	if finalParent.Status != model.ExecutionStatusCompleted {
		for _, s := range finalParent.Steps {
			t.Logf("Step %s (%s) Error: %s", s.StageID, s.Status, s.Error)
		}
		t.Fatalf("expected parent execution completed after child finished, got %v (error: %s)", finalParent.Status, finalParent.Error)
	}

	if finalParent.Output["status"] != "ONBOARDED" || finalParent.Output["kyc_score"] != float64(98) && finalParent.Output["kyc_score"] != int(98) {
		t.Fatalf("expected parent output to contain child KYC score, got %v", finalParent.Output)
	}

	// Verify child execution linkage
	children, err := store.Executions().ListChildren(ctx, parentExec.ID)
	if err != nil || len(children) != 1 {
		t.Fatalf("expected 1 linked child execution, got %d (err: %v)", len(children), err)
	}
	if children[0].ParentExecutionID != parentExec.ID {
		t.Fatalf("expected child parent execution ID match")
	}
}
