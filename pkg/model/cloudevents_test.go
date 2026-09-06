package model_test

import (
	"testing"
	"time"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

func TestCloudEventsConversion(t *testing.T) {
	wfEvent := model.WorkflowEvent{
		Type:        model.EventStepCompleted,
		ExecutionID: "exec_123",
		WorkflowID:  "wf_payment",
		StageID:     "authorize",
		Timestamp:   time.Now(),
		Payload: map[string]interface{}{
			"amount": 450.0,
			"status": "APPROVED",
		},
	}

	ce := wfEvent.ToCloudEvent("https://orchestrator.openflow.io")

	if ce.SpecVersion != "1.0" {
		t.Fatalf("expected CloudEvents specversion '1.0', got '%s'", ce.SpecVersion)
	}
	if ce.Type != "io.openflow.step.completed" {
		t.Fatalf("expected type 'io.openflow.step.completed', got '%s'", ce.Type)
	}
	if ce.Subject != "executions/exec_123" {
		t.Fatalf("expected subject 'executions/exec_123', got '%s'", ce.Subject)
	}
	if ce.Data["amount"] != 450.0 {
		t.Fatalf("expected amount 450.0 in CloudEvent data, got %v", ce.Data["amount"])
	}

	jsonBytes, err := ce.ToJSON()
	if err != nil || len(jsonBytes) == 0 {
		t.Fatalf("failed to marshal CloudEvent to JSON: %v", err)
	}
}
