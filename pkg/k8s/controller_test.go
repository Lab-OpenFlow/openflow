package k8s_test

import (
	"context"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/k8s"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestK8sWorkflowReconciler(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())

	eng := engine.NewEngine(store, registry)
	controller := k8s.NewWorkflowController(eng, store)

	// 1. Reconcile Workflow CRD
	workflowCRD := &k8s.WorkflowCRD{
		APIVersion: "openflow.dev/v1alpha1",
		Kind:       "Workflow",
		Metadata: k8s.ObjectMetadata{
			Name: "k8s-order-flow",
		},
		Spec: k8s.WorkflowSpec{
			Name:    "K8s Order Processing",
			Version: "1.0.0",
			StartAt: "stage_transform",
			Stages: []model.Stage{
				{
					ID:   "stage_transform",
					Name: "Transform Order",
					Type: model.StageTypeTransform,
					Config: map[string]interface{}{
						"mapping": map[string]interface{}{
							"order_status": "'CONFIRMED'",
						},
					},
				},
			},
		},
	}

	wfStatus, err := controller.ReconcileWorkflow(ctx, workflowCRD)
	if err != nil {
		t.Fatalf("unexpected error reconciling workflow crd: %v", err)
	}

	if wfStatus.Phase != "Active" {
		t.Fatalf("expected workflow status Active, got %s", wfStatus.Phase)
	}

	// Verify workflow exists in store
	wf, err := store.Workflows().Get(ctx, "k8s-order-flow")
	if err != nil || wf.ID != "k8s-order-flow" {
		t.Fatalf("expected workflow to be stored in openflow store, got err: %v", err)
	}

	// 2. Reconcile WorkflowRun CRD
	runCRD := &k8s.WorkflowRunCRD{
		APIVersion: "openflow.dev/v1alpha1",
		Kind:       "WorkflowRun",
		Metadata: k8s.ObjectMetadata{
			Name: "order-run-001",
		},
		Spec: k8s.WorkflowRunSpec{
			WorkflowRef: "k8s-order-flow",
			Input: map[string]interface{}{
				"order_id": "ORD-12345",
			},
		},
	}

	runStatus, err := controller.ReconcileWorkflowRun(ctx, runCRD)
	if err != nil {
		t.Fatalf("unexpected error reconciling workflow run crd: %v", err)
	}

	if runStatus.ExecutionID == "" {
		t.Fatalf("expected non-empty execution ID on triggered run")
	}

	// Wait for execution to complete
	time.Sleep(100 * time.Millisecond)

	// Update status with ongoing execution tracking
	runCRD.Status = runStatus
	finalStatus, err := controller.ReconcileWorkflowRun(ctx, runCRD)
	if err != nil {
		t.Fatalf("unexpected error reconciling running workflow run: %v", err)
	}

	if finalStatus.Phase != "RUNNING" && finalStatus.Phase != "COMPLETED" {
		t.Fatalf("expected run phase to be RUNNING or COMPLETED, got %s", finalStatus.Phase)
	}
}
