package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// WorkflowController reconciles Kubernetes Workflow and WorkflowRun CRDs with the OpenFlow engine.
type WorkflowController struct {
	engine *engine.Engine
	store  storage.Store
}

// NewWorkflowController creates a new Kubernetes Operator controller instance.
func NewWorkflowController(engine *engine.Engine, store storage.Store) *WorkflowController {
	return &WorkflowController{
		engine: engine,
		store:  store,
	}
}

// ReconcileWorkflow synchronizes a Workflow CRD into OpenFlow's workflow storage.
func (c *WorkflowController) ReconcileWorkflow(ctx context.Context, crd *WorkflowCRD) (*WorkflowStatus, error) {
	if crd == nil || crd.Metadata.Name == "" {
		return nil, fmt.Errorf("invalid workflow crd: missing metadata.name")
	}

	wf := crd.ToModel()

	// Check if already exists to decide Create vs Update
	_, err := c.store.Workflows().Get(ctx, wf.ID)
	if err != nil {
		if createErr := c.store.Workflows().Create(ctx, wf); createErr != nil {
			return nil, fmt.Errorf("failed to create workflow from crd: %w", createErr)
		}
		slog.InfoContext(ctx, "created workflow from crd", slog.String("workflow_id", wf.ID))
	} else {
		if updateErr := c.store.Workflows().Update(ctx, wf); updateErr != nil {
			return nil, fmt.Errorf("failed to update workflow from crd: %w", updateErr)
		}
		slog.InfoContext(ctx, "updated workflow from crd", slog.String("workflow_id", wf.ID))
	}

	return &WorkflowStatus{
		Phase:        "Active",
		LastSyncedAt: time.Now(),
	}, nil
}

// ReconcileWorkflowRun executes a workflow run requested through a Kubernetes WorkflowRun CRD.
func (c *WorkflowController) ReconcileWorkflowRun(ctx context.Context, crd *WorkflowRunCRD) (*WorkflowRunStatus, error) {
	if crd == nil {
		return nil, fmt.Errorf("workflow run crd cannot be nil")
	}
	if crd.Spec.WorkflowRef == "" {
		return nil, fmt.Errorf("missing spec.workflowRef in workflow run")
	}

	// 1. Initial trigger phase
	if crd.Status == nil || crd.Status.Phase == "" || crd.Status.Phase == "Pending" {
		input := crd.Spec.Input
		if input == nil {
			input = make(map[string]interface{})
		}
		if crd.Spec.IdempotencyKey != "" {
			input["__idempotency_key"] = crd.Spec.IdempotencyKey
		}

		exec, err := c.engine.Execute(ctx, crd.Spec.WorkflowRef, input, model.TriggerTypeManual)
		if err != nil {
			now := time.Now()
			return &WorkflowRunStatus{
				Phase:       "Failed",
				StartedAt:   &now,
				CompletedAt: &now,
				Error:       err.Error(),
			}, nil
		}

		now := time.Now()
		return &WorkflowRunStatus{
			ExecutionID: exec.ID,
			Phase:       string(exec.Status),
			StartedAt:   &now,
		}, nil
	}

	// 2. Ongoing phase tracking
	if crd.Status.ExecutionID != "" && crd.Status.Phase == "Running" {
		exec, err := c.store.Executions().Get(ctx, crd.Status.ExecutionID)
		if err != nil {
			return crd.Status, nil // keep current status until accessible
		}

		return &WorkflowRunStatus{
			ExecutionID: exec.ID,
			Phase:       string(exec.Status),
			StartedAt:   &exec.StartedAt,
			CompletedAt: exec.CompletedAt,
			DurationMs:  exec.DurationMs,
			Output:      exec.Output,
			Error:       exec.Error,
		}, nil
	}

	return crd.Status, nil
}
