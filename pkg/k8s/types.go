package k8s

import (
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// WorkflowCRD represents a Kubernetes Custom Resource for OpenFlow workflows.
type WorkflowCRD struct {
	APIVersion string          `json:"apiVersion" yaml:"apiVersion"`
	Kind       string          `json:"kind" yaml:"kind"`
	Metadata   ObjectMetadata  `json:"metadata" yaml:"metadata"`
	Spec       WorkflowSpec    `json:"spec" yaml:"spec"`
	Status     *WorkflowStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ObjectMetadata represents standard Kubernetes object metadata.
type ObjectMetadata struct {
	Name        string            `json:"name" yaml:"name"`
	Namespace   string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty" yaml:"annotations,omitempty"`
}

// WorkflowSpec defines the desired state of a Workflow.
type WorkflowSpec struct {
	Name         string                 `json:"name,omitempty" yaml:"name,omitempty"`
	Version      string                 `json:"version,omitempty" yaml:"version,omitempty"`
	Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
	StartAt      string                 `json:"startAt" yaml:"startAt"`
	SagaStrategy string                 `json:"sagaStrategy,omitempty" yaml:"sagaStrategy,omitempty"`
	Variables    map[string]interface{} `json:"variables,omitempty" yaml:"variables,omitempty"`
	Stages       []model.Stage          `json:"stages" yaml:"stages"`
}

// WorkflowStatus defines the observed state of a Workflow.
type WorkflowStatus struct {
	Phase            string    `json:"phase,omitempty" yaml:"phase,omitempty"`
	LastSyncedAt     time.Time `json:"lastSyncedAt,omitempty" yaml:"lastSyncedAt,omitempty"`
	ActiveExecutions int       `json:"activeExecutions,omitempty" yaml:"activeExecutions,omitempty"`
}

// ToModel converts the CRD to an internal model.Workflow.
func (w *WorkflowCRD) ToModel() *model.Workflow {
	name := w.Spec.Name
	if name == "" {
		name = w.Metadata.Name
	}
	return &model.Workflow{
		ID:           w.Metadata.Name,
		Name:         name,
		Version:      w.Spec.Version,
		Description:  w.Spec.Description,
		Status:       model.WorkflowStatusActive,
		StartAt:      w.Spec.StartAt,
		SagaStrategy: model.SagaStrategy(w.Spec.SagaStrategy),
		Variables:    w.Spec.Variables,
		Stages:       w.Spec.Stages,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

// WorkflowRunCRD represents an execution run instance requested through Kubernetes.
type WorkflowRunCRD struct {
	APIVersion string             `json:"apiVersion" yaml:"apiVersion"`
	Kind       string             `json:"kind" yaml:"kind"`
	Metadata   ObjectMetadata     `json:"metadata" yaml:"metadata"`
	Spec       WorkflowRunSpec    `json:"spec" yaml:"spec"`
	Status     *WorkflowRunStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// WorkflowRunSpec defines the workflow to run and its initial payload.
type WorkflowRunSpec struct {
	WorkflowRef    string                 `json:"workflowRef" yaml:"workflowRef"`
	IdempotencyKey string                 `json:"idempotencyKey,omitempty" yaml:"idempotencyKey,omitempty"`
	Input          map[string]interface{} `json:"input,omitempty" yaml:"input,omitempty"`
}

// WorkflowRunStatus reflects the execution progress.
type WorkflowRunStatus struct {
	ExecutionID string                 `json:"executionId,omitempty" yaml:"executionId,omitempty"`
	Phase       string                 `json:"phase" yaml:"phase"` // Pending, Running, Completed, Failed, Compensated
	StartedAt   *time.Time             `json:"startedAt,omitempty" yaml:"startedAt,omitempty"`
	CompletedAt *time.Time             `json:"completedAt,omitempty" yaml:"completedAt,omitempty"`
	DurationMs  int64                  `json:"durationMs,omitempty" yaml:"durationMs,omitempty"`
	Output      map[string]interface{} `json:"output,omitempty" yaml:"output,omitempty"`
	Error       string                 `json:"error,omitempty" yaml:"error,omitempty"`
}
