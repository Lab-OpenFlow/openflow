package model

import (
	"time"
)

// ExecutionStatus defines the current status of a workflow instance.
type ExecutionStatus string

const (
	ExecutionStatusPending      ExecutionStatus = "PENDING"
	ExecutionStatusRunning      ExecutionStatus = "RUNNING"
	ExecutionStatusCompleted    ExecutionStatus = "COMPLETED"
	ExecutionStatusFailed       ExecutionStatus = "FAILED"
	ExecutionStatusCompensating ExecutionStatus = "COMPENSATING"
	ExecutionStatusCompensated  ExecutionStatus = "COMPENSATED"
	ExecutionStatusCancelled    ExecutionStatus = "CANCELLED"
	ExecutionStatusSuspended    ExecutionStatus = "SUSPENDED"
	ExecutionStatusWaitingApproval ExecutionStatus = "WAITING_APPROVAL"
	ExecutionStatusWaitingTimer    ExecutionStatus = "WAITING_TIMER"
	ExecutionStatusWaitingSignal   ExecutionStatus = "WAITING_SIGNAL"
	ExecutionStatusWaitingChild    ExecutionStatus = "WAITING_CHILD"
	ExecutionStatusWaitingWorker   ExecutionStatus = "WAITING_WORKER"
)

// StepStatus defines the status of an individual stage execution.
type StepStatus string

const (
	StepStatusPending      StepStatus = "PENDING"
	StepStatusRunning      StepStatus = "RUNNING"
	StepStatusCompleted    StepStatus = "COMPLETED"
	StepStatusFailed       StepStatus = "FAILED"
	StepStatusSkipped      StepStatus = "SKIPPED"
	StepStatusRetrying     StepStatus = "RETRYING"
	StepStatusCompensating StepStatus = "COMPENSATING"
	StepStatusCompensated  StepStatus = "COMPENSATED"
	StepStatusWaitingApproval StepStatus = "WAITING_APPROVAL"
	StepStatusWaitingTimer    StepStatus = "WAITING_TIMER"
	StepStatusWaitingSignal   StepStatus = "WAITING_SIGNAL"
	StepStatusWaitingChild    StepStatus = "WAITING_CHILD"
	StepStatusWaitingWorker   StepStatus = "WAITING_WORKER"
)

// StepExecution represents the runtime state and history of a single stage.
type StepExecution struct {
	ID             string                 `json:"id"`
	StageID        string                 `json:"stage_id"`
	StageName      string                 `json:"stage_name"`
	StageType      StageType              `json:"stage_type"`
	Status         StepStatus             `json:"status"`
	Attempts       int                    `json:"attempts"`
	Input          map[string]interface{} `json:"input,omitempty"`
	Output         map[string]interface{} `json:"output,omitempty"`
	Error          string                 `json:"error,omitempty"`
	StartedAt      time.Time              `json:"started_at"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty"`
	DurationMs     int64                  `json:"duration_ms"`
	IsCompensation bool                   `json:"is_compensation,omitempty"`
	CompensatesFor string                 `json:"compensates_for,omitempty"`
	MerkleHash     string                 `json:"merkle_hash,omitempty"`
	HeartbeatAt    *time.Time             `json:"heartbeat_at,omitempty"`
	PrevHash       string                 `json:"prev_hash,omitempty"`
}

// Execution represents a workflow runtime instance.
type Execution struct {
	ID             string                 `json:"id"`
	WorkflowID     string                 `json:"workflow_id"`
	WorkflowName   string                 `json:"workflow_name"`
	TriggerType    TriggerType            `json:"trigger_type"`
	Status         ExecutionStatus        `json:"status"`
	IdempotencyKey    string                 `json:"idempotency_key,omitempty"`
	ParentExecutionID string                 `json:"parent_execution_id,omitempty"`
	ParentStageID     string                 `json:"parent_stage_id,omitempty"`
	WorkflowVersion   int                    `json:"workflow_version,omitempty"`
	Input          map[string]interface{} `json:"input"`
	Output         map[string]interface{} `json:"output,omitempty"`
	Variables      map[string]interface{} `json:"variables"`
	Steps          []*StepExecution       `json:"steps"`
	Error          string                 `json:"error,omitempty"`
	TraceID        string                 `json:"trace_id"`
	MerkleRoot     string                 `json:"merkle_root,omitempty"`
	StartedAt      time.Time              `json:"started_at"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty"`
	DurationMs     int64                  `json:"duration_ms"`
	HeartbeatAt    *time.Time             `json:"heartbeat_at,omitempty"`
	WorkerID       string                 `json:"worker_id,omitempty"`
}

// FindStep finds a step execution by stage ID.
func (e *Execution) FindStep(stageID string) *StepExecution {
	for _, s := range e.Steps {
		if s.StageID == stageID && !s.IsCompensation {
			return s
		}
	}
	return nil
}

// CompletedStages returns a list of successfully completed stage IDs in chronological order.
func (e *Execution) CompletedStages() []*StepExecution {
	var completed []*StepExecution
	for _, s := range e.Steps {
		if s.Status == StepStatusCompleted && !s.IsCompensation {
			completed = append(completed, s)
		}
	}
	return completed
}

// ApprovalStatus defines the status of a manual approval request.
type ApprovalStatus string

const (
	ApprovalStatusPending  ApprovalStatus = "PENDING"
	ApprovalStatusApproved ApprovalStatus = "APPROVED"
	ApprovalStatusRejected ApprovalStatus = "REJECTED"
)

// ApprovalRequest represents a human-in-the-loop manual review gate.
type ApprovalRequest struct {
	ID           string                 `json:"id"`
	ExecutionID  string                 `json:"execution_id"`
	WorkflowID   string                 `json:"workflow_id"`
	WorkflowName string                 `json:"workflow_name"`
	StageID      string                 `json:"stage_id"`
	StageName    string                 `json:"stage_name"`
	Title        string                 `json:"title"`
	Description  string                 `json:"description"`
	Payload      map[string]interface{} `json:"payload"`
	RequiredRole string                 `json:"required_role"`
	Status       ApprovalStatus         `json:"status"`
	DecidedBy    string                 `json:"decided_by,omitempty"`
	DecidedAt    *time.Time             `json:"decided_at,omitempty"`
	Reason       string                 `json:"reason,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
}
