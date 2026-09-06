package model

import "time"

// EventType defines real-time event types broadcasted over WebSockets and logs.
type EventType string

const (
	EventWorkflowCreated     EventType = "workflow.created"
	EventWorkflowUpdated     EventType = "workflow.updated"
	EventWorkflowDeleted     EventType = "workflow.deleted"
	
	EventExecutionStarted    EventType = "execution.started"
	EventExecutionProgress   EventType = "execution.progress"
	EventExecutionCompleted  EventType = "execution.completed"
	EventExecutionFailed     EventType = "execution.failed"
	EventExecutionCancelled  EventType = "execution.cancelled"
	EventExecutionWaitingApproval EventType = "execution.waiting_approval"
	EventApprovalRequested   EventType = "approval.requested"
	EventApprovalDecided     EventType = "approval.decided"
	EventExecutionWaitingTimer  EventType = "execution.waiting_timer"
	EventExecutionWaitingSignal EventType = "execution.waiting_signal"
	EventExecutionWaitingChild  EventType = "execution.waiting_child"
	EventSignalReceived         EventType = "signal.received"
	EventTimerFired             EventType = "timer.fired"
	EventChildWorkflowStarted   EventType = "child_workflow.started"
	EventChildWorkflowCompleted EventType = "child_workflow.completed" 
	
	EventStepStarted         EventType = "step.started"
	EventStepCompleted       EventType = "step.completed"
	EventStepFailed          EventType = "step.failed"
	EventStepRetrying        EventType = "step.retrying"
	
	EventSagaStarted         EventType = "saga.started"
	EventSagaStepCompleted   EventType = "saga.step.completed"
	EventSagaCompleted       EventType = "saga.completed"
	EventSagaFailed          EventType = "saga.failed"
)

// WorkflowEvent represents a real-time event message.
type WorkflowEvent struct {
	Type        EventType              `json:"type"`
	ExecutionID string                 `json:"execution_id,omitempty"`
	WorkflowID  string                 `json:"workflow_id,omitempty"`
	StageID     string                 `json:"stage_id,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	Payload     map[string]interface{} `json:"payload,omitempty"`
}

// AuditLogEntry represents an audit trail record.
type AuditLogEntry struct {
	ID          string                 `json:"id"`
	Timestamp   time.Time              `json:"timestamp"`
	Actor       string                 `json:"actor"` // e.g. "admin", "api-key:orders", "system"
	Action      string                 `json:"action"` // "CREATE_WORKFLOW", "EXECUTE_WORKFLOW", "CANCEL_EXECUTION"
	Resource    string                 `json:"resource"`
	ResourceID  string                 `json:"resource_id"`
	Details     map[string]interface{} `json:"details,omitempty"`
	ClientIP    string                 `json:"client_ip,omitempty"`
}
