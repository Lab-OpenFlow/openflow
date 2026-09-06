package storage

import (
	"context"
	"errors"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

var (
	ErrWorkflowNotFound   = errors.New("workflow not found")
	ErrExecutionNotFound  = errors.New("execution not found")
	ErrApprovalNotFound   = errors.New("approval request not found")
	ErrTimerNotFound      = errors.New("timer not found")
	ErrSignalNotFound     = errors.New("signal subscription not found")
	ErrVersionNotFound    = errors.New("workflow version not found")
)

// WorkflowFilter defines criteria for querying workflows.
type WorkflowFilter struct {
	Status model.WorkflowStatus
	Search string
	Tag    string
	Tags   []string
	Limit  int
	Offset int
}

// ExecutionFilter defines criteria for querying executions.
type ExecutionFilter struct {
	WorkflowID string
	Status     model.ExecutionStatus
	TraceID    string
	Limit      int
	Offset     int
}

// WorkflowStore manages workflow definitions.
type WorkflowStore interface {
	Create(ctx context.Context, wf *model.Workflow) error
	Get(ctx context.Context, id string) (*model.Workflow, error)
	Update(ctx context.Context, wf *model.Workflow) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context, filter WorkflowFilter) ([]*model.Workflow, int, error)
}

// ExecutionStore manages workflow execution instances and step history.
type ExecutionStore interface {
	Create(ctx context.Context, exec *model.Execution) error
	Get(ctx context.Context, id string) (*model.Execution, error)
	GetByIdempotencyKey(ctx context.Context, workflowID, idempotencyKey string) (*model.Execution, error)
	Update(ctx context.Context, exec *model.Execution) error
	List(ctx context.Context, filter ExecutionFilter) ([]*model.Execution, int, error)
	AppendStep(ctx context.Context, execID string, step *model.StepExecution) error
	ListChildren(ctx context.Context, parentExecID string) ([]*model.Execution, error)
	UpdateStepHeartbeat(ctx context.Context, stepID string, heartbeat time.Time) error
	UpdateHeartbeat(ctx context.Context, execID string, heartbeat time.Time) error
	ClaimStalledExecutions(ctx context.Context, workerID string, stalledThreshold time.Duration, limit int) ([]*model.Execution, error)
}

// AuditStore manages immutable audit trails.
type AuditStore interface {
	Log(ctx context.Context, entry *model.AuditLogEntry) error
	List(ctx context.Context, limit, offset int) ([]*model.AuditLogEntry, int, error)
}

// ApprovalStore manages manual Human-in-the-Loop review requests.
type ApprovalStore interface {
	Create(ctx context.Context, req *model.ApprovalRequest) error
	Get(ctx context.Context, id string) (*model.ApprovalRequest, error)
	GetByExecutionID(ctx context.Context, executionID string) (*model.ApprovalRequest, error)
	Update(ctx context.Context, req *model.ApprovalRequest) error
	ListPending(ctx context.Context) ([]*model.ApprovalRequest, error)
	List(ctx context.Context, limit, offset int) ([]*model.ApprovalRequest, int, error)
}

// TimerStore manages persisted durable sleep / delay timers.
type TimerStore interface {
	Create(ctx context.Context, timer *model.DurableTimer) error
	Get(ctx context.Context, id string) (*model.DurableTimer, error)
	ListDue(ctx context.Context, now time.Time) ([]*model.DurableTimer, error)
	UpdateStatus(ctx context.Context, id string, status model.TimerStatus, firedAt *time.Time) error
	Delete(ctx context.Context, id string) error
}

// SignalStore manages external event subscriptions.
type SignalStore interface {
	Create(ctx context.Context, sig *model.SignalSubscription) error
	Get(ctx context.Context, id string) (*model.SignalSubscription, error)
	ListByExecution(ctx context.Context, executionID string) ([]*model.SignalSubscription, error)
	FindWaiting(ctx context.Context, executionID, signalName string) (*model.SignalSubscription, error)
	Update(ctx context.Context, sig *model.SignalSubscription) error
}

// WorkflowVersionStore manages immutable historical version snapshots of workflows.
type WorkflowVersionStore interface {
	CreateVersion(ctx context.Context, ver *model.WorkflowVersion) error
	GetVersion(ctx context.Context, workflowID string, versionNum int) (*model.WorkflowVersion, error)
	ListVersions(ctx context.Context, workflowID string) ([]*model.WorkflowVersion, error)
	GetActiveVersion(ctx context.Context, workflowID string) (*model.WorkflowVersion, error)
	SetActiveVersion(ctx context.Context, workflowID string, versionNum int) error
}

// DLQFilter specifies search criteria for Dead Letter Queue messages.
type DLQFilter struct {
	Status     model.DLQStatus
	WorkflowID string
	Source     string
	Limit      int
	Offset     int
}

// DLQStore manages failed trigger events and poison pill messages.
type DLQStore interface {
	Push(ctx context.Context, msg *model.DLQMessage) error
	Get(ctx context.Context, id string) (*model.DLQMessage, error)
	List(ctx context.Context, filter DLQFilter) ([]*model.DLQMessage, int, error)
	UpdateStatus(ctx context.Context, id string, status model.DLQStatus, replayedAt *time.Time) error
	Delete(ctx context.Context, id string) error
	// ListPending returns up to `limit` non-dead messages eligible for auto-retry.
	ListPending(ctx context.Context, limit int) ([]*model.DLQMessage, error)
	// IncrementAttempts bumps the attempt counter and records the last error message.
	IncrementAttempts(ctx context.Context, id, errMsg string) error
	// MarkDead marks the message as permanently dead after max retries are exhausted.
	MarkDead(ctx context.Context, id, reason string) error
	// MarkResolved marks the message as replayed successfully.
	MarkResolved(ctx context.Context, id string) error
}



// WebhookStore manages dynamic inbound HTTP webhook route bindings.
type WebhookStore interface {
	Create(ctx context.Context, binding *model.WebhookBinding) error
	Get(ctx context.Context, id string) (*model.WebhookBinding, error)
	FindByPath(ctx context.Context, path string, method string) (*model.WebhookBinding, error)
	List(ctx context.Context) ([]*model.WebhookBinding, error)
	Delete(ctx context.Context, id string) error
}


// TaskQueueStore manages distributed worker task polling, leases, and completion states.
type TaskQueueStore interface {
	Create(ctx context.Context, task *model.TaskItem) error
	Get(ctx context.Context, id string) (*model.TaskItem, error)
	Update(ctx context.Context, task *model.TaskItem) error
	AcquirePending(ctx context.Context, queueName, workerID string, leaseDuration time.Duration) (*model.TaskItem, error)
	ExtendLease(ctx context.Context, taskID string, newLeaseExpiresAt time.Time) error
	ReclaimExpiredLeases(ctx context.Context) (int, error)
	List(ctx context.Context, queueName string, status model.TaskStatus) ([]*model.TaskItem, error)
}

// OutboxStore manages reliable event persistence and transactional outbox relay draining.
type OutboxStore interface {
	Push(ctx context.Context, event *model.WorkflowEvent) error
	FetchPending(ctx context.Context, limit int) ([]*model.WorkflowEvent, error)
	MarkProcessed(ctx context.Context, eventIDs []string) error
}

// Store provides unified access to all storage interfaces.
type Store interface {
	Workflows() WorkflowStore
	Executions() ExecutionStore
	Audits() AuditStore
	Approvals() ApprovalStore
	Timers() TimerStore
	Signals() SignalStore
	WorkflowVersions() WorkflowVersionStore
	DLQ() DLQStore
	Webhooks() WebhookStore
	TaskQueues() TaskQueueStore
	Outbox() OutboxStore
	Close() error
}
