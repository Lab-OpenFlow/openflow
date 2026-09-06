package model

import "time"

// TaskStatus represents the lifecycle state of a distributed worker task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusFailed    TaskStatus = "FAILED"
	TaskStatusTimedOut  TaskStatus = "TIMED_OUT"
)

// TaskItem represents an external activity task waiting for or being processed by a remote worker.
type TaskItem struct {
	ID               string                 `json:"id"`
	QueueName        string                 `json:"queue_name"`
	WorkflowID       string                 `json:"workflow_id"`
	ExecutionID      string                 `json:"execution_id"`
	StageID          string                 `json:"stage_id"`
	Input            map[string]interface{} `json:"input"`
	Output           map[string]interface{} `json:"output,omitempty"`
	ErrorMessage     string                 `json:"error_message,omitempty"`
	Status           TaskStatus             `json:"status"`
	WorkerID         string                 `json:"worker_id,omitempty"`
	LockToken        string                 `json:"lock_token,omitempty"` // UUID required to acknowledge completion
	LeaseExpiresAt   *time.Time             `json:"lease_expires_at,omitempty"`
	HeartbeatTimeout time.Duration          `json:"heartbeat_timeout"`
	Attempts         int                    `json:"attempts"`
	MaxAttempts      int                    `json:"max_attempts"`
	CreatedAt        time.Time              `json:"created_at"`
	StartedAt        *time.Time             `json:"started_at,omitempty"`
	CompletedAt      *time.Time             `json:"completed_at,omitempty"`
}
