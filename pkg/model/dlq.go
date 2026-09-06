package model

import "time"

type DLQStatus string

const (
	DLQStatusPending   DLQStatus = "PENDING"
	DLQStatusReplayed  DLQStatus = "REPLAYED"
	DLQStatusDiscarded DLQStatus = "DISCARDED"
	// DLQStatusDead marks messages that exhausted all auto-retry attempts.
	DLQStatusDead DLQStatus = "DEAD"
)

// DLQMessage represents a failed event/trigger message stored in the Dead Letter Queue.
type DLQMessage struct {
	ID           string                 `json:"id"`
	WorkflowID   string                 `json:"workflow_id"`
	Source       string                 `json:"source"` // "webhook", "kafka", "rabbitmq", "execution"
	TopicOrPath  string                 `json:"topic_or_path"`
	Payload      map[string]interface{} `json:"payload"`
	Headers      map[string]string      `json:"headers,omitempty"`
	ErrorMessage string                 `json:"error_message"`
	StackTrace   string                 `json:"stack_trace,omitempty"`
	Status       DLQStatus              `json:"status"`
	Attempts     int                    `json:"attempts"`
	CreatedAt    time.Time              `json:"created_at"`
	ReplayedAt   *time.Time             `json:"replayed_at,omitempty"`
	// Auto-retry tracking fields (populated by DLQWorker)
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
	IsDead        bool       `json:"is_dead"`
	DeadReason    string     `json:"dead_reason,omitempty"`
}

