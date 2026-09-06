package model

import "time"

// TimerStatus represents the state of a durable timer.
type TimerStatus string

const (
	TimerStatusPending   TimerStatus = "PENDING"
	TimerStatusFired     TimerStatus = "FIRED"
	TimerStatusCancelled TimerStatus = "CANCELLED"
)

// DurableTimer represents a persisted sleep / delay timer in the engine.
type DurableTimer struct {
	ID          string      `json:"id"`
	ExecutionID string      `json:"execution_id"`
	WorkflowID  string      `json:"workflow_id"`
	StageID     string      `json:"stage_id"`
	FireAt      time.Time   `json:"fire_at"`
	Status      TimerStatus `json:"status"`
	CreatedAt   time.Time   `json:"created_at"`
	FiredAt     *time.Time  `json:"fired_at,omitempty"`
}
