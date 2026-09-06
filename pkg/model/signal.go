package model

import "time"

// SignalStatus represents the state of a signal subscription.
type SignalStatus string

const (
	SignalStatusWaiting  SignalStatus = "WAITING"
	SignalStatusReceived SignalStatus = "RECEIVED"
	SignalStatusTimedOut SignalStatus = "TIMED_OUT"
)

// SignalSubscription represents an execution waiting for an external event / signal.
type SignalSubscription struct {
	ID          string                 `json:"id"`
	ExecutionID string                 `json:"execution_id"`
	WorkflowID  string                 `json:"workflow_id"`
	StageID     string                 `json:"stage_id"`
	SignalName  string                 `json:"signal_name"`
	Payload     map[string]interface{} `json:"payload,omitempty"`
	Status      SignalStatus           `json:"status"`
	TimeoutAt   *time.Time             `json:"timeout_at,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	ReceivedAt  *time.Time             `json:"received_at,omitempty"`
}
