package model

import "time"

// WebhookBinding maps an inbound HTTP route pattern to an OpenFlow workflow.
type WebhookBinding struct {
	ID          string    `json:"id"`
	PathPattern string    `json:"path_pattern"` // e.g. "/pix/inbound", "orders"
	WorkflowID  string    `json:"workflow_id"`
	Method      string    `json:"method"`       // "POST", "GET", "PUT", "*"
	SecretToken string    `json:"secret_token,omitempty"`
	Description string    `json:"description,omitempty"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
