package model

import "time"

// WorkflowVersionStatus represents the activation state of a workflow version.
type WorkflowVersionStatus string

const (
	VersionStatusActive     WorkflowVersionStatus = "ACTIVE"
	VersionStatusDeprecated WorkflowVersionStatus = "DEPRECATED"
	VersionStatusDraft      WorkflowVersionStatus = "DRAFT"
)

// WorkflowVersion represents an immutable historical snapshot of a workflow definition.
type WorkflowVersion struct {
	ID         string                `json:"id"`          // "ver_wf123_v2"
	WorkflowID string                `json:"workflow_id"` // "pix-crossborder-settlement"
	VersionNum int                   `json:"version_num"` // 1, 2, 3...
	Spec       Workflow              `json:"spec"`        // Complete frozen snapshot
	Status     WorkflowVersionStatus `json:"status"`
	CreatedAt  time.Time             `json:"created_at"`
}
