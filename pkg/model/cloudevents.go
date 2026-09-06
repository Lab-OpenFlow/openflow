package model

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CloudEvent represents a CNCF CloudEvents v1.0.2 compliant event structure.
// Specification: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
type CloudEvent struct {
	SpecVersion     string                 `json:"specversion"`
	ID              string                 `json:"id"`
	Source          string                 `json:"source"`
	Type            string                 `json:"type"`
	DataContentType string                 `json:"datacontenttype,omitempty"`
	DataSchema      string                 `json:"dataschema,omitempty"`
	Subject         string                 `json:"subject,omitempty"`
	Time            time.Time              `json:"time"`
	Data            map[string]interface{} `json:"data,omitempty"`
}

// NewCloudEvent creates a new CNCF CloudEvent envelope.
func NewCloudEvent(eventType, source string, data map[string]interface{}) *CloudEvent {
	return &CloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.New().String(),
		Source:          source,
		Type:            eventType,
		DataContentType: "application/json",
		Time:            time.Now().UTC(),
		Data:            data,
	}
}

// ToCloudEvent converts an internal WorkflowEvent to a standardized CNCF CloudEvent.
func (e *WorkflowEvent) ToCloudEvent(sourcePrefix string) *CloudEvent {
	if sourcePrefix == "" {
		sourcePrefix = "/openflow/engine"
	}
	source := fmt.Sprintf("%s/workflows/%s", sourcePrefix, e.WorkflowID)
	ceType := fmt.Sprintf("io.openflow.%s", e.Type)

	ce := NewCloudEvent(ceType, source, e.Payload)
	ce.Time = e.Timestamp.UTC()
	if e.ExecutionID != "" {
		ce.Subject = fmt.Sprintf("executions/%s", e.ExecutionID)
	}
	return ce
}

// ToJSON marshals the CloudEvent to standard JSON.
func (ce *CloudEvent) ToJSON() ([]byte, error) {
	return json.Marshal(ce)
}
