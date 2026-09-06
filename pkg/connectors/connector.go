package connectors

import (
	"context"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// ExecutionContext provides contextual information passed to connectors during execution.
type ExecutionContext struct {
	ExecutionID    string
	WorkflowID     string
	StageID        string
	StageName      string
	Variables      map[string]interface{}
	Payload        map[string]interface{}
	Steps          map[string]interface{}
	Attempt        int
	IdempotencyKey string
}

// Connector defines the lifecycle and execution interface for all protocol adapters.
type Connector interface {
	// Type returns the connector type identifier.
	Type() model.ConnectorType
	
	// Descriptor returns the metadata and schema definition of the connector.
	Descriptor() model.ConnectorDescriptor
	
	// Validate checks if the stage configuration contains all required fields and valid formats.
	Validate(config map[string]interface{}) error
	
	// Execute performs the protocol operation.
	Execute(ctx context.Context, execCtx *ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error)
}
