package model

// ConnectorType defines supported connector drivers.
type ConnectorType string

const (
	ConnectorTypeHTTP      ConnectorType = "http"
	ConnectorTypeKafka     ConnectorType = "kafka"
	ConnectorTypeRabbitMQ  ConnectorType = "rabbitmq"
	ConnectorTypeGRPC      ConnectorType = "grpc"
	ConnectorTypeWebSocket ConnectorType = "websocket"
	ConnectorTypeDatabase  ConnectorType = "database"
	ConnectorTypeTransform ConnectorType = "transform"
	ConnectorTypeScript    ConnectorType = "script"
	ConnectorTypeWasm      ConnectorType = "wasm"
	ConnectorTypeDMN       ConnectorType = "dmn"
)

// ConnectorDescriptor describes a connector's capabilities, inputs, and schemas for the UI & validation.
type ConnectorDescriptor struct {
	Type        ConnectorType          `json:"type"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Category    string                 `json:"category"` // "messaging", "network", "compute", "storage"
	Icon        string                 `json:"icon"`
	Version     string                 `json:"version"`
	ConfigSchema map[string]interface{} `json:"config_schema"` // JSON schema
	InputsSchema map[string]interface{} `json:"inputs_schema"`
	OutputsSchema map[string]interface{} `json:"outputs_schema"`
}
