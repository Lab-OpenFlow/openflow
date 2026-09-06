package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
)

// RabbitMQConnector publishes messages to RabbitMQ exchanges or queues.
type RabbitMQConnector struct {
	defaultURL string
}

// NewRabbitMQConnector creates a new RabbitMQ connector.
func NewRabbitMQConnector(url string) *RabbitMQConnector {
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	return &RabbitMQConnector{defaultURL: url}
}

func (c *RabbitMQConnector) Type() model.ConnectorType {
	return model.ConnectorTypeRabbitMQ
}

func (c *RabbitMQConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeRabbitMQ,
		Name:        "RabbitMQ Publisher",
		Description: "Publish AMQP messages to RabbitMQ exchanges with routing keys and headers",
		Category:    "messaging",
		Icon:        "Send",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"url":         map[string]interface{}{"type": "string", "description": "AMQP connection URL"},
				"exchange":    map[string]interface{}{"type": "string", "description": "Exchange name (optional)"},
				"routing_key": map[string]interface{}{"type": "string", "description": "Routing key / Queue name"},
				"payload":     map[string]interface{}{"type": "object", "description": "Message payload"},
				"headers":     map[string]interface{}{"type": "object", "description": "AMQP headers"},
			},
		},
	}
}

func (c *RabbitMQConnector) Validate(config map[string]interface{}) error {
	routingKey, _ := config["routing_key"].(string)
	exchange, _ := config["exchange"].(string)
	if strings.TrimSpace(routingKey) == "" && strings.TrimSpace(exchange) == "" {
		return fmt.Errorf("rabbitmq connector: either 'exchange' or 'routing_key' is required")
	}
	return nil
}

func (c *RabbitMQConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	url := c.defaultURL
	if customURL, ok := config["url"].(string); ok && customURL != "" {
		url = customURL
	}

	exchange, _ := config["exchange"].(string)
	routingKey, _ := config["routing_key"].(string)

	var payloadBytes []byte
	if pObj, exists := config["payload"]; exists && pObj != nil {
		b, err := json.Marshal(pObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal rabbitmq payload: %w", err)
		}
		payloadBytes = b
	} else if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal input: %w", err)
		}
		payloadBytes = b
	} else {
		payloadBytes = []byte("{}")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq (%s): %w", url, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open rabbitmq channel: %w", err)
	}
	defer ch.Close()

	headers := amqp.Table{
		"openflow-execution-id": execCtx.ExecutionID,
		"openflow-workflow-id":  execCtx.WorkflowID,
		"openflow-stage-id":     execCtx.StageID,
	}

	// Inject W3C Distributed Tracing headers (traceparent, tracestate)
	for k, v := range observability.InjectW3CTraceHeaders(ctx) {
		headers[k] = v
	}

	if customHeaders, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range customHeaders {
			headers[k] = v
		}
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         payloadBytes,
	}

	err = ch.PublishWithContext(ctx, exchange, routingKey, false, false, publishing)
	if err != nil {
		return nil, fmt.Errorf("failed to publish rabbitmq message: %w", err)
	}

	return map[string]interface{}{
		"exchange":    exchange,
		"routing_key": routingKey,
		"bytes_sent":  len(payloadBytes),
		"published":   true,
		"timestamp":   time.Now().Format(time.RFC3339),
	}, nil
}
