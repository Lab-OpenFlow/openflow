package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
)

// KafkaConnector produces events to Apache Kafka topics.
type KafkaConnector struct {
	defaultBrokers []string
}

// NewKafkaConnector creates a new Kafka connector.
func NewKafkaConnector(brokers []string) *KafkaConnector {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	return &KafkaConnector{defaultBrokers: brokers}
}

func (c *KafkaConnector) Type() model.ConnectorType {
	return model.ConnectorTypeKafka
}

func (c *KafkaConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeKafka,
		Name:        "Apache Kafka Producer",
		Description: "Publish structured events and messages to Apache Kafka topics with headers and partition keys",
		Category:    "messaging",
		Icon:        "Layers",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"required": []string{"topic"},
			"properties": map[string]interface{}{
				"brokers": map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}},
				"topic":   map[string]interface{}{"type": "string", "description": "Destination Kafka topic"},
				"key":     map[string]interface{}{"type": "string", "description": "Message partitioning key"},
				"value":   map[string]interface{}{"type": "object", "description": "Message payload (JSON)"},
				"headers": map[string]interface{}{"type": "object", "description": "Kafka message headers"},
			},
		},
	}
}

func (c *KafkaConnector) Validate(config map[string]interface{}) error {
	topic, ok := config["topic"].(string)
	if !ok || strings.TrimSpace(topic) == "" {
		return fmt.Errorf("kafka connector: 'topic' is required")
	}
	return nil
}

func (c *KafkaConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	topic, _ := config["topic"].(string)

	brokers := c.defaultBrokers
	if rawBrokers, ok := config["brokers"].([]interface{}); ok && len(rawBrokers) > 0 {
		var customBrokers []string
		for _, b := range rawBrokers {
			if s, ok := b.(string); ok {
				customBrokers = append(customBrokers, s)
			}
		}
		if len(customBrokers) > 0 {
			brokers = customBrokers
		}
	} else if rawBrokerStr, ok := config["broker"].(string); ok && rawBrokerStr != "" {
		brokers = strings.Split(rawBrokerStr, ",")
	}

	keyStr := ""
	if k, ok := config["key"].(string); ok {
		keyStr = k
	}

	var payloadBytes []byte
	if valObj, exists := config["value"]; exists && valObj != nil {
		b, err := json.Marshal(valObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal kafka value: %w", err)
		}
		payloadBytes = b
	} else if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal input payload: %w", err)
		}
		payloadBytes = b
	} else {
		payloadBytes = []byte("{}")
	}

	// Ensure topic exists on broker
	c.ensureTopic(ctx, brokers, topic)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	var headers []kafka.Header
	headers = append(headers, kafka.Header{Key: "openflow-execution-id", Value: []byte(execCtx.ExecutionID)})
	headers = append(headers, kafka.Header{Key: "openflow-workflow-id", Value: []byte(execCtx.WorkflowID)})
	headers = append(headers, kafka.Header{Key: "openflow-stage-id", Value: []byte(execCtx.StageID)})

	// Inject W3C Distributed Tracing headers (traceparent, tracestate)
	for k, v := range observability.InjectW3CTraceHeaders(ctx) {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	if customHeaders, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range customHeaders {
			headers = append(headers, kafka.Header{
				Key:   k,
				Value: []byte(fmt.Sprintf("%v", v)),
			})
		}
	}

	if execCtx != nil && execCtx.IdempotencyKey != "" {
		headers = append(headers, kafka.Header{
			Key:   "idempotency_key",
			Value: []byte(execCtx.IdempotencyKey),
		})
	}

	msg := kafka.Message{
		Key:     []byte(keyStr),
		Value:   payloadBytes,
		Headers: headers,
		Time:    time.Now(),
	}

	err := writer.WriteMessages(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to write message to kafka topic '%s' (%v): %w", topic, brokers, err)
	}

	return map[string]interface{}{
		"topic":     topic,
		"key":       keyStr,
		"brokers":   brokers,
		"bytes_sent": len(payloadBytes),
		"published": true,
		"timestamp": time.Now().Format(time.RFC3339),
	}, nil
}

func (c *KafkaConnector) ensureTopic(ctx context.Context, brokers []string, topic string) {
	if len(brokers) == 0 || topic == "" {
		return
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return
	}
	defer conn.Close()

	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}

