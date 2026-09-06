package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// WebSocketConnector publishes messages over WebSocket connections or triggers.
type WebSocketConnector struct{}

// NewWebSocketConnector creates a new WebSocket connector.
func NewWebSocketConnector() *WebSocketConnector {
	return &WebSocketConnector{}
}

func (c *WebSocketConnector) Type() model.ConnectorType {
	return model.ConnectorTypeWebSocket
}

func (c *WebSocketConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeWebSocket,
		Name:        "WebSocket Stream Connector",
		Description: "Broadcast and stream events over WebSocket client channels in real-time",
		Category:    "network",
		Icon:        "Radio",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"required": []string{"url"},
			"properties": map[string]interface{}{
				"url":     map[string]interface{}{"type": "string", "description": "Target WebSocket URL (ws:// or wss://)"},
				"payload": map[string]interface{}{"type": "object", "description": "JSON message payload to send"},
				"headers": map[string]interface{}{"type": "object", "description": "Custom handshake headers"},
			},
		},
	}
}

func (c *WebSocketConnector) Validate(config map[string]interface{}) error {
	url, ok := config["url"].(string)
	if !ok || (!strings.HasPrefix(url, "ws://") && !strings.HasPrefix(url, "wss://")) {
		return fmt.Errorf("websocket connector: 'url' must start with ws:// or wss://")
	}
	return nil
}

func (c *WebSocketConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	url, _ := config["url"].(string)

	var payloadBytes []byte
	if pObj, exists := config["payload"]; exists && pObj != nil {
		b, err := json.Marshal(pObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal websocket payload: %w", err)
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

	header := http.Header{}
	header.Set("X-OpenFlow-Execution-ID", execCtx.ExecutionID)
	if customHeaders, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range customHeaders {
			header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, url, header)
	if err != nil {
		return nil, fmt.Errorf("failed to dial websocket at %s: %w", url, err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, payloadBytes); err != nil {
		return nil, fmt.Errorf("failed to write websocket message: %w", err)
	}

	return map[string]interface{}{
		"url":        url,
		"bytes_sent": len(payloadBytes),
		"sent":       true,
		"timestamp":  time.Now().Format(time.RFC3339),
	}, nil
}
