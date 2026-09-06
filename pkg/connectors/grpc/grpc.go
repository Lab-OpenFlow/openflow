package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
)

// GRPCConnector implements high-performance outbound gRPC invocations with connection pooling.
type GRPCConnector struct {
	connPool sync.Map // map[string]*grpc.ClientConn
}

// NewGRPCConnector creates a new gRPC connector.
func NewGRPCConnector() *GRPCConnector {
	return &GRPCConnector{}
}

func (c *GRPCConnector) Type() model.ConnectorType {
	return model.ConnectorTypeGRPC
}

func (c *GRPCConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeGRPC,
		Name:        "gRPC Service Client",
		Description: "Invoke high-performance RPC methods on gRPC microservices with metadata and trace propagation",
		Category:    "network",
		Icon:        "Zap",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"required": []string{"target", "service", "method"},
			"properties": map[string]interface{}{
				"target":  map[string]interface{}{"type": "string", "description": "gRPC server target (host:port)"},
				"service": map[string]interface{}{"type": "string", "description": "Full service name (package.Service)"},
				"method":  map[string]interface{}{"type": "string", "description": "RPC Method name"},
				"data":    map[string]interface{}{"type": "object", "description": "RPC request message payload"},
				"headers": map[string]interface{}{"type": "object", "description": "gRPC metadata / headers"},
				"timeout": map[string]interface{}{"type": "string", "description": "Invocation timeout (e.g. 5s)"},
			},
		},
	}
}

func (c *GRPCConnector) Validate(config map[string]interface{}) error {
	target, _ := config["target"].(string)
	service, _ := config["service"].(string)
	method, _ := config["method"].(string)
	if strings.TrimSpace(target) == "" || strings.TrimSpace(service) == "" || strings.TrimSpace(method) == "" {
		return fmt.Errorf("grpc connector: 'target', 'service', and 'method' are required")
	}
	return nil
}

func (c *GRPCConnector) getConn(target string) (*grpc.ClientConn, error) {
	if val, ok := c.connPool.Load(target); ok {
		return val.(*grpc.ClientConn), nil
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC target '%s': %w", target, err)
	}

	c.connPool.Store(target, conn)
	return conn, nil
}

func (c *GRPCConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	target, _ := config["target"].(string)
	service, _ := config["service"].(string)
	method, _ := config["method"].(string)

	payload := input
	if dataObj, exists := config["data"].(map[string]interface{}); exists {
		payload = dataObj
	}

	fullMethod := fmt.Sprintf("/%s/%s", service, method)

	// Set timeout
	timeout := 10 * time.Second
	if timeoutStr, ok := config["timeout"].(string); ok && timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = d
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Inject W3C Trace Context and custom gRPC metadata
	mdMap := make(map[string]string)
	mdMap["x-openflow-execution-id"] = execCtx.ExecutionID
	mdMap["x-openflow-workflow-id"] = execCtx.WorkflowID
	mdMap["x-openflow-stage-id"] = execCtx.StageID

	for k, v := range observability.InjectW3CTraceHeaders(ctx) {
		mdMap[k] = v
	}

	if customHeaders, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range customHeaders {
			mdMap[strings.ToLower(k)] = fmt.Sprintf("%v", v)
		}
	}

	if execCtx != nil && execCtx.IdempotencyKey != "" {
		if _, exists := mdMap["x-idempotency-key"]; !exists {
			mdMap["x-idempotency-key"] = execCtx.IdempotencyKey
		}
	}

	md := metadata.New(mdMap)
	callCtx = metadata.NewOutgoingContext(callCtx, md)

	conn, err := c.getConn(target)
	if err != nil {
		// If real dial fails, check if running in standalone test mode or return error
		return map[string]interface{}{
			"target":      target,
			"service":     service,
			"method":      method,
			"full_method": fullMethod,
			"payload":     payload,
			"error":       err.Error(),
			"status":      "DIAL_ERROR",
			"timestamp":   time.Now().Format(time.RFC3339),
		}, fmt.Errorf("gRPC dial error: %w", err)
	}

	// Invoke raw dynamic RPC call over gRPC channel
	payloadBytes, _ := json.Marshal(payload)
	var responseBytes []byte

	err = conn.Invoke(callCtx, fullMethod, payloadBytes, &responseBytes)
	if err != nil {
		return map[string]interface{}{
			"target":      target,
			"service":     service,
			"method":      method,
			"full_method": fullMethod,
			"error":       err.Error(),
			"timestamp":   time.Now().Format(time.RFC3339),
		}, fmt.Errorf("gRPC invoke '%s' failed: %w", fullMethod, err)
	}

	var responseObj interface{}
	if err := json.Unmarshal(responseBytes, &responseObj); err != nil {
		responseObj = string(responseBytes)
	}

	return map[string]interface{}{
		"target":      target,
		"service":     service,
		"method":      method,
		"full_method": fullMethod,
		"response":    responseObj,
		"status":      "OK",
		"code":        0,
		"timestamp":   time.Now().Format(time.RFC3339),
	}, nil
}
