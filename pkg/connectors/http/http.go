package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
)

// HTTPConnector implements outbound HTTP/REST API calls.
type HTTPConnector struct {
	client *http.Client
}

// NewHTTPConnector creates a new HTTPConnector with connection pooling.
func NewHTTPConnector() *HTTPConnector {
	return &HTTPConnector{
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *HTTPConnector) Type() model.ConnectorType {
	return model.ConnectorTypeHTTP
}

func (c *HTTPConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeHTTP,
		Name:        "HTTP / REST Client",
		Description: "Invoke external RESTful APIs and Webhooks with full header, query, and payload customization",
		Category:    "network",
		Icon:        "Globe",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"required": []string{"url", "method"},
			"properties": map[string]interface{}{
				"url":     map[string]interface{}{"type": "string", "description": "Target HTTP URL"},
				"method":  map[string]interface{}{"type": "string", "enum": []string{"GET", "POST", "PUT", "PATCH", "DELETE"}},
				"headers": map[string]interface{}{"type": "object", "description": "Key-value HTTP headers"},
				"body":    map[string]interface{}{"type": "object", "description": "JSON request body"},
				"timeout": map[string]interface{}{"type": "string", "description": "Request timeout (e.g. 5s, 10s)"},
			},
		},
	}
}

func (c *HTTPConnector) Validate(config map[string]interface{}) error {
	url, ok := config["url"].(string)
	if !ok || strings.TrimSpace(url) == "" {
		return fmt.Errorf("http connector: 'url' is required")
	}
	method, ok := config["method"].(string)
	if !ok || strings.TrimSpace(method) == "" {
		return fmt.Errorf("http connector: 'method' is required")
	}
	return nil
}

func (c *HTTPConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	urlStr, _ := config["url"].(string)
	method, _ := config["method"].(string)
	if method == "" {
		method = "GET"
	}
	method = strings.ToUpper(method)

	var reqBody io.Reader
	if bodyObj, exists := config["body"]; exists && bodyObj != nil {
		bodyBytes, err := json.Marshal(bodyObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal http body: %w", err)
		}
		reqBody = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	// Set headers
	req.Header.Set("User-Agent", "OpenFlow-Engine/1.0")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-OpenFlow-Execution-ID", execCtx.ExecutionID)
	req.Header.Set("X-OpenFlow-Workflow-ID", execCtx.WorkflowID)

	// Inject W3C Distributed Tracing context (traceparent, tracestate)
	for k, v := range observability.InjectW3CTraceHeaders(ctx) {
		req.Header.Set(k, v)
	}

	if headers, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	// Inject deterministic step idempotency key if not explicitly set
	if execCtx != nil && execCtx.IdempotencyKey != "" && req.Header.Get("Idempotency-Key") == "" {
		req.Header.Set("Idempotency-Key", execCtx.IdempotencyKey)
	}

	// Set timeout if configured
	client := c.client
	if timeoutStr, ok := config["timeout"].(string); ok && timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
			req = req.WithContext(ctx)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var parsedBody interface{}
	if len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, &parsedBody); err != nil {
			parsedBody = string(respBytes)
		}
	}

	respHeaders := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			respHeaders[k] = v[0]
		}
	}

	output := map[string]interface{}{
		"status_code": resp.StatusCode,
		"status":      resp.Status,
		"body":        parsedBody,
		"headers":     respHeaders,
		"success":     resp.StatusCode >= 200 && resp.StatusCode < 300,
	}

	if resp.StatusCode >= 400 {
		return output, fmt.Errorf("http request returned error status: %d %s", resp.StatusCode, resp.Status)
	}

	return output, nil
}
