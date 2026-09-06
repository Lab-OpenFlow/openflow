package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	httpconn "github.com/Lab-OpenFlow/openflow/pkg/connectors/http"
)

func TestHTTPConnectorIdempotencyKeyInjection(t *testing.T) {
	var receivedIdempKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedIdempKey = r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	conn := httpconn.NewHTTPConnector()

	execCtx := &connectors.ExecutionContext{
		ExecutionID:    "exec_123",
		WorkflowID:     "wf_abc",
		StageID:        "charge_card",
		Attempt:        1,
		IdempotencyKey: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
	}

	config := map[string]interface{}{
		"url":    server.URL,
		"method": "POST",
	}

	out, err := conn.Execute(context.Background(), execCtx, config, map[string]interface{}{"amount": 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedIdempKey != "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" {
		t.Fatalf("expected Idempotency-Key '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08', got '%s'", receivedIdempKey)
	}

	if out["status_code"] != 200 {
		t.Fatalf("expected status_code 200, got %v", out["status_code"])
	}
	body, ok := out["body"].(map[string]interface{})
	if !ok || body["status"] != "ok" {
		t.Fatalf("expected body['status'] 'ok', got %v", out["body"])
	}
}
