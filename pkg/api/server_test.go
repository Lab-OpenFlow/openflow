package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/api"
	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connTransform "github.com/Lab-OpenFlow/openflow/pkg/connectors/transform"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestAPIServerEndpoints(t *testing.T) {
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())
	eng := engine.NewEngine(store, registry)

	server := api.NewServer(api.ServerConfig{}, eng, store, registry)
	router := server.Router()

	// 1. Test Health Check
	reqHealth, _ := http.NewRequest("GET", "/health", nil)
	recHealth := httptest.NewRecorder()
	router.ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /health, got %d", recHealth.Code)
	}

	// 2. Test Create Workflow via REST API
	wf := model.Workflow{
		Version: "v1",
		ID:      "api-test-wf",
		Name:    "API Test Workflow",
		Status:  model.WorkflowStatusActive,
		Trigger: &model.TriggerConfig{
			Type: model.TriggerTypeWebhook,
			Path: "/webhooks/pix-callbacks",
		},
		StartAt: "s1",
		Stages: []model.Stage{
			{
				ID:   "s1",
				Name: "Step 1",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"echo": "payload.msg",
					},
				},
			},
		},
	}
	wfBytes, _ := json.Marshal(wf)
	reqCreate, _ := http.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(wfBytes))
	reqCreate.Header.Set("Content-Type", "application/json")
	recCreate := httptest.NewRecorder()
	router.ServeHTTP(recCreate, reqCreate)
	if recCreate.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for POST /api/v1/workflows, got %d: %s", recCreate.Code, recCreate.Body.String())
	}

	// 3. Test Execute Workflow via REST API
	payload := map[string]interface{}{"msg": "Hello OpenFlow"}
	pBytes, _ := json.Marshal(payload)
	reqExec, _ := http.NewRequest("POST", "/api/v1/workflows/api-test-wf/execute", bytes.NewReader(pBytes))
	reqExec.Header.Set("Content-Type", "application/json")
	recExec := httptest.NewRecorder()
	router.ServeHTTP(recExec, reqExec)
	if recExec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for execute, got %d: %s", recExec.Code, recExec.Body.String())
	}

	// 4. Test Inbound Webhook Ingestion Router
	whPayload := map[string]interface{}{"msg": "Webhook Event Fired", "tx_id": "TX-998"}
	whBytes, _ := json.Marshal(whPayload)
	reqWH, _ := http.NewRequest("POST", "/api/v1/webhooks/pix-callbacks", bytes.NewReader(whBytes))
	reqWH.Header.Set("Content-Type", "application/json")
	reqWH.Header.Set("X-Idempotency-Key", "idemp-wh-12345")
	recWH := httptest.NewRecorder()
	router.ServeHTTP(recWH, reqWH)
	if recWH.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for Webhook trigger, got %d: %s", recWH.Code, recWH.Body.String())
	}

	var whResp map[string]interface{}
	_ = json.Unmarshal(recWH.Body.Bytes(), &whResp)
	if whResp["status"] != "ACCEPTED" || whResp["workflow_id"] != "api-test-wf" {
		t.Fatalf("expected webhook response to have status ACCEPTED and workflow_id api-test-wf, got: %v", whResp)
	}

	// Wait for execution completion
	time.Sleep(150 * time.Millisecond)

	// 5. Test List Workflow Versions
	reqVer, _ := http.NewRequest("GET", "/api/v1/workflows/api-test-wf/versions", nil)
	recVer := httptest.NewRecorder()
	router.ServeHTTP(recVer, reqVer)
	if recVer.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for workflow versions, got %d", recVer.Code)
	}
}

func TestServerHashRingShardedExecution(t *testing.T) {
	store := storage.NewMemoryStore()
	registry := connectors.NewRegistry()
	registry.Register(connTransform.NewTransformConnector())
	eng := engine.NewEngine(store, registry)

	// Create workflow
	wf := &model.Workflow{
		ID:      "wf-sharded-test",
		Name:    "Sharded Workflow",
		Status:  model.WorkflowStatusActive,
		StartAt: "s1",
		Stages: []model.Stage{
			{
				ID:   "s1",
				Name: "Step 1",
				Type: model.StageTypeTransform,
				Config: map[string]interface{}{
					"mapping": map[string]interface{}{
						"echo": "payload.msg",
					},
				},
			},
		},
	}
	_ = store.Workflows().Create(context.Background(), wf)

	ring := engine.NewHashRing(50)
	ring.AddNode("node-alpha")
	ring.AddNode("node-beta")

	server := api.NewServer(api.ServerConfig{
		NodeID:   "node-alpha",
		HashRing: ring,
	}, eng, store, registry)
	router := server.Router()

	// Execute workflow on local owner node
	execReq, _ := http.NewRequest("POST", "/api/v1/workflows/wf-sharded-test/execute", bytes.NewReader([]byte(`{"msg":"sharded test"}`)))
	execReq.Header.Set("Content-Type", "application/json")
	execReq.Header.Set("X-Idempotency-Key", "idemp-shard-1")
	execReq.Header.Set("X-OpenFlow-Sharded", "true")

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, execReq)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	if rec.Header().Get("X-OpenFlow-Sharded") != "true" {
		t.Fatalf("expected X-OpenFlow-Sharded header to be 'true'")
	}
	if rec.Header().Get("X-OpenFlow-Node") != "node-alpha" {
		t.Fatalf("expected X-OpenFlow-Node to be 'node-alpha', got '%s'", rec.Header().Get("X-OpenFlow-Node"))
	}
}
