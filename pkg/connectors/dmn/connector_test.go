package dmn_test

import (
	"context"
	"testing"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	connDMN "github.com/Lab-OpenFlow/openflow/pkg/connectors/dmn"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

func TestDMNConnector_Execute(t *testing.T) {
	c := connDMN.NewDMNConnector()

	if c.Type() != model.ConnectorTypeDMN {
		t.Fatalf("expected type %s, got %s", model.ConnectorTypeDMN, c.Type())
	}

	config := map[string]interface{}{
		"hit_policy": "first",
		"inputs": []interface{}{
			map[string]interface{}{"name": "amount", "expression": "amount"},
		},
		"outputs": []interface{}{
			map[string]interface{}{"name": "status"},
		},
		"rules": []interface{}{
			map[string]interface{}{
				"id":         "r1",
				"conditions": map[string]interface{}{"amount": "> 1000"},
				"outputs":    map[string]interface{}{"status": "APPROVED_VP"},
			},
			map[string]interface{}{
				"id":         "r2",
				"conditions": map[string]interface{}{"amount": "<= 1000"},
				"outputs":    map[string]interface{}{"status": "APPROVED_AUTO"},
			},
		},
	}

	if err := c.Validate(config); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	execCtx := &connectors.ExecutionContext{
		ExecutionID: "exec-123",
		WorkflowID:  "wf-dmn",
		Variables:   map[string]interface{}{},
		Steps:       map[string]interface{}{},
	}

	out, err := c.Execute(context.Background(), execCtx, config, map[string]interface{}{"amount": 2500})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if out["status"] != "APPROVED_VP" {
		t.Fatalf("expected status APPROVED_VP, got %v", out["status"])
	}
}
