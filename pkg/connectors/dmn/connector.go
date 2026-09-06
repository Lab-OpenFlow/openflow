package dmn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/engine/dmn"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// DMNConnector evaluates declarative Decision Model and Notation (DMN) decision tables.
type DMNConnector struct{}

func NewDMNConnector() *DMNConnector {
	return &DMNConnector{}
}

func (c *DMNConnector) Type() model.ConnectorType {
	return model.ConnectorTypeDMN
}

func (c *DMNConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeDMN,
		Name:        "DMN Decision Table Engine",
		Description: "Evaluates business decision tables with configurable hit policies (first, collect, rule_order)",
		Category:    "compute",
		Icon:        "table",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"hit_policy": map[string]interface{}{
				"type": "string",
				"enum": []string{"first", "collect", "rule_order"},
			},
			"inputs": map[string]interface{}{
				"type": "array",
			},
			"outputs": map[string]interface{}{
				"type": "array",
			},
			"rules": map[string]interface{}{
				"type": "array",
			},
		},
	}
}

func (c *DMNConnector) Validate(config map[string]interface{}) error {
	rulesVal, ok := config["rules"]
	if !ok || rulesVal == nil {
		return errors.New("dmn connector: 'rules' array is required in configuration")
	}
	return nil
}

func (c *DMNConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("dmn connector: failed to serialize config: %w", err)
	}

	var dt dmn.DecisionTable
	if err := json.Unmarshal(configBytes, &dt); err != nil {
		return nil, fmt.Errorf("dmn connector: invalid decision table schema: %w", err)
	}

	// Build evaluation state combining payload, variables and previous step outputs
	state := map[string]interface{}{
		"payload":   input,
		"variables": execCtx.Variables,
		"steps":     execCtx.Steps,
	}
	for k, v := range input {
		state[k] = v
	}

	result, err := dt.Evaluate(state)
	if err != nil {
		return nil, fmt.Errorf("dmn connector evaluation error: %w", err)
	}

	output := map[string]interface{}{
		"hit_rules": result.HitRules,
		"outputs":   result.Outputs,
	}
	for k, v := range result.Outputs {
		output[k] = v
	}
	if len(result.AllHits) > 0 {
		output["all_hits"] = result.AllHits
	}

	return output, nil
}
