package transform

import (
	"context"
	"fmt"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// TransformConnector executes payload transformations, calculations, and mappings.
type TransformConnector struct{}

// NewTransformConnector creates a new Transform connector.
func NewTransformConnector() *TransformConnector {
	return &TransformConnector{}
}

func (c *TransformConnector) Type() model.ConnectorType {
	return model.ConnectorTypeTransform
}

func (c *TransformConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeTransform,
		Name:        "Data Transformation & Filter",
		Description: "Transform, enrich, map, calculate or filter data using powerful Golang expressions",
		Category:    "compute",
		Icon:        "Cpu",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"expression": map[string]interface{}{"type": "string", "description": "Single Go expression (e.g. payload.amount * 1.1)"},
				"mapping":    map[string]interface{}{"type": "object", "description": "Key-expression mapping dictionary"},
			},
		},
	}
}

func (c *TransformConnector) Validate(config map[string]interface{}) error {
	exprStr, hasExpr := config["expression"].(string)
	_, hasMapping := config["mapping"].(map[string]interface{})
	if (!hasExpr || strings.TrimSpace(exprStr) == "") && !hasMapping {
		return fmt.Errorf("transform connector: either 'expression' or 'mapping' is required")
	}
	return nil
}

func (c *TransformConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	env := map[string]interface{}{
		"payload":   execCtx.Payload,
		"variables": execCtx.Variables,
		"steps":     execCtx.Steps,
		"input":     input,
	}

	result := make(map[string]interface{})

	// 1. Evaluate single expression if provided
	if exprStr, ok := config["expression"].(string); ok && strings.TrimSpace(exprStr) != "" {
		program, err := expr.Compile(exprStr, expr.Env(env))
		if err != nil {
			return nil, fmt.Errorf("failed to compile transform expression '%s': %w", exprStr, err)
		}
		val, err := expr.Run(program, env)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate transform expression '%s': %w", exprStr, err)
		}

		if m, isMap := val.(map[string]interface{}); isMap {
			result = m
		} else {
			result["result"] = val
		}
	}

	// 2. Evaluate mapping dictionary if provided
	if mapping, ok := config["mapping"].(map[string]interface{}); ok {
		for key, rawExpr := range mapping {
			exprStr, isStr := rawExpr.(string)
			if !isStr {
				result[key] = rawExpr
				continue
			}

			program, err := expr.Compile(exprStr, expr.Env(env))
			if err != nil {
				// If not a valid expr program, treat as static string
				result[key] = exprStr
				continue
			}

			val, err := expr.Run(program, env)
			if err != nil {
				result[key] = exprStr
			} else {
				result[key] = val
			}
		}
	}

	return result, nil
}
