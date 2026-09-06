package wasm

import (
	"context"
	"fmt"
	"strings"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/wasm"
)

// WASMConnector executes WebAssembly (WASM) binary plugins in a sandboxed, zero-trust runtime.
type WASMConnector struct{}

// NewWASMConnector creates a new WASM connector.
func NewWASMConnector() *WASMConnector {
	return &WASMConnector{}
}

func (c *WASMConnector) Type() model.ConnectorType {
	return model.ConnectorTypeWasm
}

func (c *WASMConnector) Descriptor() model.ConnectorDescriptor {
	return model.ConnectorDescriptor{
		Type:        model.ConnectorTypeWasm,
		Name:        "WebAssembly (WASM) Plugin",
		Description: "Execute high-performance, sandboxed plugins compiled from Rust, Go, TypeScript, or C to WebAssembly",
		Category:    "compute",
		Icon:        "Cpu",
		Version:     "1.0.0",
		ConfigSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"file":        map[string]interface{}{"type": "string", "description": "Path to .wasm file on disk"},
				"base64":      map[string]interface{}{"type": "string", "description": "Base64 encoded .wasm binary"},
				"function":    map[string]interface{}{"type": "string", "description": "Exported function name to call"},
				"timeout":     map[string]interface{}{"type": "string", "description": "Execution timeout (e.g. 5s)"},
				"max_memory":  map[string]interface{}{"type": "integer", "description": "Maximum memory limit in MB"},
			},
		},
	}
}

func (c *WASMConnector) Validate(config map[string]interface{}) error {
	file, _ := config["file"].(string)
	b64, _ := config["base64"].(string)

	if strings.TrimSpace(file) == "" && strings.TrimSpace(b64) == "" {
		return fmt.Errorf("wasm connector: either 'file' or 'base64' wasm binary must be provided")
	}
	return nil
}

func (c *WASMConnector) Execute(ctx context.Context, execCtx *connectors.ExecutionContext, config map[string]interface{}, input map[string]interface{}) (map[string]interface{}, error) {
	file, _ := config["file"].(string)
	b64, _ := config["base64"].(string)
	fnName, _ := config["function"].(string)
	if fnName == "" {
		fnName = "process"
	}

	var inst *wasm.WASMInstance
	var err error

	if b64 != "" {
		inst, err = wasm.NewWASMInstanceFromBase64(b64)
	} else if file != "" {
		inst, err = wasm.NewWASMInstanceFromFile(file)
	} else {
		return nil, fmt.Errorf("missing wasm binary source")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to instantiate wasm sandbox: %w", err)
	}

	return inst.Execute(ctx, fnName, input)
}
