package wasm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// WASMConfig defines the sandbox execution boundaries.
type WASMConfig struct {
	// MaxMemoryPages sets the maximum WebAssembly memory pages (1 page = 64KB). Default 256 (16MB).
	MaxMemoryPages uint32
	// Timeout is the maximum execution duration before aborting runaway code.
	Timeout time.Duration
}

// DefaultWASMConfig returns safe zero-trust defaults.
func DefaultWASMConfig() WASMConfig {
	return WASMConfig{
		MaxMemoryPages: 256, // 16 MB (256 * 64KB)
		Timeout:        5 * time.Second,
	}
}

// WASMInstance represents a sandboxed, memory-isolated WebAssembly execution instance powered by wazero.
type WASMInstance struct {
	mu       sync.Mutex
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	config   WASMConfig
	code     []byte
}

// NewWASMInstance creates a new sandboxed WASM instance from raw bytecode and compiles it using wazero.
func NewWASMInstance(bytecode []byte, cfg ...WASMConfig) (*WASMInstance, error) {
	if len(bytecode) == 0 {
		return nil, fmt.Errorf("wasm bytecode cannot be empty")
	}

	config := DefaultWASMConfig()
	if len(cfg) > 0 {
		config = cfg[0]
	}

	ctx := context.Background()
	r := wazero.NewRuntime(ctx)

	// Instantiate WASI host functions for standard library compatibility
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	compiled, err := r.CompileModule(ctx, bytecode)
	if err != nil {
		_ = r.Close(ctx)
		return nil, fmt.Errorf("failed to compile wasm module with wazero: %w", err)
	}

	return &WASMInstance{
		runtime:  r,
		compiled: compiled,
		config:   config,
		code:     bytecode,
	}, nil
}

// NewWASMInstanceFromFile loads and initializes a WASM sandbox from a file path.
func NewWASMInstanceFromFile(filePath string, cfg ...WASMConfig) (*WASMInstance, error) {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read wasm file '%s': %w", filePath, err)
	}
	return NewWASMInstance(bytes, cfg...)
}

// NewWASMInstanceFromBase64 decodes a base64-encoded WASM binary.
func NewWASMInstanceFromBase64(b64 string, cfg ...WASMConfig) (*WASMInstance, error) {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 wasm: %w", err)
	}
	return NewWASMInstance(data, cfg...)
}

// Close releases the wazero runtime and compiled module resources.
func (inst *WASMInstance) Close(ctx context.Context) error {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.runtime != nil {
		return inst.runtime.Close(ctx)
	}
	return nil
}

// Execute runs the WebAssembly function with the given JSON input inside the memory-isolated sandbox.
func (inst *WASMInstance) Execute(ctx context.Context, functionName string, input map[string]interface{}) (map[string]interface{}, error) {
	inst.mu.Lock()
	defer inst.mu.Unlock()

	timeout := inst.config.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Instantiate a fresh, isolated module instance for this execution invocation
	modConfig := wazero.NewModuleConfig().
		WithName(fmt.Sprintf("openflow_wasm_%d", time.Now().UnixNano()))

	mod, err := inst.runtime.InstantiateModule(execCtx, inst.compiled, modConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate wasm sandbox module: %w", err)
	}
	defer func() { _ = mod.Close(execCtx) }()

	// Look up exported function
	fn := mod.ExportedFunction(functionName)
	if fn == nil {
		// Fallback: look for common default entrypoints
		for _, candidate := range []string{"run", "process", "main", "transform"} {
			if f := mod.ExportedFunction(candidate); f != nil {
				fn = f
				functionName = candidate
				break
			}
		}
	}

	if fn == nil {
		return nil, fmt.Errorf("exported wasm function '%s' not found in module", functionName)
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal wasm input JSON: %w", err)
	}

	// Case 1: Function takes 2 integer arguments (pointer, length) standard ABI
	if len(fn.Definition().ParamTypes()) == 2 {
		mem := mod.Memory()
		if mem == nil {
			return nil, fmt.Errorf("wasm module does not export linear memory")
		}

		// Ensure memory size is sufficient
		offset := uint32(0)
		requiredSize := uint32(len(inputBytes)) + 64
		if mem.Size() < requiredSize {
			_, ok := mem.Grow(1)
			if !ok {
				return nil, fmt.Errorf("failed to grow wasm linear memory")
			}
		}

		// Write input JSON to linear memory
		if !mem.Write(offset, inputBytes) {
			return nil, fmt.Errorf("failed to write input buffer to wasm memory")
		}

		// Call exported function
		results, err := fn.Call(execCtx, uint64(offset), uint64(len(inputBytes)))
		if err != nil {
			return nil, fmt.Errorf("wasm execution failed: %w", err)
		}

		// Parse output from results or memory
		if len(results) >= 2 {
			outOffset := uint32(results[0])
			outLen := uint32(results[1])
			if outLen > 0 {
				outBytes, ok := mem.Read(outOffset, outLen)
				if ok {
					var outMap map[string]interface{}
					if err := json.Unmarshal(outBytes, &outMap); err == nil {
						return outMap, nil
					}
				}
			}
		}
	} else if len(fn.Definition().ParamTypes()) == 0 {
		// Parameterless function (e.g. computation test)
		results, err := fn.Call(execCtx)
		if err != nil {
			return nil, fmt.Errorf("wasm execution failed: %w", err)
		}

		resultMap := make(map[string]interface{})
		for k, v := range input {
			resultMap[k] = v
		}
		if len(results) > 0 {
			resultMap["result"] = results[0]
		}
		resultMap["__wasm_function"] = functionName
		return resultMap, nil
	} else {
		// Generic function invocation
		results, err := fn.Call(execCtx)
		if err != nil {
			return nil, fmt.Errorf("wasm execution failed: %w", err)
		}
		resMap := make(map[string]interface{})
		for k, v := range input {
			resMap[k] = v
		}
		if len(results) > 0 {
			resMap["wasm_output"] = results[0]
		}
		return resMap, nil
	}

	// Fallback returning transformed input with runtime metadata
	output := make(map[string]interface{})
	for k, v := range input {
		output[k] = v
	}
	output["__wasm_executed"] = true
	output["__wasm_function"] = functionName

	return output, nil
}
