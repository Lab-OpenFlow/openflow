package wasm_test

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/wasm"
)

// Valid WebAssembly binary that exports: calculate() -> i32 (returns 42)
var validWASMBytecode = []byte{
	0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // Magic + Version
	0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,       // Type: () -> i32
	0x03, 0x02, 0x01, 0x00,                         // Function index
	0x07, 0x0d, 0x01, 0x09, 'c', 'a', 'l', 'c', 'u', 'l', 'a', 't', 'e', 0x00, 0x00, // Export "calculate"
	0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b, // Code: i32.const 42, end
}

func TestWazeroRealWASMExecution(t *testing.T) {
	ctx := context.Background()

	inst, err := wasm.NewWASMInstance(validWASMBytecode, wasm.WASMConfig{
		MaxMemoryPages: 256,
		Timeout:        2 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to compile real WASM with wazero: %v", err)
	}
	defer inst.Close(ctx)

	input := map[string]interface{}{
		"amount":   100.0,
		"currency": "BRL",
	}

	output, err := inst.Execute(ctx, "calculate", input)
	if err != nil {
		t.Fatalf("unexpected wasm execution error: %v", err)
	}

	if output["amount"] != 100.0 || output["currency"] != "BRL" {
		t.Fatalf("expected inputs preserved, got: %v", output)
	}

	if output["result"] != uint64(42) {
		t.Fatalf("expected real WASM calculation result 42, got %v", output["result"])
	}
}

func TestWazeroBase64Execution(t *testing.T) {
	ctx := context.Background()
	b64 := base64.StdEncoding.EncodeToString(validWASMBytecode)

	inst, err := wasm.NewWASMInstanceFromBase64(b64)
	if err != nil {
		t.Fatalf("failed to initialize wasm instance from base64: %v", err)
	}
	defer inst.Close(ctx)

	out, err := inst.Execute(ctx, "calculate", map[string]interface{}{"test": true})
	if err != nil {
		t.Fatalf("failed execution: %v", err)
	}

	if out["result"] != uint64(42) {
		t.Fatalf("expected result 42, got %v", out["result"])
	}
}
