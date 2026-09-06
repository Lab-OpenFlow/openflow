package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/Lab-OpenFlow/openflow/pkg/observability"
	"go.opentelemetry.io/otel"
)

func TestStructuredLoggingWithOTelCorrelation(t *testing.T) {
	var buf bytes.Buffer
	baseHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	otelHandler := observability.NewOTelContextHandler(baseHandler)
	logger := slog.New(otelHandler)

	ctx := context.Background()
	_, _ = observability.InitTracer(ctx, "logger-test-service")

	tr := otel.GetTracerProvider().Tracer("test-logger")
	spanCtx, span := tr.Start(ctx, "test-span-operation")
	defer span.End()

	logger.InfoContext(spanCtx, "workflow stage processed",
		slog.String("workflow_id", "order_pipeline"),
		slog.Int("stage_duration_ms", 45),
	)

	var logMap map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
		t.Fatalf("failed to parse structured JSON log: %v (raw: %s)", err, buf.String())
	}

	if logMap["msg"] != "workflow stage processed" {
		t.Fatalf("expected message 'workflow stage processed', got: %v", logMap["msg"])
	}

	if logMap["trace_id"] == nil || logMap["trace_id"] == "" {
		t.Fatalf("expected non-empty trace_id in structured log record, got: %v", logMap)
	}

	if logMap["span_id"] == nil || logMap["span_id"] == "" {
		t.Fatalf("expected non-empty span_id in structured log record, got: %v", logMap)
	}
}
