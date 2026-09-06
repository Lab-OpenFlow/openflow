package observability_test

import (
	"context"
	"testing"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
	"go.opentelemetry.io/otel"
)

func TestW3CTraceContextPropagation(t *testing.T) {
	ctx := context.Background()
	_, err := observability.InitTracer(ctx, "test-tracer-service")
	if err != nil {
		t.Fatalf("failed to init tracer: %v", err)
	}

	tr := otel.GetTracerProvider().Tracer("test")
	spanCtx, span := tr.Start(ctx, "test-parent-operation")
	defer span.End()

	// Inject W3C Trace Headers
	headers := observability.InjectW3CTraceHeaders(spanCtx)
	if headers["traceparent"] == "" {
		t.Fatalf("expected non-empty traceparent header, got: %v", headers)
	}

	// Extract W3C Trace Context
	extractedCtx := observability.ExtractW3CTraceContext(context.Background(), headers)
	if extractedCtx == nil {
		t.Fatalf("expected non-nil extracted context")
	}
}
