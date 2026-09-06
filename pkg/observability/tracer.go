package observability

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var Tracer trace.Tracer

// InitTracer initializes OpenTelemetry Tracer Provider with OTLP exporter for Jaeger/OTel Collector.
func InitTracer(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	if serviceName == "" {
		serviceName = "openflow-orchestrator"
	}

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// If running in docker or local
		if os.Getenv("KAFKA_BROKERS") != "" {
			endpoint = "jaeger:4318"
		} else {
			endpoint = "localhost:4318"
		}
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		slog.Warn("failed to create OTLP trace exporter", slog.String("endpoint", endpoint), slog.String("error", err.Error()))
		// Return no-op tracer provider if endpoint not reachable
		tp := sdktrace.NewTracerProvider()
		otel.SetTracerProvider(tp)
		Tracer = tp.Tracer(serviceName)
		return tp, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("environment", "production"),
			attribute.String("engine.version", "1.0.0"),
		),
	)
	if err != nil {
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	Tracer = tp.Tracer(serviceName)
	slog.Info("opentelemetry tracing initialized", slog.String("endpoint", endpoint))

	return tp, nil
}

// InjectW3CTraceHeaders injects the current trace context (traceparent, tracestate) into a string map.
func InjectW3CTraceHeaders(ctx context.Context) map[string]string {
	headers := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	return headers
}

// ExtractW3CTraceContext extracts trace context from inbound headers and returns a linked context.
func ExtractW3CTraceContext(ctx context.Context, headers map[string]string) context.Context {
	if headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}
