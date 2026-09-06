package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// OTelContextHandler is an slog.Handler middleware that enriches structured logs with OpenTelemetry trace_id and span_id.
type OTelContextHandler struct {
	handler slog.Handler
}

// NewOTelContextHandler wraps a base slog.Handler with OpenTelemetry correlation.
func NewOTelContextHandler(h slog.Handler) *OTelContextHandler {
	return &OTelContextHandler{handler: h}
}

func (h *OTelContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *OTelContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		span := trace.SpanFromContext(ctx)
		if span.SpanContext().IsValid() {
			r.AddAttrs(
				slog.String("trace_id", span.SpanContext().TraceID().String()),
				slog.String("span_id", span.SpanContext().SpanID().String()),
			)
		}
	}
	return h.handler.Handle(ctx, r)
}

func (h *OTelContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &OTelContextHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *OTelContextHandler) WithGroup(name string) slog.Handler {
	return &OTelContextHandler{handler: h.handler.WithGroup(name)}
}

// InitLogger initializes the global slog structured logger based on environment variables.
func InitLogger() *slog.Logger {
	levelStr := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	var level slog.Level
	switch levelStr {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: false,
	}

	format := strings.ToLower(os.Getenv("LOG_FORMAT"))
	var baseHandler slog.Handler
	if format == "json" || os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		baseHandler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		baseHandler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(NewOTelContextHandler(baseHandler))
	slog.SetDefault(logger)
	return logger
}
