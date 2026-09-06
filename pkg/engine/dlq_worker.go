package engine

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// DLQWorker continuously polls the Dead Letter Queue and re-executes failed
// workflows using exponential backoff. Each batch is processed every 15 seconds.
//
// Backoff schedule (baseBackoff = 30s):
//
//	attempt 1 → 30s,  attempt 2 → 60s,  attempt 3 → 120s,
//	attempt 4 → 240s, attempt 5+ → maxBackoff (24h)
type DLQWorker struct {
	engine      ExecutableEngine
	store       storage.Store
	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	stopChan    chan struct{}
}

// ExecutableEngine is the minimal interface DLQWorker needs from the engine,
// allowing easy substitution in tests without importing the full Engine type.
type ExecutableEngine interface {
	Execute(ctx context.Context, workflowID string, input map[string]interface{}, triggerType model.TriggerType) (*model.Execution, error)
}

// NewDLQWorker creates a DLQ background processor with production-safe defaults.
func NewDLQWorker(engine ExecutableEngine, store storage.Store) *DLQWorker {
	return &DLQWorker{
		engine:      engine,
		store:       store,
		maxAttempts: 5,
		baseBackoff: 30 * time.Second,
		maxBackoff:  24 * time.Hour,
		stopChan:    make(chan struct{}),
	}
}

// Start launches the background processing goroutine.
// It will stop when ctx is cancelled or Close() is called.
func (w *DLQWorker) Start(ctx context.Context) {
	go w.processLoop(ctx)
	slog.InfoContext(ctx, "dlq worker started",
		slog.Duration("interval", 15*time.Second),
		slog.Int("max_attempts", w.maxAttempts),
	)
}

func (w *DLQWorker) processLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *DLQWorker) processBatch(ctx context.Context) {
	msgs, err := w.store.DLQ().ListPending(ctx, 20)
	if err != nil {
		slog.WarnContext(ctx, "failed to list pending DLQ messages", slog.String("error", err.Error()))
		return
	}
	if len(msgs) == 0 {
		return
	}

	for _, msg := range msgs {
		w.processMessage(ctx, msg)
	}
}

func (w *DLQWorker) processMessage(ctx context.Context, msg *model.DLQMessage) {
	referenceTime := msg.CreatedAt
	if msg.LastAttemptAt != nil {
		referenceTime = *msg.LastAttemptAt
	}
	backoff := w.calculateBackoff(msg.Attempts)
	if time.Since(referenceTime) < backoff {
		return
	}

	if msg.Attempts >= w.maxAttempts {
		_ = w.store.DLQ().MarkDead(ctx, msg.ID, "max_attempts_exceeded")
		observability.GlobalMetrics.DLQMessagesRetried.WithLabelValues(msg.WorkflowID, "dead").Inc()
		slog.WarnContext(ctx, "dlq message marked dead",
			slog.String("dlq_id", msg.ID),
			slog.String("workflow_id", msg.WorkflowID),
			slog.Int("attempts", msg.Attempts),
		)
		return
	}

	_, execErr := w.engine.Execute(ctx, msg.WorkflowID, msg.Payload, model.TriggerTypeRetry)
	if execErr != nil {
		_ = w.store.DLQ().IncrementAttempts(ctx, msg.ID, execErr.Error())
		observability.GlobalMetrics.DLQMessagesRetried.WithLabelValues(msg.WorkflowID, "failed").Inc()
		slog.ErrorContext(ctx, "dlq retry failed",
			slog.String("dlq_id", msg.ID),
			slog.String("workflow_id", msg.WorkflowID),
			slog.Int("attempt", msg.Attempts+1),
			slog.String("error", execErr.Error()),
			slog.Duration("next_backoff", w.calculateBackoff(msg.Attempts+1)),
		)
		return
	}

	_ = w.store.DLQ().MarkResolved(ctx, msg.ID)
	observability.GlobalMetrics.DLQMessagesRetried.WithLabelValues(msg.WorkflowID, "success").Inc()
	slog.InfoContext(ctx, "dlq message replayed successfully",
		slog.String("dlq_id", msg.ID),
		slog.String("workflow_id", msg.WorkflowID),
		slog.Int("attempts_taken", msg.Attempts+1),
	)
}

// calculateBackoff returns the wait time before the next retry attempt.
// Formula: baseBackoff × 2^attempts, capped at maxBackoff.
func (w *DLQWorker) calculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return 0
	}
	backoff := float64(w.baseBackoff) * math.Pow(2, float64(attempts-1))
	if backoff > float64(w.maxBackoff) {
		return w.maxBackoff
	}
	return time.Duration(backoff)
}

// Close signals the worker to stop after the current batch finishes.
func (w *DLQWorker) Close() {
	close(w.stopChan)
}
