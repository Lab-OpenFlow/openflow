package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// OutboxRelay periodically polls outbox_events and dispatches pending events.
type OutboxRelay struct {
	store      storage.Store
	onDispatch func(event model.WorkflowEvent)
	interval   time.Duration
	stopChan   chan struct{}
}

// NewOutboxRelay creates a new outbox relay worker.
func NewOutboxRelay(store storage.Store, onDispatch func(event model.WorkflowEvent)) *OutboxRelay {
	return &OutboxRelay{
		store:      store,
		onDispatch: onDispatch,
		interval:   200 * time.Millisecond,
		stopChan:   make(chan struct{}),
	}
}

// Start launches the background relay drain loop.
func (r *OutboxRelay) Start(ctx context.Context) {
	go r.drainLoop(ctx)
	slog.InfoContext(ctx, "outbox relay started")
}

// Stop stops the background relay worker.
func (r *OutboxRelay) Stop() {
	select {
	case <-r.stopChan:
	default:
		close(r.stopChan)
	}
}

func (r *OutboxRelay) drainLoop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-ticker.C:
			r.drainBatch(ctx)
		}
	}
}

func (r *OutboxRelay) drainBatch(ctx context.Context) {
	events, err := r.store.Outbox().FetchPending(ctx, 50)
	if err != nil || len(events) == 0 {
		return
	}

	var processedIDs []string
	for _, evt := range events {
		if r.onDispatch != nil {
			r.onDispatch(*evt)
		}
		processedIDs = append(processedIDs, fmt.Sprintf("obx_%s_%d", evt.ExecutionID, evt.Timestamp.UnixNano()))
	}

	if len(processedIDs) > 0 {
		_ = r.store.Outbox().MarkProcessed(ctx, processedIDs)
	}
}
