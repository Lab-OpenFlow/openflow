package engine

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// RecoveryWatchdog monitors and resumes stalled executions across worker nodes.
type RecoveryWatchdog struct {
	engine           *Engine
	store            storage.Store
	workerID         string
	stalledThreshold time.Duration
}

// NewRecoveryWatchdog creates a new Crash Recovery Watchdog.
func NewRecoveryWatchdog(engine *Engine, store storage.Store) *RecoveryWatchdog {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "watchdog-" + uuid.New().String()[:8]
	}
	return &RecoveryWatchdog{
		engine:           engine,
		store:            store,
		workerID:         hostname,
		stalledThreshold: 1 * time.Minute,
	}
}

// WithStalledThreshold overrides default stalled threshold (useful in tests).
func (w *RecoveryWatchdog) WithStalledThreshold(d time.Duration) *RecoveryWatchdog {
	if d > 0 {
		w.stalledThreshold = d
	}
	return w
}

// WithWorkerID overrides worker identifier.
func (w *RecoveryWatchdog) WithWorkerID(id string) *RecoveryWatchdog {
	if id != "" {
		w.workerID = id
	}
	return w
}

// RecoverInterruptedExecutions claims and deterministically resumes only truly stalled executions.
func (w *RecoveryWatchdog) RecoverInterruptedExecutions(ctx context.Context) (int, error) {
	claimedExecutions, err := w.store.Executions().ClaimStalledExecutions(ctx, w.workerID, w.stalledThreshold, 100)
	if err != nil {
		return 0, err
	}

	if len(claimedExecutions) == 0 {
		return 0, nil
	}

	slog.InfoContext(ctx, "watchdog claimed stalled executions",
		slog.String("worker_id", w.workerID),
		slog.Int("count", len(claimedExecutions)),
	)
	recoveredCount := 0

	for _, exec := range claimedExecutions {
		_, err := w.engine.ResumeExecution(ctx, exec.ID)
		if err != nil {
			slog.ErrorContext(ctx, "watchdog failed to recover execution",
				slog.String("execution_id", exec.ID),
				slog.String("error", err.Error()),
			)
			continue
		}
		recoveredCount++
	}

	slog.InfoContext(ctx, "watchdog queued executions for resumption",
		slog.Int("recovered", recoveredCount),
		slog.Int("total", len(claimedExecutions)),
	)
	return recoveredCount, nil
}

// Start launches a periodic background recovery loop.
func (w *RecoveryWatchdog) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// Run initial sweep immediately on startup
	_, _ = w.RecoverInterruptedExecutions(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = w.RecoverInterruptedExecutions(ctx)
		}
	}
}
