package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// DurableTimerScheduler coordinates background polling and triggering of persistent sleep / delay timers.
type DurableTimerScheduler struct {
	engine *Engine
	store  storage.Store
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDurableTimerScheduler creates a new scheduler for durable timers.
func NewDurableTimerScheduler(engine *Engine, store storage.Store) *DurableTimerScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &DurableTimerScheduler{
		engine: engine,
		store:  store,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins background polling loop.
func (s *DurableTimerScheduler) Start() {
	s.wg.Add(1)
	go s.runLoop()
	slog.Info("durable timer scheduler started")
}

// Stop gracefully stops the timer scheduler.
func (s *DurableTimerScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
	slog.Info("durable timer scheduler stopped")
}

func (s *DurableTimerScheduler) runLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			s.pollAndFireDueTimers(now)
		}
	}
}

func (s *DurableTimerScheduler) pollAndFireDueTimers(now time.Time) {
	dueTimers, err := s.store.Timers().ListDue(s.ctx, now)
	if err != nil || len(dueTimers) == 0 {
		return
	}

	for _, timer := range dueTimers {
		nowFired := time.Now()
		_ = s.store.Timers().UpdateStatus(s.ctx, timer.ID, model.TimerStatusFired, &nowFired)
		slog.Info("firing due durable timer",
			slog.String("timer_id", timer.ID),
			slog.String("execution_id", timer.ExecutionID),
			slog.String("stage_id", timer.StageID),
		)

		go func(t *model.DurableTimer) {
			bgCtx := context.Background()
			_ = s.engine.ResumeTimer(bgCtx, t)
		}(timer)
	}
}
