package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/engine"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

func TestTransactionalOutboxRelay(t *testing.T) {
	memStore := storage.NewMemoryStore()

	var dispatchedMu sync.Mutex
	var dispatched []model.WorkflowEvent

	relay := engine.NewOutboxRelay(memStore, func(evt model.WorkflowEvent) {
		dispatchedMu.Lock()
		defer dispatchedMu.Unlock()
		dispatched = append(dispatched, evt)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Push 3 events to Outbox
	now := time.Now()
	for i := 1; i <= 3; i++ {
		err := memStore.Outbox().Push(ctx, &model.WorkflowEvent{
			Type:        model.EventStepCompleted,
			ExecutionID: "exec_outbox_test",
			WorkflowID:  "wf_outbox",
			StageID:     "stage_outbox",
			Timestamp:   now.Add(time.Duration(i) * time.Millisecond),
			Payload:     map[string]interface{}{"idx": i},
		})
		if err != nil {
			t.Fatalf("failed to push event to outbox: %v", err)
		}
	}

	// Verify pending count
	pending, err := memStore.Outbox().FetchPending(ctx, 10)
	if err != nil || len(pending) != 3 {
		t.Fatalf("expected 3 pending events, got %d (err: %v)", len(pending), err)
	}

	// Start relay and let it drain
	relay.Start(ctx)
	defer relay.Stop()

	// Wait for drain to process
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dispatchedMu.Lock()
		count := len(dispatched)
		dispatchedMu.Unlock()
		if count >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	dispatchedMu.Lock()
	finalCount := len(dispatched)
	dispatchedMu.Unlock()

	if finalCount != 3 {
		t.Fatalf("expected 3 dispatched events, got %d", finalCount)
	}

	// Verify that subsequent fetch has 0 pending
	remaining, err := memStore.Outbox().FetchPending(ctx, 10)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("expected 0 remaining pending events, got %d", len(remaining))
	}
}
