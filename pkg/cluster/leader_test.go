package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaderElectorInMemoryFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var electedCount int32
	var revokedCount int32

	elector := NewLeaderElector(nil, 12345)

	elector.Start(ctx, func(c context.Context) {
		atomic.AddInt32(&electedCount, 1)
	}, func() {
		atomic.AddInt32(&revokedCount, 1)
	})

	time.Sleep(100 * time.Millisecond)

	if !elector.IsLeader() {
		t.Fatalf("expected elector to acquire leadership on standalone node")
	}

	if atomic.LoadInt32(&electedCount) != 1 {
		t.Fatalf("expected electedCount = 1, got %d", electedCount)
	}

	elector.Stop()

	if elector.IsLeader() {
		t.Fatalf("expected elector to release leadership after stop")
	}
}
