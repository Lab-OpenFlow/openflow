package cluster

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// LeaderElector coordinates leadership election across multi-replica pods
// using PostgreSQL Advisory Locks (pg_try_advisory_lock).
type LeaderElector struct {
	mu          sync.RWMutex
	nodeID      string
	db          *sql.DB
	conn        *sql.Conn
	isLeader    bool
	lockID      int64
	onElected   func(ctx context.Context)
	onRevoked   func()
	cancelElect context.CancelFunc
}

// NewLeaderElector creates a new LeaderElector instance.
func NewLeaderElector(db *sql.DB, lockID int64) *LeaderElector {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node-" + uuid.New().String()[:8]
	}

	if lockID == 0 {
		lockID = 884920481 // Deterministic 64-bit int for OpenFlow cluster
	}

	return &LeaderElector{
		nodeID:   hostname,
		db:       db,
		lockID:   lockID,
		isLeader: false,
	}
}

// Start begins background leader election loop.
func (l *LeaderElector) Start(ctx context.Context, onElected func(ctx context.Context), onRevoked func()) {
	l.onElected = onElected
	l.onRevoked = onRevoked

	electCtx, cancel := context.WithCancel(ctx)
	l.cancelElect = cancel

	// Immediate election check on startup
	l.tryAcquireOrMaintain(electCtx)

	go l.runElectionLoop(electCtx)
}

// Stop gracefully releases leadership and stops election loop.
func (l *LeaderElector) Stop() {
	if l.cancelElect != nil {
		l.cancelElect()
	}
	l.releaseLeadership()
}

// IsLeader reports whether this node is currently the elected cluster leader.
func (l *LeaderElector) IsLeader() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isLeader
}

// NodeID returns this replica's identifier.
func (l *LeaderElector) NodeID() string {
	return l.nodeID
}

func (l *LeaderElector) runElectionLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.releaseLeadership()
			return
		case <-ticker.C:
			l.tryAcquireOrMaintain(ctx)
		}
	}
}

func (l *LeaderElector) tryAcquireOrMaintain(ctx context.Context) {
	if l.db == nil {
		// Single-node in-memory fallback
		if !l.IsLeader() {
			l.setLeadership(true, ctx)
		}
		return
	}

	l.mu.Lock()
	currentConn := l.conn
	isLeader := l.isLeader
	l.mu.Unlock()

	if !isLeader {
		// Obtain a dedicated session connection from the pool for advisory lock ownership
		conn, err := l.db.Conn(ctx)
		if err != nil {
			slog.WarnContext(ctx, "failed to obtain dedicated connection for leader election",
				slog.String("node_id", l.nodeID),
				slog.String("error", err.Error()),
			)
			return
		}

		var acquired bool
		err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", l.lockID).Scan(&acquired)
		if err != nil {
			_ = conn.Close()
			slog.WarnContext(ctx, "advisory lock check failed",
				slog.String("node_id", l.nodeID),
				slog.String("error", err.Error()),
			)
			return
		}

		if acquired {
			l.mu.Lock()
			l.conn = conn
			l.mu.Unlock()
			slog.InfoContext(ctx, "cluster leadership acquired",
				slog.String("node_id", l.nodeID),
				slog.Int64("lock_id", l.lockID),
			)
			l.setLeadership(true, ctx)
		} else {
			// Lock held by another node
			_ = conn.Close()
		}
		return
	}

	// Verify session connection
	if currentConn == nil {
		slog.WarnContext(ctx, "cluster leadership lost, connection is nil",
			slog.String("node_id", l.nodeID),
		)
		l.setLeadership(false, ctx)
		return
	}

	var ping int
	err := currentConn.QueryRowContext(ctx, "SELECT 1").Scan(&ping)
	if err != nil {
		slog.WarnContext(ctx, "cluster leadership lost, session severed",
			slog.String("node_id", l.nodeID),
			slog.String("error", err.Error()),
		)
		l.mu.Lock()
		_ = currentConn.Close()
		l.conn = nil
		l.mu.Unlock()
		l.setLeadership(false, ctx)
	}
}

func (l *LeaderElector) setLeadership(leader bool, ctx context.Context) {
	l.mu.Lock()
	l.isLeader = leader
	l.mu.Unlock()

	if leader && l.onElected != nil {
		l.onElected(ctx)
	} else if !leader && l.onRevoked != nil {
		l.onRevoked()
	}
}

func (l *LeaderElector) releaseLeadership() {
	l.mu.Lock()
	wasLeader := l.isLeader
	l.isLeader = false
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()

	if wasLeader && conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", l.lockID)
		_ = conn.Close()
	}

	if wasLeader && l.onRevoked != nil {
		l.onRevoked()
	}
}
