package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

var (
	ErrTaskNotFound       = errors.New("task not found")
	ErrInvalidLockToken   = errors.New("invalid or expired task lock token")
	ErrTaskAlreadyDone    = errors.New("task already completed or failed")
	ErrMaxAttemptsReached = errors.New("task exceeded maximum execution attempts")
)

// TaskQueueManager coordinates distributed external worker task polling, leases, and heartbeats.
type TaskQueueManager struct {
	store      storage.Store
	mu         sync.RWMutex
	queues     map[string]*queueDispatcher
	stopChan   chan struct{}
	onComplete func(ctx context.Context, executionID, stageID string, output map[string]interface{}) error
	onFail     func(ctx context.Context, executionID, stageID string, errMsg string) error
}

type queueDispatcher struct {
	mu     sync.Mutex
	cond   *sync.Cond
	notify chan struct{}
}

func newQueueDispatcher() *queueDispatcher {
	qd := &queueDispatcher{
		notify: make(chan struct{}, 1),
	}
	qd.cond = sync.NewCond(&qd.mu)
	return qd
}

// NewTaskQueueManager creates a new distributed task queue manager.
func NewTaskQueueManager(
	store storage.Store,
	onComplete func(ctx context.Context, executionID, stageID string, output map[string]interface{}) error,
	onFail func(ctx context.Context, executionID, stageID string, errMsg string) error,
) *TaskQueueManager {
	tqm := &TaskQueueManager{
		store:      store,
		queues:     make(map[string]*queueDispatcher),
		stopChan:   make(chan struct{}),
		onComplete: onComplete,
		onFail:     onFail,
	}

	go tqm.reapExpiredLeasesLoop()
	return tqm
}

func (m *TaskQueueManager) getOrCreateDispatcher(queueName string) *queueDispatcher {
	m.mu.Lock()
	defer m.mu.Unlock()
	qd, exists := m.queues[queueName]
	if !exists {
		qd = newQueueDispatcher()
		m.queues[queueName] = qd
	}
	return qd
}

// Enqueue registers a new activity task in the specified queue and signals waiting workers.
func (m *TaskQueueManager) Enqueue(ctx context.Context, task *model.TaskItem) error {
	if task.ID == "" {
		task.ID = fmt.Sprintf("task_%s_%s", task.QueueName, uuid.New().String()[:8])
	}
	if task.HeartbeatTimeout == 0 {
		task.HeartbeatTimeout = 30 * time.Second
	}
	if task.MaxAttempts == 0 {
		task.MaxAttempts = 3
	}
	task.Status = model.TaskStatusPending
	task.CreatedAt = time.Now()

	if err := m.store.TaskQueues().Create(ctx, task); err != nil {
		return fmt.Errorf("failed to store task in queue: %w", err)
	}

	// Signal waiting workers
	qd := m.getOrCreateDispatcher(task.QueueName)
	select {
	case qd.notify <- struct{}{}:
	default:
	}

	return nil
}

// Poll fetches a pending task from the queue using non-blocking check + HTTP long-polling with lease lock.
func (m *TaskQueueManager) Poll(ctx context.Context, queueName, workerID string, timeout time.Duration) (*model.TaskItem, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	deadline := time.Now().Add(timeout)
	qd := m.getOrCreateDispatcher(queueName)

	for {
		// Attempt to acquire pending task
		task, err := m.tryAcquireNext(ctx, queueName, workerID)
		if err == nil && task != nil {
			return task, nil
		}

		// Wait for signal or timeout
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil // No task available within timeout (HTTP 204 No Content)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-m.stopChan:
			return nil, errors.New("task queue manager stopped")
		case <-qd.notify:
			// Task arrived, loop and try acquire
		case <-time.After(100 * time.Millisecond):
			// Small jittered fallback poll
		}
	}
}

func (m *TaskQueueManager) tryAcquireNext(ctx context.Context, queueName, workerID string) (*model.TaskItem, error) {
	task, err := m.store.TaskQueues().AcquirePending(ctx, queueName, workerID, 30*time.Second)
	if err != nil {
		return nil, err
	}
	return task, nil
}

// Heartbeat extends the lease expiration of an in-progress task.
func (m *TaskQueueManager) Heartbeat(ctx context.Context, taskID, workerID, lockToken string) error {
	task, err := m.store.TaskQueues().Get(ctx, taskID)
	if err != nil {
		return ErrTaskNotFound
	}
	if task.Status != model.TaskStatusRunning {
		return ErrTaskAlreadyDone
	}
	if task.LockToken != lockToken || task.WorkerID != workerID {
		return ErrInvalidLockToken
	}

	newLease := time.Now().Add(task.HeartbeatTimeout)
	return m.store.TaskQueues().ExtendLease(ctx, taskID, newLease)
}

// Complete acknowledges successful task completion and resumes the parent workflow.
func (m *TaskQueueManager) Complete(ctx context.Context, taskID, workerID, lockToken string, output map[string]interface{}) error {
	task, err := m.store.TaskQueues().Get(ctx, taskID)
	if err != nil {
		return ErrTaskNotFound
	}
	if task.Status != model.TaskStatusRunning {
		return ErrTaskAlreadyDone
	}
	if task.LockToken != lockToken || task.WorkerID != workerID {
		return ErrInvalidLockToken
	}

	now := time.Now()
	task.Status = model.TaskStatusCompleted
	task.Output = output
	task.CompletedAt = &now

	if err := m.store.TaskQueues().Update(ctx, task); err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	// Resume workflow execution
	if m.onComplete != nil {
		go func() {
			_ = m.onComplete(context.Background(), task.ExecutionID, task.StageID, output)
		}()
	}
	return nil
}

// Fail marks task as failed, triggering retry or workflow failure/compensation.
func (m *TaskQueueManager) Fail(ctx context.Context, taskID, workerID, lockToken string, errorMsg string) error {
	task, err := m.store.TaskQueues().Get(ctx, taskID)
	if err != nil {
		return ErrTaskNotFound
	}
	if task.Status != model.TaskStatusRunning {
		return ErrTaskAlreadyDone
	}
	if task.LockToken != lockToken || task.WorkerID != workerID {
		return ErrInvalidLockToken
	}

	now := time.Now()
	task.Status = model.TaskStatusFailed
	task.ErrorMessage = errorMsg
	task.CompletedAt = &now

	if err := m.store.TaskQueues().Update(ctx, task); err != nil {
		return fmt.Errorf("failed to fail task: %w", err)
	}

	// Notify failure
	if m.onFail != nil {
		go func() {
			_ = m.onFail(context.Background(), task.ExecutionID, task.StageID, errorMsg)
		}()
	}
	return nil
}

// reapExpiredLeasesLoop periodically checks for dead workers and re-queues expired tasks.
func (m *TaskQueueManager) reapExpiredLeasesLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			if m.store == nil || m.store.TaskQueues() == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			requeuedCount, err := m.store.TaskQueues().ReclaimExpiredLeases(ctx)
			cancel()
			if err == nil && requeuedCount > 0 {
				// Re-notify all queues
				m.mu.RLock()
				for _, qd := range m.queues {
					select {
					case qd.notify <- struct{}{}:
					default:
					}
				}
				m.mu.RUnlock()
			}
		}
	}
}

// Close stops the task queue background workers.
func (m *TaskQueueManager) Close() {
	close(m.stopChan)
}
