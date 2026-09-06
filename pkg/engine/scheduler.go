package engine

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// ScheduledTask represents an item in the priority queue.
type ScheduledTask struct {
	ID       string
	Deadline time.Time
	Callback func()
	index    int // index in the heap, maintained by heap.Interface
}

// taskPriorityQueue implements heap.Interface and holds ScheduledTasks ordered by earliest deadline (Min-Heap).
type taskPriorityQueue []*ScheduledTask

func (pq taskPriorityQueue) Len() int           { return len(pq) }
func (pq taskPriorityQueue) Less(i, j int) bool { return pq[i].Deadline.Before(pq[j].Deadline) }
func (pq taskPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *taskPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*ScheduledTask)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *taskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// TaskScheduler is a non-blocking, O(log N) Min-Heap priority queue scheduler.
// It manages millions of delayed workflow stages and retry backoffs using a single timer goroutine.
type TaskScheduler struct {
	mu      sync.Mutex
	pq      taskPriorityQueue
	taskMap map[string]*ScheduledTask
	wakeUp  chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewTaskScheduler initializes the binary min-heap task scheduler.
func NewTaskScheduler() *TaskScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &TaskScheduler{
		pq:      make(taskPriorityQueue, 0),
		taskMap: make(map[string]*ScheduledTask),
		wakeUp:  make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
	}
	heap.Init(&s.pq)
	go s.dispatcherLoop()
	return s
}

// Schedule inserts a new task to be executed after the specified delay.
func (s *TaskScheduler) Schedule(id string, delay time.Duration, callback func()) *ScheduledTask {
	deadline := time.Now().Add(delay)
	task := &ScheduledTask{
		ID:       id,
		Deadline: deadline,
		Callback: callback,
	}

	s.mu.Lock()
	heap.Push(&s.pq, task)
	s.taskMap[id] = task
	isRoot := task.index == 0
	s.mu.Unlock()

	// If the new task is at the root (earliest deadline), signal the dispatcher to reset timer
	if isRoot {
		select {
		case s.wakeUp <- struct{}{}:
		default:
		}
	}

	return task
}

// Cancel removes a scheduled task from the min-heap before its deadline expires.
func (s *TaskScheduler) Cancel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.taskMap[id]
	if !exists || task.index < 0 {
		return false
	}

	heap.Remove(&s.pq, task.index)
	delete(s.taskMap, id)
	return true
}

// dispatcherLoop is the single master loop that fires ready tasks at their exact deadlines.
func (s *TaskScheduler) dispatcherLoop() {
	var timer *time.Timer

	for {
		s.mu.Lock()
		var nextDeadline time.Duration
		hasTask := len(s.pq) > 0
		if hasTask {
			earliest := s.pq[0].Deadline
			now := time.Now()
			if earliest.After(now) {
				nextDeadline = earliest.Sub(now)
			} else {
				nextDeadline = 0
			}
		}
		s.mu.Unlock()

		if !hasTask {
			select {
			case <-s.ctx.Done():
				return
			case <-s.wakeUp:
				continue
			}
		}

		if timer == nil {
			timer = time.NewTimer(nextDeadline)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(nextDeadline)
		}

		select {
		case <-s.ctx.Done():
			timer.Stop()
			return

		case <-s.wakeUp:
			// A task with earlier deadline was inserted, loop around to recalculate timer
			continue

		case <-timer.C:
			// Execute all ready tasks
			s.mu.Lock()
			now := time.Now()
			var readyTasks []*ScheduledTask

			for len(s.pq) > 0 && !s.pq[0].Deadline.After(now) {
				task := heap.Pop(&s.pq).(*ScheduledTask)
				delete(s.taskMap, task.ID)
				readyTasks = append(readyTasks, task)
			}
			s.mu.Unlock()

			for _, t := range readyTasks {
				if t.Callback != nil {
					go func(cb func()) {
						defer func() { _ = recover() }()
						cb()
					}(t.Callback)
				}
			}
		}
	}
}

// Close gracefully terminates the scheduler.
func (s *TaskScheduler) Close() {
	s.cancel()
}
