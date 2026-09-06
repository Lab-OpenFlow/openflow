package engine

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

// cronJobEntry represents a scheduled workflow in the Min-Heap.
type cronJobEntry struct {
	workflowID    string
	scheduleExpr  string
	parsedSched   cron.Schedule
	overlapPolicy model.OverlapPolicy
	nextRun       time.Time
	index         int // Index in the heap
}

type cronMinHeap []*cronJobEntry

func (h cronMinHeap) Len() int           { return len(h) }
func (h cronMinHeap) Less(i, j int) bool { return h[i].nextRun.Before(h[j].nextRun) }
func (h cronMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *cronMinHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*cronJobEntry)
	item.index = n
	*h = append(*h, item)
}
func (h *cronMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// CronWorkflowScheduler manages high-precision distributed scheduled workflow dispatches via Min-Heap.
type CronWorkflowScheduler struct {
	store       storage.Store
	executeFn   func(ctx context.Context, workflowID string, input map[string]interface{}, triggerType model.TriggerType) (*model.Execution, error)
	cancelFn    func(ctx context.Context, executionID string) error
	parser      cron.Parser
	mu          sync.Mutex
	pq          cronMinHeap
	jobMap      map[string]*cronJobEntry
	stopChan    chan struct{}
	reschedChan chan struct{}
}

// NewCronWorkflowScheduler initializes the distributed cron scheduler.
func NewCronWorkflowScheduler(
	store storage.Store,
	executeFn func(ctx context.Context, workflowID string, input map[string]interface{}, triggerType model.TriggerType) (*model.Execution, error),
	cancelFn func(ctx context.Context, executionID string) error,
) *CronWorkflowScheduler {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s := &CronWorkflowScheduler{
		store:       store,
		executeFn:   executeFn,
		cancelFn:    cancelFn,
		parser:      parser,
		pq:          make(cronMinHeap, 0),
		jobMap:      make(map[string]*cronJobEntry),
		stopChan:    make(chan struct{}),
		reschedChan: make(chan struct{}, 1),
	}
	heap.Init(&s.pq)
	return s
}

// Start loads all active workflows with cron schedules and starts the scheduler loop.
func (s *CronWorkflowScheduler) Start(ctx context.Context) error {
	wfs, _, err := s.store.Workflows().List(ctx, storage.WorkflowFilter{Status: model.WorkflowStatusActive})
	if err != nil {
		return fmt.Errorf("failed to list workflows for cron scheduler: %w", err)
	}

	for _, wf := range wfs {
		if wf.CronSchedule != "" {
			_ = s.RegisterSchedule(wf.ID, wf.CronSchedule, wf.OverlapPolicy)
		} else if wf.Trigger != nil && wf.Trigger.Type == model.TriggerTypeCron && wf.Trigger.Cron != "" {
			_ = s.RegisterSchedule(wf.ID, wf.Trigger.Cron, wf.OverlapPolicy)
		}
	}

	go s.schedulerLoop()
	return nil
}

// RegisterSchedule adds or updates a workflow in the Min-Heap schedule.
func (s *CronWorkflowScheduler) RegisterSchedule(workflowID, cronExpr string, overlap model.OverlapPolicy) error {
	sched, err := s.parser.Parse(cronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression '%s': %w", cronExpr, err)
	}

	if overlap == "" {
		overlap = model.OverlapPolicySkip
	}

	now := time.Now()
	nextRun := sched.Next(now)

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.jobMap[workflowID]
	if exists {
		entry.scheduleExpr = cronExpr
		entry.parsedSched = sched
		entry.overlapPolicy = overlap
		entry.nextRun = nextRun
		heap.Fix(&s.pq, entry.index)
	} else {
		entry = &cronJobEntry{
			workflowID:    workflowID,
			scheduleExpr:  cronExpr,
			parsedSched:   sched,
			overlapPolicy: overlap,
			nextRun:       nextRun,
		}
		s.jobMap[workflowID] = entry
		heap.Push(&s.pq, entry)
	}

	// Trigger immediate rescheduling check
	select {
	case s.reschedChan <- struct{}{}:
	default:
	}
	return nil
}

// UnregisterSchedule removes a workflow from the cron schedule.
func (s *CronWorkflowScheduler) UnregisterSchedule(workflowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.jobMap[workflowID]
	if !exists {
		return
	}
	heap.Remove(&s.pq, entry.index)
	delete(s.jobMap, workflowID)
}

func (s *CronWorkflowScheduler) schedulerLoop() {
	for {
		s.mu.Lock()
		if s.pq.Len() == 0 {
			s.mu.Unlock()
			select {
			case <-s.stopChan:
				return
			case <-s.reschedChan:
				continue
			case <-time.After(1 * time.Minute):
				continue
			}
		}

		nextEntry := s.pq[0]
		now := time.Now()
		sleepDur := nextEntry.nextRun.Sub(now)
		s.mu.Unlock()

		if sleepDur > 0 {
			timer := time.NewTimer(sleepDur)
			select {
			case <-s.stopChan:
				timer.Stop()
				return
			case <-s.reschedChan:
				timer.Stop()
				continue
			case <-timer.C:
				// Timer fired, proceed to dispatch
			}
		}

		// Pop and process due entries
		s.mu.Lock()
		now = time.Now()
		for s.pq.Len() > 0 && !s.pq[0].nextRun.After(now) {
			dueItem := heap.Pop(&s.pq).(*cronJobEntry)
			wfID := dueItem.workflowID
			overlap := dueItem.overlapPolicy

			// Calculate and reschedule next run in O(log N)
			dueItem.nextRun = dueItem.parsedSched.Next(now)
			heap.Push(&s.pq, dueItem)

			// Dispatch in background
			go s.dispatchCronInstance(context.Background(), wfID, overlap)
		}
		s.mu.Unlock()
	}
}

func (s *CronWorkflowScheduler) dispatchCronInstance(ctx context.Context, workflowID string, overlap model.OverlapPolicy) {
	// Handle overlap policy
	if overlap == model.OverlapPolicySkip || overlap == model.OverlapPolicyCancelOther {
		activeList, _, err := s.store.Executions().List(ctx, storage.ExecutionFilter{
			WorkflowID: workflowID,
			Status:     model.ExecutionStatusRunning,
			Limit:      5,
		})
		if err == nil && len(activeList) > 0 {
			if overlap == model.OverlapPolicySkip {
				slog.Info("Cron execution skipped due to active instance (SKIP policy)",
					slog.String("workflow_id", workflowID),
				)
				return
			} else if overlap == model.OverlapPolicyCancelOther {
				for _, runningExec := range activeList {
					if s.cancelFn != nil {
						_ = s.cancelFn(ctx, runningExec.ID)
					}
				}
			}
		}
	}

	payload := map[string]interface{}{
		"cron_triggered": true,
		"triggered_at":   time.Now().Format(time.RFC3339),
	}

	if s.executeFn != nil {
		_, err := s.executeFn(ctx, workflowID, payload, model.TriggerTypeCron)
		if err != nil {
			slog.Error("Failed to trigger scheduled cron workflow",
				slog.String("workflow_id", workflowID),
				slog.String("error", err.Error()),
			)
		}
	}
}

// Close stops the scheduler.
func (s *CronWorkflowScheduler) Close() {
	close(s.stopChan)
}
