package storage

import (
	"context"
	"strings"
	"fmt"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

var ErrAlreadyExists = errors.New("record already exists")

// MemoryStore provides thread-safe in-memory storage for all entities.
type MemoryStore struct {
	workflows  *MemoryWorkflowStore
	executions *MemoryExecutionStore
	audits     *MemoryAuditStore
	approvals  *MemoryApprovalStore
	timers     *MemoryTimerStore
	signals    *MemorySignalStore
	versions   *MemoryWorkflowVersionStore
	dlq        *MemoryDLQStore
	webhooks   *MemoryWebhookStore
	tasks      *MemoryTaskQueueStore
	outbox     *MemoryOutboxStore
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows:  &MemoryWorkflowStore{data: make(map[string]*model.Workflow)},
		executions: &MemoryExecutionStore{data: make(map[string]*model.Execution)},
		audits:     &MemoryAuditStore{entries: make([]*model.AuditLogEntry, 0)},
		approvals:  &MemoryApprovalStore{data: make(map[string]*model.ApprovalRequest)},
		timers:     &MemoryTimerStore{data: make(map[string]*model.DurableTimer)},
		signals:    &MemorySignalStore{data: make(map[string]*model.SignalSubscription)},
		versions:   &MemoryWorkflowVersionStore{data: make(map[string][]*model.WorkflowVersion)},
		dlq:        NewMemoryDLQStore(),
		webhooks:   NewMemoryWebhookStore(),
		tasks:      NewMemoryTaskQueueStore(),
		outbox:     NewMemoryOutboxStore(),
	}
}

func (s *MemoryStore) Workflows() WorkflowStore                   { return s.workflows }
func (s *MemoryStore) Executions() ExecutionStore                 { return s.executions }
func (s *MemoryStore) Audits() AuditStore                         { return s.audits }
func (s *MemoryStore) Approvals() ApprovalStore                   { return s.approvals }
func (s *MemoryStore) Timers() TimerStore                         { return s.timers }
func (s *MemoryStore) Signals() SignalStore                       { return s.signals }
func (s *MemoryStore) WorkflowVersions() WorkflowVersionStore     { return s.versions }
func (s *MemoryStore) DLQ() DLQStore                              { return s.dlq }
func (s *MemoryStore) Webhooks() WebhookStore                     { return s.webhooks }
func (s *MemoryStore) TaskQueues() TaskQueueStore                 { return s.tasks }
func (s *MemoryStore) Outbox() OutboxStore                        { return s.outbox }
func (s *MemoryStore) Close() error                               { return nil }

// MemoryWorkflowStore manages workflows in memory.
type MemoryWorkflowStore struct {
	mu   sync.RWMutex
	data map[string]*model.Workflow
}

func (m *MemoryWorkflowStore) Create(ctx context.Context, wf *model.Workflow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[wf.ID]; exists {
		return errors.New("workflow already exists")
	}
	if wf.TenantID == "" {
		wf.TenantID = "default"
	}
	clone := *wf
	m.data[wf.ID] = &clone
	return nil
}

func (m *MemoryWorkflowStore) Get(ctx context.Context, id string) (*model.Workflow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	wf, exists := m.data[id]
	if !exists {
		return nil, ErrWorkflowNotFound
	}
	clone := *wf
	return &clone, nil
}

func (m *MemoryWorkflowStore) Update(ctx context.Context, wf *model.Workflow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[wf.ID]; !exists {
		return ErrWorkflowNotFound
	}
	if wf.TenantID == "" {
		wf.TenantID = "default"
	}
	clone := *wf
	m.data[wf.ID] = &clone
	return nil
}

func (m *MemoryWorkflowStore) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[id]; !exists {
		return ErrWorkflowNotFound
	}
	delete(m.data, id)
	return nil
}

func (m *MemoryWorkflowStore) List(ctx context.Context, filter WorkflowFilter) ([]*model.Workflow, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*model.Workflow
	for _, wf := range m.data {
		if filter.TenantID != "" {
			wfTenant := wf.TenantID
			if wfTenant == "" {
				wfTenant = "default"
			}
			if wfTenant != filter.TenantID {
				continue
			}
		}
		if filter.Status != "" && wf.Status != filter.Status {
			continue
		}
		clone := *wf
		result = append(result, &clone)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	total := len(result)
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}

	return result, total, nil
}

// MemoryExecutionStore manages executions in memory.
type MemoryExecutionStore struct {
	mu   sync.RWMutex
	data map[string]*model.Execution
}

func (m *MemoryExecutionStore) Create(ctx context.Context, exec *model.Execution) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if exec.TenantID == "" {
		exec.TenantID = "default"
	}
	clone := *exec
	m.data[exec.ID] = &clone
	return nil
}

func (m *MemoryExecutionStore) Get(ctx context.Context, id string) (*model.Execution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	exec, exists := m.data[id]
	if !exists {
		return nil, ErrExecutionNotFound
	}
	clone := *exec
	return &clone, nil
}

func (m *MemoryExecutionStore) GetByIdempotencyKey(ctx context.Context, workflowID, idempotencyKey string) (*model.Execution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, e := range m.data {
		if e.WorkflowID == workflowID && e.IdempotencyKey == idempotencyKey {
			clone := *e
			return &clone, nil
		}
	}
	return nil, ErrExecutionNotFound
}

func (m *MemoryExecutionStore) Update(ctx context.Context, exec *model.Execution) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[exec.ID]; !exists {
		return ErrExecutionNotFound
	}
	if exec.TenantID == "" {
		exec.TenantID = "default"
	}
	clone := *exec
	m.data[exec.ID] = &clone
	return nil
}

func (m *MemoryExecutionStore) List(ctx context.Context, filter ExecutionFilter) ([]*model.Execution, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*model.Execution
	for _, e := range m.data {
		if filter.TenantID != "" {
			eTenant := e.TenantID
			if eTenant == "" {
				eTenant = "default"
			}
			if eTenant != filter.TenantID {
				continue
			}
		}
		if filter.WorkflowID != "" && e.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.Status != "" && e.Status != filter.Status {
			continue
		}
		clone := *e
		result = append(result, &clone)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StartedAt.After(result[j].StartedAt)
	})

	total := len(result)
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}

	return result, total, nil
}

func (m *MemoryExecutionStore) AppendStep(ctx context.Context, execID string, step *model.StepExecution) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exec, exists := m.data[execID]
	if !exists {
		return ErrExecutionNotFound
	}
	found := false
	for i, s := range exec.Steps {
		if s.ID == step.ID || (s.StageID == step.StageID && s.Status == step.Status) {
			exec.Steps[i] = step
			found = true
			break
		}
	}
	if !found {
		exec.Steps = append(exec.Steps, step)
	}
	return nil
}

func (m *MemoryExecutionStore) ListChildren(ctx context.Context, parentExecID string) ([]*model.Execution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*model.Execution
	for _, e := range m.data {
		if e.ParentExecutionID == parentExecID {
			clone := *e
			result = append(result, &clone)
		}
	}
	return result, nil
}

func (m *MemoryExecutionStore) UpdateStepHeartbeat(ctx context.Context, stepID string, heartbeat time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.data {
		for _, s := range e.Steps {
			if s.ID == stepID {
				s.HeartbeatAt = &heartbeat
				return nil
			}
		}
	}
	return nil
}

func (m *MemoryExecutionStore) UpdateHeartbeat(ctx context.Context, execID string, heartbeat time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exec, exists := m.data[execID]
	if !exists {
		return ErrExecutionNotFound
	}
	exec.HeartbeatAt = &heartbeat
	return nil
}

func (m *MemoryExecutionStore) ClaimStalledExecutions(ctx context.Context, workerID string, stalledThreshold time.Duration, limit int) ([]*model.Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = 50
	}
	cutoff := time.Now().Add(-stalledThreshold)
	now := time.Now()

	var claimed []*model.Execution
	for _, exec := range m.data {
		if exec.Status == model.ExecutionStatusRunning {
			if exec.HeartbeatAt == nil || exec.HeartbeatAt.Before(cutoff) {
				exec.WorkerID = workerID
				exec.HeartbeatAt = &now
				clone := *exec
				claimed = append(claimed, &clone)
				if len(claimed) >= limit {
					break
				}
			}
		}
	}

	return claimed, nil
}

// MemoryAuditStore manages audit logs in memory.
type MemoryAuditStore struct {
	mu      sync.RWMutex
	entries []*model.AuditLogEntry
}

func (m *MemoryAuditStore) Log(ctx context.Context, entry *model.AuditLogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *entry
	m.entries = append(m.entries, &clone)
	return nil
}

func (m *MemoryAuditStore) List(ctx context.Context, limit, offset int) ([]*model.AuditLogEntry, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := len(m.entries)
	var reversed []*model.AuditLogEntry
	for i := len(m.entries) - 1; i >= 0; i-- {
		reversed = append(reversed, m.entries[i])
	}

	if offset > 0 && offset < len(reversed) {
		reversed = reversed[offset:]
	}
	if limit > 0 && limit < len(reversed) {
		reversed = reversed[:limit]
	}

	return reversed, total, nil
}

// MemoryApprovalStore manages approvals in memory.
type MemoryApprovalStore struct {
	mu   sync.RWMutex
	data map[string]*model.ApprovalRequest
}

func (m *MemoryApprovalStore) Create(ctx context.Context, req *model.ApprovalRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *req
	m.data[req.ID] = &clone
	return nil
}

func (m *MemoryApprovalStore) Get(ctx context.Context, id string) (*model.ApprovalRequest, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	req, exists := m.data[id]
	if !exists {
		return nil, ErrApprovalNotFound
	}
	clone := *req
	return &clone, nil
}

func (m *MemoryApprovalStore) GetByExecutionID(ctx context.Context, executionID string) (*model.ApprovalRequest, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, req := range m.data {
		if req.ExecutionID == executionID {
			clone := *req
			return &clone, nil
		}
	}
	return nil, ErrApprovalNotFound
}

func (m *MemoryApprovalStore) Update(ctx context.Context, req *model.ApprovalRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[req.ID]; !exists {
		return ErrApprovalNotFound
	}
	clone := *req
	m.data[req.ID] = &clone
	return nil
}


func (m *MemoryApprovalStore) List(ctx context.Context, limit, offset int) ([]*model.ApprovalRequest, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var all []*model.ApprovalRequest
	for _, req := range m.data {
		clone := *req
		all = append(all, &clone)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})

	total := len(all)
	if offset > 0 && offset < len(all) {
		all = all[offset:]
	}
	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}

	return all, total, nil
}

func (m *MemoryApprovalStore) ListPending(ctx context.Context) ([]*model.ApprovalRequest, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var pending []*model.ApprovalRequest
	for _, req := range m.data {
		if req.Status == model.ApprovalStatusPending {
			clone := *req
			pending = append(pending, &clone)
		}
	}
	return pending, nil
}

// MemoryTimerStore manages timers in memory.
type MemoryTimerStore struct {
	mu   sync.RWMutex
	data map[string]*model.DurableTimer
}

func (m *MemoryTimerStore) Create(ctx context.Context, timer *model.DurableTimer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *timer
	m.data[timer.ID] = &clone
	return nil
}

func (m *MemoryTimerStore) Get(ctx context.Context, id string) (*model.DurableTimer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	timer, exists := m.data[id]
	if !exists {
		return nil, ErrTimerNotFound
	}
	clone := *timer
	return &clone, nil
}

func (m *MemoryTimerStore) ListDue(ctx context.Context, now time.Time) ([]*model.DurableTimer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var due []*model.DurableTimer
	for _, t := range m.data {
		if t.Status == model.TimerStatusPending && (t.FireAt.Before(now) || t.FireAt.Equal(now)) {
			clone := *t
			due = append(due, &clone)
		}
	}
	return due, nil
}

func (m *MemoryTimerStore) UpdateStatus(ctx context.Context, id string, status model.TimerStatus, firedAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	timer, exists := m.data[id]
	if !exists {
		return ErrTimerNotFound
	}
	timer.Status = status
	timer.FiredAt = firedAt
	return nil
}

func (m *MemoryTimerStore) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, id)
	return nil
}

// MemorySignalStore manages signals in memory.
type MemorySignalStore struct {
	mu   sync.RWMutex
	data map[string]*model.SignalSubscription
}

func (m *MemorySignalStore) Create(ctx context.Context, sig *model.SignalSubscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *sig
	m.data[sig.ID] = &clone
	return nil
}

func (m *MemorySignalStore) Get(ctx context.Context, id string) (*model.SignalSubscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sig, exists := m.data[id]
	if !exists {
		return nil, ErrSignalNotFound
	}
	clone := *sig
	return &clone, nil
}

func (m *MemorySignalStore) ListByExecution(ctx context.Context, executionID string) ([]*model.SignalSubscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*model.SignalSubscription
	for _, s := range m.data {
		if s.ExecutionID == executionID {
			clone := *s
			result = append(result, &clone)
		}
	}
	return result, nil
}

func (m *MemorySignalStore) FindWaiting(ctx context.Context, executionID, signalName string) (*model.SignalSubscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.data {
		if s.ExecutionID == executionID && s.SignalName == signalName && s.Status == model.SignalStatusWaiting {
			clone := *s
			return &clone, nil
		}
	}
	return nil, ErrSignalNotFound
}

func (m *MemorySignalStore) Update(ctx context.Context, sig *model.SignalSubscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.data[sig.ID]; !exists {
		return ErrSignalNotFound
	}
	clone := *sig
	m.data[sig.ID] = &clone
	return nil
}

// MemoryWorkflowVersionStore manages workflow versions in memory.
type MemoryWorkflowVersionStore struct {
	mu   sync.RWMutex
	data map[string][]*model.WorkflowVersion // workflowID -> versions
}

func (m *MemoryWorkflowVersionStore) CreateVersion(ctx context.Context, ver *model.WorkflowVersion) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *ver
	m.data[ver.WorkflowID] = append(m.data[ver.WorkflowID], &clone)
	return nil
}

func (m *MemoryWorkflowVersionStore) GetVersion(ctx context.Context, workflowID string, versionNum int) (*model.WorkflowVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.data[workflowID] {
		if v.VersionNum == versionNum {
			clone := *v
			return &clone, nil
		}
	}
	return nil, ErrVersionNotFound
}

func (m *MemoryWorkflowVersionStore) ListVersions(ctx context.Context, workflowID string) ([]*model.WorkflowVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*model.WorkflowVersion
	for _, v := range m.data[workflowID] {
		clone := *v
		result = append(result, &clone)
	}
	return result, nil
}

func (m *MemoryWorkflowVersionStore) GetActiveVersion(ctx context.Context, workflowID string) (*model.WorkflowVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.data[workflowID] {
		if v.Status == model.VersionStatusActive {
			clone := *v
			return &clone, nil
		}
	}
	return nil, ErrVersionNotFound
}

func (m *MemoryWorkflowVersionStore) SetActiveVersion(ctx context.Context, workflowID string, versionNum int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	found := false
	for _, v := range m.data[workflowID] {
		if v.VersionNum == versionNum {
			v.Status = model.VersionStatusActive
			found = true
		} else if v.Status == model.VersionStatusActive {
			v.Status = model.VersionStatusDeprecated
		}
	}
	if !found {
		return ErrVersionNotFound
	}
	return nil
}

// MemoryDLQStore manages DLQ messages in memory.
type MemoryDLQStore struct {
	mu       sync.RWMutex
	messages map[string]*model.DLQMessage
}

func NewMemoryDLQStore() *MemoryDLQStore {
	return &MemoryDLQStore{
		messages: make(map[string]*model.DLQMessage),
	}
}

func (s *MemoryDLQStore) Push(ctx context.Context, msg *model.DLQMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("dlq_%d", time.Now().UnixNano())
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.Status == "" {
		msg.Status = model.DLQStatusPending
	}
	s.messages[msg.ID] = msg
	return nil
}

func (s *MemoryDLQStore) Get(ctx context.Context, id string) (*model.DLQMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.messages[id]
	if !ok {
		return nil, errors.New("dlq message not found")
	}
	return msg, nil
}

func (s *MemoryDLQStore) List(ctx context.Context, filter DLQFilter) ([]*model.DLQMessage, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*model.DLQMessage
	for _, m := range s.messages {
		if filter.Status != "" && m.Status != filter.Status {
			continue
		}
		if filter.WorkflowID != "" && m.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.Source != "" && m.Source != filter.Source {
			continue
		}
		result = append(result, m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	total := len(result)
	if filter.Offset > 0 {
		if filter.Offset >= total {
			return []*model.DLQMessage{}, total, nil
		}
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}

	return result, total, nil
}

func (s *MemoryDLQStore) UpdateStatus(ctx context.Context, id string, status model.DLQStatus, replayedAt *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[id]
	if !ok {
		return errors.New("dlq message not found")
	}
	msg.Status = status
	if replayedAt != nil {
		msg.ReplayedAt = replayedAt
	}
	return nil
}

func (s *MemoryDLQStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.messages, id)
	return nil
}

func (s *MemoryDLQStore) ListPending(ctx context.Context, limit int) ([]*model.DLQMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*model.DLQMessage
	for _, m := range s.messages {
		if m.Status == model.DLQStatusPending && !m.IsDead {
			result = append(result, m)
		}
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (s *MemoryDLQStore) IncrementAttempts(ctx context.Context, id, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[id]
	if !ok {
		return errors.New("dlq message not found")
	}
	msg.Attempts++
	msg.ErrorMessage = errMsg
	now := time.Now()
	msg.LastAttemptAt = &now
	return nil
}

func (s *MemoryDLQStore) MarkDead(ctx context.Context, id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[id]
	if !ok {
		return errors.New("dlq message not found")
	}
	msg.IsDead = true
	msg.DeadReason = reason
	msg.Status = model.DLQStatusDead
	return nil
}

func (s *MemoryDLQStore) MarkResolved(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.messages[id]
	if !ok {
		return errors.New("dlq message not found")
	}
	now := time.Now()
	msg.Status = model.DLQStatusReplayed
	msg.ReplayedAt = &now
	return nil
}


// MemoryWebhookStore manages webhook bindings in memory.
type MemoryWebhookStore struct {
	mu   sync.RWMutex
	data map[string]*model.WebhookBinding
}

func NewMemoryWebhookStore() *MemoryWebhookStore {
	return &MemoryWebhookStore{data: make(map[string]*model.WebhookBinding)}
}

func (m *MemoryWebhookStore) Create(ctx context.Context, binding *model.WebhookBinding) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if binding.ID == "" {
		binding.ID = fmt.Sprintf("whk_%d", time.Now().UnixNano())
	}
	binding.CreatedAt = time.Now()
	binding.UpdatedAt = time.Now()
	clone := *binding
	m.data[binding.ID] = &clone
	return nil
}

func (m *MemoryWebhookStore) Get(ctx context.Context, id string) (*model.WebhookBinding, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.data[id]
	if !ok {
		return nil, errors.New("webhook binding not found")
	}
	clone := *b
	return &clone, nil
}

func (m *MemoryWebhookStore) FindByPath(ctx context.Context, path string, method string) (*model.WebhookBinding, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cleanPath := strings.Trim(path, "/")
	cleanPath = strings.TrimPrefix(cleanPath, "webhooks/")

	for _, b := range m.data {
		if !b.Enabled {
			continue
		}
		bClean := strings.Trim(b.PathPattern, "/")
		bClean = strings.TrimPrefix(bClean, "webhooks/")
		if bClean == cleanPath {
			if b.Method == "*" || b.Method == "" || strings.EqualFold(b.Method, method) {
				clone := *b
				return &clone, nil
			}
		}
	}
	return nil, errors.New("no matching webhook binding")
}

func (m *MemoryWebhookStore) List(ctx context.Context) ([]*model.WebhookBinding, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	list := make([]*model.WebhookBinding, 0, len(m.data))
	for _, b := range m.data {
		clone := *b
		list = append(list, &clone)
	}
	return list, nil
}

func (m *MemoryWebhookStore) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, id)
	return nil
}

// MemoryTaskQueueStore manages task queues in memory.
type MemoryTaskQueueStore struct {
	mu   sync.RWMutex
	data map[string]*model.TaskItem
}

func NewMemoryTaskQueueStore() *MemoryTaskQueueStore {
	return &MemoryTaskQueueStore{data: make(map[string]*model.TaskItem)}
}

func (m *MemoryTaskQueueStore) Create(ctx context.Context, task *model.TaskItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *task
	m.data[task.ID] = &clone
	return nil
}

func (m *MemoryTaskQueueStore) Get(ctx context.Context, id string) (*model.TaskItem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.data[id]
	if !ok {
		return nil, errors.New("task not found")
	}
	clone := *t
	return &clone, nil
}

func (m *MemoryTaskQueueStore) Update(ctx context.Context, task *model.TaskItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[task.ID]; !ok {
		return errors.New("task not found")
	}
	clone := *task
	m.data[task.ID] = &clone
	return nil
}

func (m *MemoryTaskQueueStore) AcquirePending(ctx context.Context, queueName, workerID string, leaseDuration time.Duration) (*model.TaskItem, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, t := range m.data {
		if t.QueueName == queueName && t.Status == model.TaskStatusPending {
			token := fmt.Sprintf("tok_%d_%d", now.UnixNano(), t.Attempts+1)
			dur := leaseDuration
			if t.HeartbeatTimeout > 0 {
				dur = t.HeartbeatTimeout
			}
			leaseExp := now.Add(dur)

			t.Status = model.TaskStatusRunning
			t.WorkerID = workerID
			t.LockToken = token
			t.LeaseExpiresAt = &leaseExp
			t.StartedAt = &now
			t.Attempts++

			clone := *t
			return &clone, nil
		}
	}
	return nil, nil
}

func (m *MemoryTaskQueueStore) ExtendLease(ctx context.Context, taskID string, newLeaseExpiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.data[taskID]
	if !ok {
		return errors.New("task not found")
	}
	t.LeaseExpiresAt = &newLeaseExpiresAt
	return nil
}

func (m *MemoryTaskQueueStore) ReclaimExpiredLeases(ctx context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	reclaimed := 0
	for _, t := range m.data {
		if t.Status == model.TaskStatusRunning && t.LeaseExpiresAt != nil && now.After(*t.LeaseExpiresAt) {
			if t.Attempts >= t.MaxAttempts {
				t.Status = model.TaskStatusTimedOut
				t.ErrorMessage = "task timed out: maximum lease attempts exceeded without heartbeat"
			} else {
				t.Status = model.TaskStatusPending
				t.WorkerID = ""
				t.LockToken = ""
				t.LeaseExpiresAt = nil
				reclaimed++
			}
		}
	}
	return reclaimed, nil
}

func (m *MemoryTaskQueueStore) List(ctx context.Context, queueName string, status model.TaskStatus) ([]*model.TaskItem, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]*model.TaskItem, 0)
	for _, t := range m.data {
		if (queueName == "" || t.QueueName == queueName) && (status == "" || t.Status == status) {
			clone := *t
			res = append(res, &clone)
		}
	}
	return res, nil
}

// MemoryOutboxStore manages transactional outbox events in memory.
type memoryOutboxItem struct {
	id        string
	event     *model.WorkflowEvent
	status    string
	createdAt time.Time
}

type MemoryOutboxStore struct {
	mu     sync.RWMutex
	events []*memoryOutboxItem
}

func NewMemoryOutboxStore() *MemoryOutboxStore {
	return &MemoryOutboxStore{
		events: make([]*memoryOutboxItem, 0),
	}
}

func (m *MemoryOutboxStore) Push(ctx context.Context, event *model.WorkflowEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	eventID := fmt.Sprintf("obx_%s_%d", event.ExecutionID, event.Timestamp.UnixNano())
	clone := *event
	m.events = append(m.events, &memoryOutboxItem{
		id:        eventID,
		event:     &clone,
		status:    "PENDING",
		createdAt: event.Timestamp,
	})
	return nil
}

func (m *MemoryOutboxStore) FetchPending(ctx context.Context, limit int) ([]*model.WorkflowEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = 100
	}

	var pending []*model.WorkflowEvent
	for _, item := range m.events {
		if item.status == "PENDING" {
			clone := *item.event
			pending = append(pending, &clone)
			if len(pending) >= limit {
				break
			}
		}
	}
	return pending, nil
}

func (m *MemoryOutboxStore) MarkProcessed(ctx context.Context, eventIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idMap := make(map[string]bool, len(eventIDs))
	for _, id := range eventIDs {
		idMap[id] = true
	}

	for _, item := range m.events {
		if idMap[item.id] || idMap[item.event.ExecutionID] {
			item.status = "PROCESSED"
		}
	}
	return nil
}

