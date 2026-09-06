package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Lab-OpenFlow/openflow/pkg/connectors"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
	"github.com/Lab-OpenFlow/openflow/pkg/observability"
	"github.com/Lab-OpenFlow/openflow/pkg/security"
	"github.com/Lab-OpenFlow/openflow/pkg/storage"
)

var (
	ErrWorkflowInvalid = errors.New("invalid workflow definition")
	ErrExecutionFailed = errors.New("workflow execution failed")
)

// EventListener receives real-time execution events.
type EventListener func(event model.WorkflowEvent)

// EngineOptions controls runtime behaviour of the engine.
type EngineOptions struct {
	// MaxConcurrentExecutions limits the number of simultaneously running workflow goroutines.
	// Defaults to 100 if not set.
	MaxConcurrentExecutions int
}

// Engine is the central orchestration execution engine.
type Engine struct {
	concurrencyLimits sync.Map // map[workflowID]chan struct{}
	store      storage.Store
	registry   *connectors.Registry
	evaluator  *Evaluator
	saga       *SagaManager
	breakers          *CircuitBreakerRegistry
	idempotencyFilter *RotatingBloomFilter
	listeners         []EventListener
	listenerMu        sync.RWMutex
	semaphore         chan struct{}
	semaphoreCapacity int
	eventChan         chan model.WorkflowEvent
	dispatcherCtx     context.Context
	dispatcherCancel  context.CancelFunc
	taskQueues       *TaskQueueManager
	cronScheduler    *CronWorkflowScheduler
	// dlqWorker auto-retries failed executions from the Dead Letter Queue
	dlqWorker   *DLQWorker
	outboxRelay *OutboxRelay
}

// NewEngine creates a new workflow orchestration engine.
func NewEngine(store storage.Store, registry *connectors.Registry, opts ...EngineOptions) *Engine {
	maxConc := 100
	if len(opts) > 0 && opts[0].MaxConcurrentExecutions > 0 {
		maxConc = opts[0].MaxConcurrentExecutions
	}
	evaluator := NewEvaluator()
	dispCtx, dispCancel := context.WithCancel(context.Background())

	eng := &Engine{
		store:             store,
		registry:          registry,
		evaluator:         evaluator,
		saga:              NewSagaManager(evaluator),
		breakers:          NewCircuitBreakerRegistry(),
		idempotencyFilter: NewRotatingBloomFilter(131072, 5, 1*time.Hour),
		semaphore:         make(chan struct{}, maxConc),
		semaphoreCapacity: maxConc,
		eventChan:         make(chan model.WorkflowEvent, 2048),
		dispatcherCtx:     dispCtx,
		dispatcherCancel:  dispCancel,
	}

	// Start auto-rotation on sliding-window Bloom filter
	eng.idempotencyFilter.StartAutoRotation(dispCtx)

	// Initialize Task Queue Manager for distributed workers
	eng.taskQueues = NewTaskQueueManager(store, eng.ResumeTaskWorker, eng.FailTaskWorker)

	// Initialize Distributed Cron Scheduler
	eng.cronScheduler = NewCronWorkflowScheduler(store, eng.Execute, eng.CancelExecution)
	_ = eng.cronScheduler.Start(context.Background())

	// Start background event dispatcher with parallel fan-out
	go eng.eventDispatcherLoop()

	// Start Transactional Outbox Relay for guaranteed at-least-once event delivery
	if store != nil && store.Outbox() != nil {
		eng.outboxRelay = NewOutboxRelay(store, func(event model.WorkflowEvent) {
			eng.listenerMu.RLock()
			listeners := make([]EventListener, len(eng.listeners))
			copy(listeners, eng.listeners)
			eng.listenerMu.RUnlock()
			for _, l := range listeners {
				go l(event)
			}
		})
		eng.outboxRelay.Start(dispCtx)
	}

	// Start DLQ Worker for automatic retry of dead-lettered executions
	eng.dlqWorker = NewDLQWorker(eng, store)
	eng.dlqWorker.Start(dispCtx)

	// Start saturation metrics monitor (SemaphoreUtilization + EventChannelFillRatio)
	go eng.saturationMetricsLoop(dispCtx)

	return eng
}

// Close stops the engine's background workers.
func (e *Engine) Close() {
	if e.outboxRelay != nil {
		e.outboxRelay.Stop()
	}
	if e.idempotencyFilter != nil {
		e.idempotencyFilter.Stop()
	}
	if e.dispatcherCancel != nil {
		e.dispatcherCancel()
	}
}

// SubscribeEventListener adds a listener for real-time workflow events (e.g. WebSockets).
func (e *Engine) SubscribeEventListener(l EventListener) {
	e.listenerMu.Lock()
	defer e.listenerMu.Unlock()
	e.listeners = append(e.listeners, l)
}

// emitEvent enqueues an event into the transactional outbox and the bounded event channel.
func (e *Engine) emitEvent(event model.WorkflowEvent) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Guaranteed at-least-once persistence via Transactional Outbox
	if e.store != nil && e.store.Outbox() != nil {
		_ = e.store.Outbox().Push(context.Background(), &event)
	}

	select {
	case e.eventChan <- event:
	default:
		// Drop from bounded in-memory buffer if consumer is stalled; OutboxRelay guarantees eventual delivery
	}
}

// eventDispatcherLoop dequeues events from the bounded channel and fans them out
// to all registered listeners in parallel, each bounded by a 50ms deadline.
func (e *Engine) eventDispatcherLoop() {
	const listenerTimeout = 50 * time.Millisecond

	for {
		select {
		case <-e.dispatcherCtx.Done():
			return
		case ev, ok := <-e.eventChan:
			if !ok {
				return
			}

			// Snapshot the listener slice under read lock to avoid holding the
			// lock across potentially slow goroutine launches.
			e.listenerMu.RLock()
			listeners := make([]EventListener, len(e.listeners))
			copy(listeners, e.listeners)
			e.listenerMu.RUnlock()

			if len(listeners) == 0 {
				continue
			}

			var wg sync.WaitGroup
			for _, l := range listeners {
				wg.Add(1)
				go func(listener EventListener) {
					defer wg.Done()
					defer func() { _ = recover() }()

					done := make(chan struct{}, 1)
					go func() {
						listener(ev)
						done <- struct{}{}
					}()
					select {
					case <-done:
					case <-time.After(listenerTimeout):
						observability.GlobalMetrics.SlowListenerDrops.Inc()
					}
				}(l)
			}
			// Fire-and-forget: don't block the dispatch loop waiting for completion.
			go wg.Wait()
		}
	}
}

// saturationMetricsLoop publishes semaphore utilization and event channel fill ratio
// to Prometheus every 5 seconds. These are the key "Saturation" Golden Signals.
func (e *Engine) saturationMetricsLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			used := float64(len(e.semaphore))
			capacity := float64(e.semaphoreCapacity)
			if capacity > 0 {
				observability.GlobalMetrics.SemaphoreUtilization.Set(used / capacity)
			}
			chanUsed := float64(len(e.eventChan))
			chanCap := float64(cap(e.eventChan))
			if chanCap > 0 {
				observability.GlobalMetrics.EventChannelFillRatio.Set(chanUsed / chanCap)
			}
		}
	}
}

// ValidateWorkflow checks graph integrity, target existence, and detects directed cycles.
func (e *Engine) ValidateWorkflow(wf *model.Workflow) error {
	if wf.ID == "" {
		return fmt.Errorf("%w: missing workflow id", ErrWorkflowInvalid)
	}
	if len(wf.Stages) == 0 {
		return fmt.Errorf("%w: workflow has no stages", ErrWorkflowInvalid)
	}
	if wf.StartAt == "" {
		wf.StartAt = wf.Stages[0].ID
	}

	stageMap := make(map[string]bool)
	adj := make(map[string][]string)

	for _, s := range wf.Stages {
		if s.ID == "" {
			return fmt.Errorf("%w: found stage with empty id", ErrWorkflowInvalid)
		}
		if stageMap[s.ID] {
			return fmt.Errorf("%w: duplicate stage id '%s'", ErrWorkflowInvalid, s.ID)
		}
		stageMap[s.ID] = true

		// Build adjacency list for graph traversal
		var targets []string
		targets = append(targets, s.Next...)
		for _, b := range s.Branches {
			if b.Target != "" {
				targets = append(targets, b.Target)
			}
		}
		adj[s.ID] = targets
	}

	if !stageMap[wf.StartAt] {
		return fmt.Errorf("%w: start_at stage '%s' not found in workflow", ErrWorkflowInvalid, wf.StartAt)
	}

	// 1. Verify all edges point to existing stages (no dangling references)
	for fromID, targets := range adj {
		for _, toID := range targets {
			if !stageMap[toID] {
				return fmt.Errorf("%w: stage '%s' references non-existent target stage '%s'", ErrWorkflowInvalid, fromID, toID)
			}
		}
	}

	// 2. Cycle Detection via DFS 3-Coloring: 0 = White (unvisited), 1 = Gray (visiting), 2 = Black (visited)
	colors := make(map[string]int)

	var hasCycleDFS func(u string) error
	hasCycleDFS = func(u string) error {
		colors[u] = 1 // Mark Gray (currently in recursion stack)
		for _, v := range adj[u] {
			if colors[v] == 1 {
				return fmt.Errorf("%w: cycle detected in workflow graph involving transition '%s' -> '%s'", ErrWorkflowInvalid, u, v)
			}
			if colors[v] == 0 {
				if err := hasCycleDFS(v); err != nil {
					return err
				}
			}
		}
		colors[u] = 2 // Mark Black (finished)
		return nil
	}

	// Traverse from StartAt first
	if err := hasCycleDFS(wf.StartAt); err != nil {
		return err
	}

	// Check remaining subgraphs/islands
	for _, s := range wf.Stages {
		if colors[s.ID] == 0 {
			if err := hasCycleDFS(s.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

// ExecuteChild runs a child sub-workflow with parent execution linkage established prior to goroutine dispatch.
func (e *Engine) ExecuteChild(ctx context.Context, workflowID string, input map[string]interface{}, parentExecID, parentStageID string) (*model.Execution, error) {
	wf, err := e.store.Workflows().Get(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("workflow '%s' not found: %w", workflowID, err)
	}

	now := time.Now()
	exec := &model.Execution{
		ID:                fmt.Sprintf("exec_%s_%s", wf.ID, uuid.New().String()[:8]),
		WorkflowID:        wf.ID,
		WorkflowName:      wf.Name,
		TriggerType:       model.TriggerTypeChildWorkflow,
		Status:            model.ExecutionStatusRunning,
		Input:             input,
		Variables:         wf.Variables,
		Steps:             make([]*model.StepExecution, 0),
		TraceID:           uuid.New().String(),
		StartedAt:         now,
		ParentExecutionID: parentExecID,
		ParentStageID:     parentStageID,
	}

	if err := e.store.Executions().Create(ctx, exec); err != nil {
		return nil, fmt.Errorf("failed to create child execution record: %w", err)
	}

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventExecutionStarted,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"input":             input,
			"parent_execution": parentExecID,
		},
	})

	observability.GlobalMetrics.ActiveExecutions.Inc()

	execCtxCopy := ctx
	go func() {
		e.semaphore <- struct{}{}
		defer func() { <-e.semaphore }()
		e.runWorkflow(execCtxCopy, wf, exec)
	}()

	return exec, nil
}

// Execute starts execution of a workflow with the given initial input payload.
func (e *Engine) Execute(ctx context.Context, workflowID string, input map[string]interface{}, triggerType model.TriggerType) (*model.Execution, error) {
	wf, err := e.store.Workflows().Get(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workflow: %w", err)
	}

	if err := e.ValidateWorkflow(wf); err != nil {
		return nil, err
	}

	if input == nil {
		input = make(map[string]interface{})
	}

	// Idempotency check: if caller supplied __idempotency_key in the input payload,
	// check existing execution.
	var idempotencyKey string
	if v, ok := input["__idempotency_key"]; ok {
		if s, ok := v.(string); ok && s != "" {
			idempotencyKey = s
			if e.idempotencyFilter.Contains(idempotencyKey) {
				existing, err := e.store.Executions().GetByIdempotencyKey(ctx, workflowID, idempotencyKey)
				if err == nil {
					return existing, nil
				}
				if !errors.Is(err, storage.ErrExecutionNotFound) {
					return nil, fmt.Errorf("idempotency check failed: %w", err)
				}
			}
		}
	}

	execID := fmt.Sprintf("exec_%s_%s", wf.ID, uuid.New().String()[:8])
	now := time.Now()

	// Clone initial variables from workflow definition
	variables := make(map[string]interface{})
	for k, v := range wf.Variables {
		variables[k] = v
	}

	// Extract and merge execution-time variables if provided in structured payload
	if structuredInput, ok := input["input"].(map[string]interface{}); ok {
		if reqVars, ok := input["variables"].(map[string]interface{}); ok {
			for k, v := range reqVars {
				variables[k] = v
			}
		}
		input = structuredInput
	}

	exec := &model.Execution{
		ID:             execID,
		WorkflowID:     wf.ID,
		WorkflowName:   wf.Name,
		TriggerType:    triggerType,
		Status:         model.ExecutionStatusRunning,
		IdempotencyKey: idempotencyKey,
		Input:          input,
		Variables:      variables,
		Steps:          make([]*model.StepExecution, 0),
		TraceID:        uuid.New().String(),
		StartedAt:      now,
	}

	if err := e.store.Executions().Create(ctx, exec); err != nil {
		return nil, fmt.Errorf("failed to create execution record: %w", err)
	}

	// Register key in in-memory Bloom Filter
	if idempotencyKey != "" {
		e.idempotencyFilter.Add(idempotencyKey)
	}

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventExecutionStarted,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"input": input,
		},
	})

	// Track active executions gauge
	observability.GlobalMetrics.ActiveExecutions.Inc()

	// Run execution asynchronously via worker pool.
	execCtxCopy := ctx
	go func() {
		e.semaphore <- struct{}{}
		defer func() { <-e.semaphore }()
		e.runWorkflow(execCtxCopy, wf, exec)
	}()

	return exec, nil
}

func hasParallelWaves(waves []ExecutionWave) bool {
	for _, w := range waves {
		if len(w) > 1 {
			return true
		}
	}
	return false
}

func canExecuteAsWaves(wf *model.Workflow) bool {
	for _, s := range wf.Stages {
		if s.Type == model.StageTypeExclusiveXOR ||
			s.Type == model.StageTypeDelay ||
			s.Type == model.StageTypeApproval ||
			s.Type == model.StageTypeWorkerTask ||
			s.Type == model.StageTypeSubflow {
			return false
		}
	}
	return true
}

// runWorkflowWaves executes a DAG workflow by dependency waves.
func (e *Engine) runWorkflowWaves(
	ctx context.Context,
	wf *model.Workflow,
	exec *model.Execution,
	waves []ExecutionWave,
) {
	defer observability.GlobalMetrics.ActiveExecutions.Dec()

	tr := observability.Tracer
	if tr == nil {
		tr = otel.GetTracerProvider().Tracer("openflow-engine")
	}

	spanCtx, rootSpan := tr.Start(ctx, fmt.Sprintf("WorkflowWaveDAG: %s", wf.ID),
		trace.WithAttributes(
			attribute.String("workflow.id", wf.ID),
			attribute.String("workflow.name", wf.Name),
			attribute.String("execution.id", exec.ID),
			attribute.String("trigger.type", string(exec.TriggerType)),
			attribute.Int("dag.waves", len(waves)),
		),
	)
	defer rootSpan.End()

	var execMu sync.Mutex
	rollingMerkle := security.NewRollingMerkleTree()
	stepsMap := make(map[string]interface{})
	currentPayload := exec.Input

	// Active execution heartbeat tracking
	initHb := time.Now()
	exec.HeartbeatAt = &initHb
	_ = e.store.Executions().UpdateHeartbeat(ctx, exec.ID, initHb)

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				hb := time.Now()
				_ = e.store.Executions().UpdateHeartbeat(heartbeatCtx, exec.ID, hb)
			}
		}
	}()

	var execErr error

	for waveIdx, wave := range waves {
		if execErr != nil {
			break
		}

		type waveStageResult struct {
			stageID string
			step    *model.StepExecution
			output  map[string]interface{}
			err     error
		}

		results := make([]waveStageResult, len(wave))
		var wg sync.WaitGroup

		for i, stageID := range wave {
			wg.Add(1)
			go func(idx int, sID string) {
				defer wg.Done()

				stage, found := wf.FindStage(sID)
				if !found {
					results[idx] = waveStageResult{
						stageID: sID,
						err:     fmt.Errorf("stage '%s' in wave %d not found", sID, waveIdx),
					}
					return
				}

				execMu.Lock()
				env := map[string]interface{}{
					"payload":   currentPayload,
					"input":     exec.Input,
					"variables": exec.Variables,
					"steps":     stepsMap,
				}
				execMu.Unlock()

				stepExec, out, err := e.executeStage(spanCtx, wf, exec, stage, currentPayload, env)
				results[idx] = waveStageResult{
					stageID: sID,
					step:    stepExec,
					output:  out,
					err:     err,
				}
			}(i, stageID)
		}

		wg.Wait()

		// Consolidate wave outputs: preserve previous payload and layer on wave outputs
		waveOutput := make(map[string]interface{})
		for k, v := range currentPayload {
			waveOutput[k] = v
		}

		execMu.Lock()
		for _, res := range results {
			if res.step != nil {
				rollingMerkle.AppendStep(res.step)
				exec.Steps = append(exec.Steps, res.step)
				exec.MerkleRoot = rollingMerkle.CurrentRoot()
				_ = e.store.Executions().AppendStep(ctx, exec.ID, res.step)
				if res.output != nil {
					stepsMap[res.stageID] = res.output
					for k, v := range res.output {
						waveOutput[k] = v
					}
				}
			}
			if res.err != nil && execErr == nil {
				execErr = res.err
			}
		}
		execMu.Unlock()

		if execErr != nil {
			break
		}

		currentPayload = waveOutput
	}

	finishTime := time.Now()
	exec.CompletedAt = &finishTime
	exec.DurationMs = finishTime.Sub(exec.StartedAt).Milliseconds()
	durationSec := float64(exec.DurationMs) / 1000.0

	if execErr != nil {
		exec.Status = model.ExecutionStatusFailed
		exec.Error = execErr.Error()

		dlqMsg := &model.DLQMessage{
			ID:           fmt.Sprintf("dlq_%s_%d", exec.ID, time.Now().UnixNano()),
			WorkflowID:   wf.ID,
			Source:       string(exec.TriggerType),
			TopicOrPath:  fmt.Sprintf("/workflows/%s/execute", wf.ID),
			Payload:      exec.Input,
			ErrorMessage: execErr.Error(),
			Status:       model.DLQStatusPending,
			Attempts:     len(exec.Steps),
			CreatedAt:    finishTime,
		}
		_ = e.store.DLQ().Push(ctx, dlqMsg)
		observability.GlobalMetrics.DLQMessagesTotal.WithLabelValues(wf.ID, "execution_failure").Inc()

		env := map[string]interface{}{
			"payload":   currentPayload,
			"input":     exec.Input,
			"variables": exec.Variables,
			"steps":     stepsMap,
		}
		if rollbackErr := e.saga.Rollback(spanCtx, wf, exec, env, e.executeCompensationStep, e.emitEvent); rollbackErr != nil {
			slog.ErrorContext(ctx, "saga rollback encountered failures",
				slog.String("execution_id", exec.ID),
				slog.String("error", rollbackErr.Error()),
			)
		}

		for _, step := range exec.Steps {
			if step.IsCompensation {
				_ = e.store.Executions().AppendStep(ctx, exec.ID, step)
			}
		}

		observability.GlobalMetrics.ExecutionsFailed.WithLabelValues(wf.ID, "wave_execution_error").Inc()
		observability.GlobalMetrics.ExecutionsFinished.WithLabelValues(wf.ID, string(exec.Status)).Inc()
		observability.GlobalMetrics.ExecutionDuration.WithLabelValues(wf.ID, string(exec.TriggerType), string(exec.Status)).Observe(durationSec)

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventExecutionFailed,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"error": execErr.Error(),
			},
		})
	} else {
		exec.Status = model.ExecutionStatusCompleted
		exec.Output = currentPayload

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventExecutionCompleted,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"output": currentPayload,
			},
		})

		observability.GlobalMetrics.ExecutionsFinished.WithLabelValues(wf.ID, string(exec.Status)).Inc()
		observability.GlobalMetrics.ExecutionDuration.WithLabelValues(wf.ID, string(exec.TriggerType), string(exec.Status)).Observe(durationSec)
	}

	if rollingMerkle.CurrentRoot() != "" && len(exec.Steps) == len(rollingMerkle.Leaves()) {
		exec.MerkleRoot = rollingMerkle.CurrentRoot()
	} else {
		merkleRoot, _ := security.BuildMerkleTree(exec.Steps)
		exec.MerkleRoot = merkleRoot
	}

	execMu.Lock()
	if storeErr := e.store.Executions().Update(ctx, exec); storeErr != nil {
		slog.ErrorContext(ctx, "failed to update wave DAG execution final state in storage",
			slog.String("execution_id", exec.ID),
			slog.String("error", storeErr.Error()),
		)
	}
	execMu.Unlock()
}

// runWorkflow executes the stages of the workflow along the graph from the start stage.
// If the workflow is a DAG with parallel branches, it executes stages by dependency waves.
func (e *Engine) runWorkflow(ctx context.Context, wf *model.Workflow, exec *model.Execution) {
	if waves, err := PartitionWorkflowWaves(wf); err == nil && hasParallelWaves(waves) && canExecuteAsWaves(wf) {
		e.runWorkflowWaves(ctx, wf, exec, waves)
		return
	}
	e.runWorkflowFromStage(ctx, wf, exec, wf.StartAt, exec.Input, nil)
}

// runWorkflowFromStage executes the workflow starting from a designated stage ID (used for both normal and replayed executions).
func (e *Engine) runWorkflowFromStage(
	ctx context.Context,
	wf *model.Workflow,
	exec *model.Execution,
	startStageID string,
	initialOutput map[string]interface{},
	initialStepsMap map[string]interface{},
) {
	defer observability.GlobalMetrics.ActiveExecutions.Dec()

	// OpenTelemetry Root Span
	tr := observability.Tracer
	if tr == nil {
		tr = otel.GetTracerProvider().Tracer("openflow-engine")
	}

	spanCtx, rootSpan := tr.Start(ctx, fmt.Sprintf("Workflow: %s", wf.ID),
		trace.WithAttributes(
			attribute.String("workflow.id", wf.ID),
			attribute.String("workflow.name", wf.Name),
			attribute.String("execution.id", exec.ID),
			attribute.String("trigger.type", string(exec.TriggerType)),
		),
	)
	defer rootSpan.End()

	var execMu sync.Mutex
	stepsMap := make(map[string]interface{})
	if initialStepsMap != nil {
		for k, v := range initialStepsMap {
			stepsMap[k] = v
		}
	}

	// Active execution heartbeat tracking: update timestamp so Watchdog knows this pod is alive
	initHb := time.Now()
	exec.HeartbeatAt = &initHb
	_ = e.store.Executions().UpdateHeartbeat(ctx, exec.ID, initHb)

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				hb := time.Now()
				_ = e.store.Executions().UpdateHeartbeat(heartbeatCtx, exec.ID, hb)
			}
		}
	}()

	rollingMerkle := security.NewRollingMerkleTree()
	currentStageID := startStageID
	var lastOutput map[string]interface{} = initialOutput
	if lastOutput == nil {
		lastOutput = exec.Input
	}
	var execErr error

	// Prepare execution environment for expression evaluation
	buildEnv := func() map[string]interface{} {
		execMu.Lock()
		defer execMu.Unlock()
		currPayload := lastOutput
		if currPayload == nil {
			currPayload = exec.Input
		}
		return map[string]interface{}{
			"payload":   currPayload,
			"input":     exec.Input,
			"variables": exec.Variables,
			"steps":     stepsMap,
		}
	}

	visitedStages := make(map[string]bool)

	for currentStageID != "" {
		stage, found := wf.FindStage(currentStageID)
		if !found {
			execErr = fmt.Errorf("stage '%s' not found", currentStageID)
			break
		}

		visitedStages[currentStageID] = true
		env := buildEnv()

				// StageTypeDelay: Persistent Durable Timer (survives crashes & restarts)
		if stage.Type == model.StageTypeDelay {
			delayDuration := 1 * time.Second
			if dStr, ok := stage.Config["duration"].(string); ok && dStr != "" {
				if d, err := time.ParseDuration(dStr); err == nil {
					delayDuration = d
				}
			}

			stepExec := &model.StepExecution{
				ID:        fmt.Sprintf("step_%s_%d", stage.ID, time.Now().UnixNano()),
				StageID:   stage.ID,
				StageName: stage.Name,
				StageType: stage.Type,
				Status:    model.StepStatusWaitingTimer,
				Input:     lastOutput,
				StartedAt: time.Now(),
				Attempts:  1,
			}
			execMu.Lock()
			exec.Steps = append(exec.Steps, stepExec)
			exec.Status = model.ExecutionStatusWaitingTimer
			_ = e.store.Executions().Update(ctx, exec)
			_ = e.store.Executions().AppendStep(ctx, exec.ID, stepExec)
			execMu.Unlock()

			timerID := fmt.Sprintf("tmr_%s_%s", exec.ID, stage.ID)
			timer := &model.DurableTimer{
				ID:          timerID,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				FireAt:      time.Now().Add(delayDuration),
				Status:      model.TimerStatusPending,
				CreatedAt:   time.Now(),
			}
			_ = e.store.Timers().Create(ctx, timer)

			e.emitEvent(model.WorkflowEvent{
				Type:        model.EventExecutionWaitingTimer,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				Timestamp:   time.Now(),
				Payload: map[string]interface{}{
					"timer_id": timerID,
					"fire_at":  timer.FireAt.Format(time.RFC3339),
				},
			})

			return
		}

		// StageTypeWaitForSignal: External Event / Signal Injection
		if stage.Type == model.StageTypeWaitForSignal {
			sigName, _ := stage.Config["signal_name"].(string)
			if sigName == "" {
				sigName = fmt.Sprintf("sig_%s", stage.ID)
			}

			stepExec := &model.StepExecution{
				ID:        fmt.Sprintf("step_%s_%d", stage.ID, time.Now().UnixNano()),
				StageID:   stage.ID,
				StageName: stage.Name,
				StageType: stage.Type,
				Status:    model.StepStatusWaitingSignal,
				Input:     lastOutput,
				StartedAt: time.Now(),
				Attempts:  1,
			}
			execMu.Lock()
			exec.Steps = append(exec.Steps, stepExec)
			exec.Status = model.ExecutionStatusWaitingSignal
			_ = e.store.Executions().Update(ctx, exec)
			_ = e.store.Executions().AppendStep(ctx, exec.ID, stepExec)
			execMu.Unlock()

			var timeoutAt *time.Time
			if toStr, ok := stage.Config["timeout"].(string); ok && toStr != "" {
				if d, err := time.ParseDuration(toStr); err == nil {
					t := time.Now().Add(d)
					timeoutAt = &t
				}
			}

			sigID := fmt.Sprintf("sig_%s_%s", exec.ID, stage.ID)
			sig := &model.SignalSubscription{
				ID:          sigID,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				SignalName:  sigName,
				Status:      model.SignalStatusWaiting,
				TimeoutAt:   timeoutAt,
				CreatedAt:   time.Now(),
			}
			_ = e.store.Signals().Create(ctx, sig)

			e.emitEvent(model.WorkflowEvent{
				Type:        model.EventExecutionWaitingSignal,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				Timestamp:   time.Now(),
				Payload: map[string]interface{}{
					"signal_id":   sigID,
					"signal_name": sigName,
				},
			})

			return
		}

		// StageTypeChildWorkflow: Hierarchical Sub-Workflow Invocation
		if stage.Type == model.StageTypeChildWorkflow {
			childWfID, _ := stage.Config["workflow_id"].(string)
			childInput := lastOutput
			if inputMapping, ok := stage.Config["input"].(map[string]interface{}); ok {
				childInput = e.evaluator.EvaluateMapping(inputMapping, env)
			}

			stepExec := &model.StepExecution{
				ID:        fmt.Sprintf("step_%s_%d", stage.ID, time.Now().UnixNano()),
				StageID:   stage.ID,
				StageName: stage.Name,
				StageType: stage.Type,
				Status:    model.StepStatusWaitingChild,
				Input:     childInput,
				StartedAt: time.Now(),
				Attempts:  1,
			}
			execMu.Lock()
			exec.Steps = append(exec.Steps, stepExec)
			exec.Status = model.ExecutionStatusWaitingChild
			_ = e.store.Executions().Update(ctx, exec)
			_ = e.store.Executions().AppendStep(ctx, exec.ID, stepExec)
			execMu.Unlock()

			// Trigger child execution with parent linkage
			childExec, err := e.ExecuteChild(ctx, childWfID, childInput, exec.ID, stage.ID)
			if err != nil {
				execErr = fmt.Errorf("failed to trigger child workflow '%s': %w", childWfID, err)
				break
			}

			e.emitEvent(model.WorkflowEvent{
				Type:        model.EventChildWorkflowStarted,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				Timestamp:   time.Now(),
				Payload: map[string]interface{}{
					"child_workflow_id":  childWfID,
					"child_execution_id": childExec.ID,
				},
			})

			return
		}

				// StageTypeWorkerTask: External Worker Activity Task (Distributed Task Queue & Leases)
		if stage.Type == model.StageTypeWorkerTask {
			stepExec := &model.StepExecution{
				ID:        fmt.Sprintf("step_%s_%d", stage.ID, time.Now().UnixNano()),
				StageID:   stage.ID,
				StageName: stage.Name,
				StageType: stage.Type,
				Status:    model.StepStatusWaitingWorker,
				Input:     lastOutput,
				StartedAt: time.Now(),
				Attempts:  1,
			}
			execMu.Lock()
			exec.Steps = append(exec.Steps, stepExec)
			exec.Status = model.ExecutionStatusWaitingWorker
			_ = e.store.Executions().Update(ctx, exec)
			_ = e.store.Executions().AppendStep(ctx, exec.ID, stepExec)
			execMu.Unlock()

			queueName := "default"
			if qn, ok := stage.Config["queue_name"].(string); ok && qn != "" {
				queueName = qn
			}

			hbTimeout := 30 * time.Second
			if hbStr, ok := stage.Config["heartbeat_timeout"].(string); ok {
				if d, err := time.ParseDuration(hbStr); err == nil {
					hbTimeout = d
				}
			}

			task := &model.TaskItem{
				QueueName:        queueName,
				WorkflowID:       wf.ID,
				ExecutionID:      exec.ID,
				StageID:          stage.ID,
				Input:            lastOutput,
				HeartbeatTimeout: hbTimeout,
				Status:           model.TaskStatusPending,
			}

			if err := e.taskQueues.Enqueue(ctx, task); err != nil {
				slog.ErrorContext(ctx, "failed to enqueue worker task",
					slog.String("execution_id", exec.ID),
					slog.String("stage_id", stage.ID),
					slog.String("error", err.Error()),
				)
			}

			e.emitEvent(model.WorkflowEvent{
				Type:        model.EventType("task.enqueued"),
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				Timestamp:   time.Now(),
				Payload: map[string]interface{}{
					"queue_name": queueName,
					"task_id":    task.ID,
					"input":      lastOutput,
				},
			})

			return
		}

		// StageTypeApproval: Human-in-the-loop manual review gate
		if stage.Type == model.StageTypeApproval {
			stepExec := &model.StepExecution{
				ID:        fmt.Sprintf("step_%s_%d", stage.ID, time.Now().UnixNano()),
				StageID:   stage.ID,
				StageName: stage.Name,
				StageType: stage.Type,
				Status:    model.StepStatusWaitingApproval,
				Input:     lastOutput,
				StartedAt: time.Now(),
				Attempts:  1,
			}
			execMu.Lock()
			exec.Steps = append(exec.Steps, stepExec)
			exec.Status = model.ExecutionStatusWaitingApproval
			_ = e.store.Executions().Update(ctx, exec)
			_ = e.store.Executions().AppendStep(ctx, exec.ID, stepExec)
			execMu.Unlock()

			title := fmt.Sprintf("Approval required: %s", stage.Name)
			if t, ok := stage.Config["title"].(string); ok && t != "" {
				title = t
			}
			desc := fmt.Sprintf("Execution %s waiting for manual approval on stage %s", exec.ID, stage.Name)
			if d, ok := stage.Config["description"].(string); ok && d != "" {
				desc = d
			}
			role := "operator"
			if r, ok := stage.Config["required_role"].(string); ok && r != "" {
				role = r
			}

			approvalID := fmt.Sprintf("appr_%s_%s", exec.ID, stage.ID)
			approvalReq := &model.ApprovalRequest{
				ID:           approvalID,
				ExecutionID:  exec.ID,
				WorkflowID:   wf.ID,
				WorkflowName: wf.Name,
				StageID:      stage.ID,
				StageName:    stage.Name,
				Title:        title,
				Description:  desc,
				Payload:      lastOutput,
				RequiredRole: role,
				Status:       model.ApprovalStatusPending,
				CreatedAt:    time.Now(),
			}
			_ = e.store.Approvals().Create(ctx, approvalReq)

			e.emitEvent(model.WorkflowEvent{
				Type:        model.EventExecutionWaitingApproval,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				Timestamp:   time.Now(),
				Payload: map[string]interface{}{
					"approval_id": approvalID,
					"stage_name":  stage.Name,
					"payload":     lastOutput,
				},
			})

			return
		}

		// Execute the stage with retries and tracing
		stepExec, output, err := e.executeStage(spanCtx, wf, exec, stage, lastOutput, env)

		execMu.Lock()
		rollingMerkle.AppendStep(stepExec)
		exec.Steps = append(exec.Steps, stepExec)
		exec.MerkleRoot = rollingMerkle.CurrentRoot()
		if output != nil {
			stepsMap[stage.ID] = map[string]interface{}{
				"output":   output,
				"status":   stepExec.Status,
				"duration": stepExec.DurationMs,
			}
			lastOutput = output
		}
		// Persist new step row
		if storeErr := e.store.Executions().AppendStep(ctx, exec.ID, stepExec); storeErr != nil {
			slog.ErrorContext(ctx, "failed to append step execution delta to storage",
				slog.String("execution_id", exec.ID),
				slog.String("stage_id", stage.ID),
				slog.String("error", storeErr.Error()),
			)
		}
		execMu.Unlock()

		if err != nil {
			execErr = fmt.Errorf("stage '%s' failed: %w", stage.ID, err)
			break
		}

		// Handle branching / next stage resolution
		nextStage := e.resolveNextStage(stage, env, output)

		// Execute parallel branches concurrently
		if stage.Type == model.StageTypeParallelFork && len(stage.Next) > 1 {
			branchOutput, branchErr := e.runParallelBranches(spanCtx, wf, exec, stage.Next, lastOutput, &execMu, stepsMap)
			if branchErr != nil {
				execErr = branchErr
				break
			}
			if branchOutput != nil {
				lastOutput = branchOutput
			}
			// After parallel join, find the ParallelJoin stage that comes after the branches
			currentStageID = e.findParallelJoin(wf, stage.Next)
			continue
		}

		currentStageID = nextStage
	}

	finishTime := time.Now()
	exec.CompletedAt = &finishTime
	exec.DurationMs = finishTime.Sub(exec.StartedAt).Milliseconds()
	durationSec := float64(exec.DurationMs) / 1000.0

	if execErr != nil {
				exec.Status = model.ExecutionStatusFailed
		exec.Error = execErr.Error()

		// Push to Dead Letter Queue on execution failure
		dlqMsg := &model.DLQMessage{
			ID:           fmt.Sprintf("dlq_%s_%d", exec.ID, time.Now().UnixNano()),
			WorkflowID:   wf.ID,
			Source:       string(exec.TriggerType),
			TopicOrPath:  fmt.Sprintf("/workflows/%s/execute", wf.ID),
			Payload:      exec.Input,
			ErrorMessage: execErr.Error(),
			Status:       model.DLQStatusPending,
			Attempts:     len(exec.Steps),
			CreatedAt:    finishTime,
		}
		if pushErr := e.store.DLQ().Push(ctx, dlqMsg); pushErr != nil {
			slog.ErrorContext(ctx, "failed to auto-push execution error to DLQ",
				slog.String("execution_id", exec.ID),
				slog.String("error", pushErr.Error()),
			)
		}

		rootSpan.RecordError(execErr)
		rootSpan.SetStatus(codes.Error, execErr.Error())

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventExecutionFailed,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"error": execErr.Error(),
			},
		})

		// Trigger Saga Rollback if any stage defined compensations
		env := buildEnv()
		if rollbackErr := e.saga.Rollback(spanCtx, wf, exec, env, e.executeCompensationStep, e.emitEvent); rollbackErr != nil {
			slog.ErrorContext(ctx, "saga rollback encountered failures",
				slog.String("execution_id", exec.ID),
				slog.String("error", rollbackErr.Error()),
			)
		}

		// Persist compensation steps via delta-write (Rollback mutates exec.Steps in-memory)
		for _, step := range exec.Steps {
			if step.IsCompensation {
				if storeErr := e.store.Executions().AppendStep(ctx, exec.ID, step); storeErr != nil {
					slog.ErrorContext(ctx, "failed to append compensation step to storage",
						slog.String("execution_id", exec.ID),
						slog.String("step_id", step.ID),
						slog.String("error", storeErr.Error()),
					)
				}
			}
		}

		// Record metrics on failure
		observability.GlobalMetrics.ExecutionsFinished.WithLabelValues(wf.ID, string(exec.Status)).Inc()
		observability.GlobalMetrics.ExecutionDuration.WithLabelValues(wf.ID, string(exec.TriggerType), string(exec.Status)).Observe(durationSec)
		observability.GlobalMetrics.SagaRollbacks.WithLabelValues(wf.ID, "parallel").Inc()
	} else {
		exec.Status = model.ExecutionStatusCompleted
		exec.Output = lastOutput

		rootSpan.SetStatus(codes.Ok, "Workflow Completed")
		// If this is a child execution, notify parent
		if exec.ParentExecutionID != "" {
			go func(pID string, out map[string]interface{}) {
				_ = e.ResumeChildWorkflow(context.Background(), pID, out)
			}(exec.ParentExecutionID, lastOutput)
		}
		// Record audit log entry for completion
		_ = e.store.Audits().Log(ctx, &model.AuditLogEntry{
			ID:         fmt.Sprintf("aud_comp_%s", exec.ID),
			Timestamp:  finishTime,
			Actor:      "engine:orchestrator",
			Action:     "WORKFLOW_COMPLETED",
			Resource:   "execution",
			ResourceID: exec.ID,
			ClientIP:   "127.0.0.1",
			Details: map[string]interface{}{
				"workflow_id":   wf.ID,
				"workflow_name": wf.Name,
				"status":        string(exec.Status),
				"duration_ms":   exec.DurationMs,
				"merkle_root":   exec.MerkleRoot,
				"step_count":    len(exec.Steps),
				"output":        lastOutput,
			},
		})

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventExecutionCompleted,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"output": lastOutput,
			},
		})

		// Record metrics on completion
		observability.GlobalMetrics.ExecutionsFinished.WithLabelValues(wf.ID, string(exec.Status)).Inc()
		observability.GlobalMetrics.ExecutionDuration.WithLabelValues(wf.ID, string(exec.TriggerType), string(exec.Status)).Observe(durationSec)
	}

	// Cryptographic Merkle Root seal
	if rollingMerkle.CurrentRoot() != "" && len(exec.Steps) == len(rollingMerkle.Leaves()) {
		exec.MerkleRoot = rollingMerkle.CurrentRoot()
	} else {
		merkleRoot, _ := security.BuildMerkleTree(exec.Steps)
		exec.MerkleRoot = merkleRoot
	}

	// Update top-level execution state
	execMu.Lock()
	if storeErr := e.store.Executions().Update(ctx, exec); storeErr != nil {
		slog.ErrorContext(ctx, "failed to update execution final state in storage",
			slog.String("execution_id", exec.ID),
			slog.String("error", storeErr.Error()),
		)
	}
	execMu.Unlock()
}

// executeStage runs a single stage with retry policy, template interpolation, metrics, and tracing spans.
func (e *Engine) executeStage(
	ctx context.Context,
	wf *model.Workflow,
	exec *model.Execution,
	stage *model.Stage,
	input map[string]interface{},
	env map[string]interface{},
) (*model.StepExecution, map[string]interface{}, error) {
	startTime := time.Now()
	stepID := fmt.Sprintf("step_%s_%s", stage.ID, uuid.New().String()[:6])

	// Child Span for Stage
	tr := observability.Tracer
	if tr == nil {
		tr = otel.GetTracerProvider().Tracer("openflow-engine")
	}

	stageCtx, stageSpan := tr.Start(ctx, fmt.Sprintf("Stage: %s", stage.Name),
		trace.WithAttributes(
			attribute.String("stage.id", stage.ID),
			attribute.String("stage.name", stage.Name),
			attribute.String("stage.type", string(stage.Type)),
			attribute.String("step.id", stepID),
		),
	)
	defer stageSpan.End()

	step := &model.StepExecution{
		ID:        stepID,
		StageID:   stage.ID,
		StageName: stage.Name,
		StageType: stage.Type,
		Status:    model.StepStatusRunning,
		Attempts:  0,
		Input:     input,
		StartedAt: startTime,
	}

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventStepStarted,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     stage.ID,
		Timestamp:   startTime,
		Payload: map[string]interface{}{
			"stage_name": stage.Name,
			"stage_type": stage.Type,
			"input":      input,
		},
	})

	// Add small micro-pause (80ms) for visual observability and event propagation
	time.Sleep(80 * time.Millisecond)

	// Handle Special BPMN Gateway types
	switch stage.Type {
	case model.StageTypeDelay:
		delayDur := 1 * time.Second
		if dStr, ok := stage.Config["duration"].(string); ok {
			if d, err := time.ParseDuration(dStr); err == nil {
				delayDur = d
			}
		}
		time.Sleep(delayDur)
		finishTime := time.Now()
		step.Status = model.StepStatusCompleted
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		step.Output = input
		stageSpan.SetStatus(codes.Ok, "Delayed")
		observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)
		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventStepCompleted,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			StageID:     stage.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"stage_name":  stage.Name,
				"stage_type":  stage.Type,
				"duration_ms": step.DurationMs,
				"output":      step.Output,
				"status":      step.Status,
			},
		})
		return step, input, nil

	case model.StageTypeExclusiveXOR:
		finishTime := time.Now()
		step.Status = model.StepStatusCompleted
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		step.Output = input
		stageSpan.SetStatus(codes.Ok, "Routed")
		observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)
		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventStepCompleted,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			StageID:     stage.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"stage_name":  stage.Name,
				"stage_type":  stage.Type,
				"duration_ms": step.DurationMs,
				"output":      step.Output,
				"status":      step.Status,
			},
		})
		return step, input, nil

	case model.StageTypeParallelFork:
		finishTime := time.Now()
		step.Status = model.StepStatusCompleted
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		step.Output = input
		stageSpan.SetStatus(codes.Ok, "Forked")
		observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)
		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventStepCompleted,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			StageID:     stage.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"stage_name":  stage.Name,
				"stage_type":  stage.Type,
				"duration_ms": step.DurationMs,
				"output":      step.Output,
				"status":      step.Status,
			},
		})
		return step, input, nil
	}

	// Interpolate stage configuration templates
	interpolatedConfig, err := e.evaluator.InterpolateMap(stage.Config, env)
	if err != nil {
		finishTime := time.Now()
		step.Status = model.StepStatusFailed
		step.Error = fmt.Sprintf("interpolation error: %v", err)
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		stageSpan.RecordError(err)
		stageSpan.SetStatus(codes.Error, step.Error)
		observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)
		return step, nil, err
	}

	// Control Flow Gateway stage types (ParallelJoin, ParallelFork, ExclusiveXOR) are internal to engine
	if stage.Type == model.StageTypeParallelJoin || stage.Type == model.StageTypeParallelFork || stage.Type == model.StageTypeExclusiveXOR {
		finishTime := time.Now()
		step.Status = model.StepStatusCompleted
		step.Output = input
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		stageSpan.SetStatus(codes.Ok, "Gateway Processed")
		return step, input, nil
	}

	// Fetch connector
	connType := model.ConnectorType(stage.Type)
	conn, err := e.registry.Get(connType)
	if err != nil {
		finishTime := time.Now()
		step.Status = model.StepStatusFailed
		step.Error = err.Error()
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		stageSpan.RecordError(err)
		stageSpan.SetStatus(codes.Error, err.Error())
		observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)
		return step, nil, err
	}

	// Check circuit breaker before execution
	breakerKey := fmt.Sprintf("%s:%s", stage.Type, stage.ID)
	cb := e.breakers.GetOrCreate(breakerKey)
	if allowErr := cb.Allow(); allowErr != nil {
		finishTime := time.Now()
		step.Status = model.StepStatusFailed
		step.Error = allowErr.Error()
		step.CompletedAt = &finishTime
		step.DurationMs = finishTime.Sub(startTime).Milliseconds()
		stageSpan.RecordError(allowErr)
		stageSpan.SetStatus(codes.Error, allowErr.Error())
		observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventStepFailed,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			StageID:     stage.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"error": allowErr.Error(),
			},
		})
		return step, nil, allowErr
	}

	var stepsMap map[string]interface{}
	if s, ok := env["steps"].(map[string]interface{}); ok {
		stepsMap = s
	} else {
		stepsMap = make(map[string]interface{})
	}

	execCtx := &connectors.ExecutionContext{
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     stage.ID,
		StageName:   stage.Name,
		Variables:   exec.Variables,
		Payload:     input,
		Steps:       stepsMap,
	}

	// Retry execution loop
	maxAttempts := 1
	if stage.Retry != nil && stage.Retry.MaxAttempts > 1 {
		maxAttempts = stage.Retry.MaxAttempts
	}

	var lastOutput map[string]interface{}
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		step.Attempts = attempt
		execCtx.Attempt = attempt
		idempHash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", exec.ID, stage.ID, attempt)))
		execCtx.IdempotencyKey = hex.EncodeToString(idempHash[:])

		if attempt > 1 {
			step.Status = model.StepStatusRetrying
			e.emitEvent(model.WorkflowEvent{
				Type:        model.EventStepRetrying,
				ExecutionID: exec.ID,
				WorkflowID:  wf.ID,
				StageID:     stage.ID,
				Timestamp:   time.Now(),
				Payload: map[string]interface{}{
					"attempt": attempt,
				},
			})
			backoff := CalculateBackoff(stage.Retry, attempt)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				finishTime := time.Now()
				step.Status = model.StepStatusFailed
				step.Error = ctx.Err().Error()
				step.CompletedAt = &finishTime
				step.DurationMs = finishTime.Sub(startTime).Milliseconds()
				return step, nil, ctx.Err()
			}
		}

		lastOutput, lastErr = conn.Execute(stageCtx, execCtx, interpolatedConfig, input)
		if lastErr == nil {
			break
		}
	}

	finishTime := time.Now()
	step.CompletedAt = &finishTime
	step.DurationMs = finishTime.Sub(startTime).Milliseconds()
	observability.GlobalMetrics.StageDuration.WithLabelValues(string(stage.Type), string(step.Status)).Observe(float64(step.DurationMs) / 1000.0)

	if lastErr != nil {
		cb.RecordFailure()
		step.Status = model.StepStatusFailed
		step.Error = lastErr.Error()
		stageSpan.RecordError(lastErr)
		stageSpan.SetStatus(codes.Error, lastErr.Error())

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventStepFailed,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			StageID:     stage.ID,
			Timestamp:   finishTime,
			Payload: map[string]interface{}{
				"error":    lastErr.Error(),
				"attempts": step.Attempts,
			},
		})
		return step, nil, lastErr
	}

	cb.RecordSuccess()
	step.Status = model.StepStatusCompleted
	step.Output = lastOutput
	stageSpan.SetStatus(codes.Ok, "Stage Completed")

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventStepCompleted,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     stage.ID,
		Timestamp:   finishTime,
		Payload: map[string]interface{}{
			"output":   lastOutput,
			"duration": step.DurationMs,
		},
	})

	return step, lastOutput, nil
}

// executeCompensationStep is called by SagaManager during rollback.
func (e *Engine) executeCompensationStep(
	ctx context.Context,
	comp *model.CompensationConfig,
	originalStep *model.StepExecution,
	env map[string]interface{},
) (map[string]interface{}, error) {
	tr := observability.Tracer
	if tr == nil {
		tr = otel.GetTracerProvider().Tracer("openflow-engine")
	}

	compCtx, compSpan := tr.Start(ctx, fmt.Sprintf("Compensate: %s", comp.Name),
		trace.WithAttributes(
			attribute.String("compensation.id", comp.ID),
			attribute.String("compensation.for_stage", originalStep.StageID),
			attribute.String("stage.type", string(comp.Type)),
		),
	)
	defer compSpan.End()

	connType := model.ConnectorType(comp.Type)
	conn, err := e.registry.Get(connType)
	if err != nil {
		compSpan.RecordError(err)
		compSpan.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	compHash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:comp", originalStep.ID, comp.ID)))
	execCtx := &connectors.ExecutionContext{
		ExecutionID:    originalStep.ID,
		StageID:        comp.ID,
		StageName:      comp.Name,
		Payload:        originalStep.Input,
		Variables:      env["variables"].(map[string]interface{}),
		Steps:          env["steps"].(map[string]interface{}),
		Attempt:        1,
		IdempotencyKey: hex.EncodeToString(compHash[:]),
	}

	out, err := conn.Execute(compCtx, execCtx, comp.Config, originalStep.Output)
	if err != nil {
		compSpan.RecordError(err)
		compSpan.SetStatus(codes.Error, err.Error())
	} else {
		compSpan.SetStatus(codes.Ok, "Compensated")
	}
	return out, err
}

// resolveNextStage determines the next stage ID to transition to.
func (e *Engine) resolveNextStage(stage *model.Stage, env map[string]interface{}, lastOutput map[string]interface{}) string {
	// If Exclusive XOR Gateway with branches
	if stage.Type == model.StageTypeExclusiveXOR && len(stage.Branches) > 0 {
		var defaultTarget string
		for _, b := range stage.Branches {
			if b.Default {
				defaultTarget = b.Target
			}
			if b.Condition != "" {
				matched, err := e.evaluator.EvalCondition(b.Condition, env)
				if err == nil && matched {
					return b.Target
				}
			}
		}
		if defaultTarget != "" {
			return defaultTarget
		}
	}

	// Normal Next step
	if len(stage.Next) > 0 {
		return stage.Next[0]
	}

	return ""
}

// branchResult captures the output and error of a single parallel branch.
type branchResult struct {
	stageID string
	output  map[string]interface{}
	err     error
	steps   []*model.StepExecution
}

// runParallelBranches executes branch stages concurrently.
func (e *Engine) runParallelBranches(
	ctx context.Context,
	wf *model.Workflow,
	exec *model.Execution,
	branchIDs []string,
	input map[string]interface{},
	execMu *sync.Mutex,
	stepsMap map[string]interface{},
) (map[string]interface{}, error) {
	type result struct {
		output map[string]interface{}
		err    error
		steps  []*model.StepExecution
	}

	results := make([]result, len(branchIDs))
	var wg sync.WaitGroup

	for i, branchID := range branchIDs {
		wg.Add(1)
		go func(idx int, startID string) {
			defer wg.Done()
			var branchSteps []*model.StepExecution
			lastOut := input
			currentID := startID

			for currentID != "" {
				stage, found := wf.FindStage(currentID)
				if !found {
					results[idx].err = fmt.Errorf("parallel branch stage '%s' not found", currentID)
					return
				}
				// Stop at join gate; handled by main loop
				if stage.Type == model.StageTypeParallelJoin {
					break
				}

				execMu.Lock()
				env := map[string]interface{}{
					"payload":   exec.Input,
					"variables": exec.Variables,
					"steps":     stepsMap,
				}
				execMu.Unlock()

				stepExec, out, err := e.executeStage(ctx, wf, exec, stage, lastOut, env)
				branchSteps = append(branchSteps, stepExec)

				// Delta-write the branch step immediately
				_ = e.store.Executions().AppendStep(ctx, exec.ID, stepExec)

				if err != nil {
					results[idx].err = fmt.Errorf("branch stage '%s' failed: %w", currentID, err)
					return
				}
				if out != nil {
					lastOut = out
				}
				currentID = e.resolveNextStage(stage, env, out)
			}

			results[idx] = result{output: lastOut, steps: branchSteps}
		}(i, branchID)
	}

	wg.Wait()

	// Collect results: fail fast on any branch error; merge steps and outputs
	var mergedOutput map[string]interface{}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		execMu.Lock()
		exec.Steps = append(exec.Steps, r.steps...)
		execMu.Unlock()
		if r.output != nil {
			mergedOutput = r.output
		}
	}
	return mergedOutput, nil
}

// findParallelJoin finds the first ParallelJoin stage reachable from any of the given branch IDs.
func (e *Engine) findParallelJoin(wf *model.Workflow, branchIDs []string) string {
	branchSet := make(map[string]bool)
	for _, b := range branchIDs {
		branchSet[b] = true
	}

	// 1. Direct join_sources lookup
	for _, stage := range wf.Stages {
		if stage.Type == model.StageTypeParallelJoin {
			for _, src := range stage.JoinSources {
				if branchSet[src] {
					return stage.ID
				}
			}
		}
	}

	// 2. Trace next pointers
	for _, branchID := range branchIDs {
		current := branchID
		for current != "" {
			stage, found := wf.FindStage(current)
			if !found {
				break
			}
			if stage.Type == model.StageTypeParallelJoin {
				return current
			}
			if len(stage.Next) == 0 {
				break
			}
			current = stage.Next[0]
		}
	}
	return ""
}

// ResumeApproval approves or rejects a paused Human-in-the-loop workflow execution.
func (e *Engine) ResumeApproval(ctx context.Context, approvalID string, approved bool, actor string, reason string) error {
	req, err := e.store.Approvals().Get(ctx, approvalID)
	if err != nil {
		return fmt.Errorf("failed to get approval: %w", err)
	}

	if req.Status != model.ApprovalStatusPending {
		return fmt.Errorf("approval is already %s", req.Status)
	}

	now := time.Now()
	req.DecidedBy = actor
	req.DecidedAt = &now
	req.Reason = reason

	if approved {
		req.Status = model.ApprovalStatusApproved
	} else {
		req.Status = model.ApprovalStatusRejected
	}
	_ = e.store.Approvals().Update(ctx, req)

	exec, err := e.store.Executions().Get(ctx, req.ExecutionID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}

	wf, err := e.store.Workflows().Get(ctx, req.WorkflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow: %w", err)
	}

	// Update step execution in instance
	for _, s := range exec.Steps {
		if s.StageID == req.StageID {
			s.CompletedAt = &now
			s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
			if approved {
				s.Status = model.StepStatusCompleted
				s.Output = map[string]interface{}{
					"approved":   true,
					"decided_by": actor,
					"decided_at": now,
				}
			} else {
				s.Status = model.StepStatusFailed
				s.Error = fmt.Sprintf("Rejected by %s: %s", actor, reason)
			}
			_ = e.store.Executions().AppendStep(ctx, exec.ID, s)
			break
		}
	}

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventApprovalDecided,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     req.StageID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"approval_id": approvalID,
			"approved":    approved,
			"actor":       actor,
			"reason":      reason,
		},
	})

	if !approved {
		// Reject -> fail step, trigger saga rollback
		exec.Status = model.ExecutionStatusFailed
		exec.Error = fmt.Sprintf("Approval rejected by %s: %s", actor, reason)
		exec.CompletedAt = &now
		exec.DurationMs = now.Sub(exec.StartedAt).Milliseconds()
		_ = e.store.Executions().Update(ctx, exec)

		e.emitEvent(model.WorkflowEvent{
			Type:        model.EventExecutionFailed,
			ExecutionID: exec.ID,
			WorkflowID:  wf.ID,
			Timestamp:   now,
			Payload: map[string]interface{}{
				"error": exec.Error,
			},
		})
		return nil
	}

	// Approved -> mark running and resume next stages in background goroutine
	exec.Status = model.ExecutionStatusRunning
	_ = e.store.Executions().Update(ctx, exec)

	go func() {
		bgCtx := context.Background()
		stage, found := wf.FindStage(req.StageID)
		if found && len(stage.Next) > 0 {
			nextStageID := stage.Next[0]
			// Resume execution loop from next stage
			_ = e.resumeFromStage(bgCtx, wf, exec, nextStageID, req.Payload)
		}
	}()

	return nil
}

func (e *Engine) resumeFromStage(ctx context.Context, wf *model.Workflow, exec *model.Execution, startStageID string, initialPayload map[string]interface{}) error {
	e.runWorkflowFromStage(ctx, wf, exec, startStageID, initialPayload, nil)
	return nil
}

// ResumeTimer resumes an execution when a durable timer fires.
func (e *Engine) ResumeTimer(ctx context.Context, timer *model.DurableTimer) error {
	exec, err := e.store.Executions().Get(ctx, timer.ExecutionID)
	if err != nil {
		return err
	}
	wf, err := e.store.Workflows().Get(ctx, timer.WorkflowID)
	if err != nil {
		return err
	}

	stage, found := wf.FindStage(timer.StageID)
	if !found {
		return fmt.Errorf("timer stage '%s' not found", timer.StageID)
	}

	// Complete the waiting timer step
	now := time.Now()
	for _, s := range exec.Steps {
		if s.StageID == timer.StageID && s.Status == model.StepStatusWaitingTimer {
			s.Status = model.StepStatusCompleted
			s.CompletedAt = &now
			s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
			_ = e.store.Executions().AppendStep(ctx, exec.ID, s)
			break
		}
	}

	exec.Status = model.ExecutionStatusRunning
	_ = e.store.Executions().Update(ctx, exec)

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventTimerFired,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     timer.StageID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"timer_id": timer.ID,
		},
	})

	// Rebuild env and resume
	lastOutput := exec.Input
	stepsMap := make(map[string]interface{})
	for _, s := range exec.Steps {
		if s.Output != nil {
			lastOutput = s.Output
			stepsMap[s.StageID] = map[string]interface{}{
				"output":   s.Output,
				"status":   s.Status,
				"duration": s.DurationMs,
			}
		}
	}

	env := map[string]interface{}{
		"payload":   lastOutput,
		"input":     exec.Input,
		"variables": exec.Variables,
		"steps":     stepsMap,
	}

	nextStageID := e.resolveNextStage(stage, env, lastOutput)
	go e.resumeFromStage(context.Background(), wf, exec, nextStageID, lastOutput)
	return nil
}

// ResumeSignal resumes an execution when an external signal is received.
func (e *Engine) ResumeSignal(ctx context.Context, execID, signalName string, payload map[string]interface{}) error {
	sig, err := e.store.Signals().FindWaiting(ctx, execID, signalName)
	if err != nil {
		return fmt.Errorf("no execution '%s' waiting for signal '%s'", execID, signalName)
	}

	now := time.Now()
	sig.Status = model.SignalStatusReceived
	sig.Payload = payload
	sig.ReceivedAt = &now
	_ = e.store.Signals().Update(ctx, sig)

	exec, err := e.store.Executions().Get(ctx, execID)
	if err != nil {
		return err
	}
	wf, err := e.store.Workflows().Get(ctx, exec.WorkflowID)
	if err != nil {
		return err
	}

	stage, found := wf.FindStage(sig.StageID)
	if !found {
		return fmt.Errorf("signal stage '%s' not found", sig.StageID)
	}

	for _, s := range exec.Steps {
		if s.StageID == sig.StageID && s.Status == model.StepStatusWaitingSignal {
			s.Status = model.StepStatusCompleted
			s.CompletedAt = &now
			s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
			s.Output = payload
			_ = e.store.Executions().AppendStep(ctx, exec.ID, s)
			break
		}
	}

	exec.Status = model.ExecutionStatusRunning
	_ = e.store.Executions().Update(ctx, exec)

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventSignalReceived,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     sig.StageID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"signal_name": signalName,
			"payload":     payload,
		},
	})

	lastOutput := payload
	stepsMap := make(map[string]interface{})
	for _, s := range exec.Steps {
		if s.Output != nil {
			stepsMap[s.StageID] = map[string]interface{}{
				"output":   s.Output,
				"status":   s.Status,
				"duration": s.DurationMs,
			}
		}
	}

	env := map[string]interface{}{
		"payload":   lastOutput,
		"input":     exec.Input,
		"variables": exec.Variables,
		"steps":     stepsMap,
	}

	nextStageID := e.resolveNextStage(stage, env, lastOutput)
	go e.resumeFromStage(context.Background(), wf, exec, nextStageID, lastOutput)
	return nil
}

// ResumeChildWorkflow resumes a parent workflow when a child workflow completes.
func (e *Engine) ResumeChildWorkflow(ctx context.Context, parentExecID string, childOutput map[string]interface{}) error {
	exec, err := e.store.Executions().Get(ctx, parentExecID)
	if err != nil {
		return err
	}
	if exec.Status != model.ExecutionStatusWaitingChild {
		return nil // Not waiting
	}

	wf, err := e.store.Workflows().Get(ctx, exec.WorkflowID)
	if err != nil {
		return err
	}

	var childStage *model.Stage
	for _, s := range exec.Steps {
		if s.Status == model.StepStatusWaitingChild {
			now := time.Now()
			s.Status = model.StepStatusCompleted
			s.CompletedAt = &now
			s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
			s.Output = childOutput
			_ = e.store.Executions().AppendStep(ctx, exec.ID, s)
			stg, _ := wf.FindStage(s.StageID)
			childStage = stg
			break
		}
	}

	if childStage == nil {
		return fmt.Errorf("no waiting child stage found on execution '%s'", parentExecID)
	}

	exec.Status = model.ExecutionStatusRunning
	_ = e.store.Executions().Update(ctx, exec)

	lastOutput := childOutput
	stepsMap := make(map[string]interface{})
	for _, s := range exec.Steps {
		if s.Output != nil {
			stepsMap[s.StageID] = map[string]interface{}{
				"output":   s.Output,
				"status":   s.Status,
				"duration": s.DurationMs,
			}
		}
	}

	env := map[string]interface{}{
		"payload":   lastOutput,
		"input":     exec.Input,
		"variables": exec.Variables,
		"steps":     stepsMap,
	}

	nextStageID := e.resolveNextStage(childStage, env, lastOutput)
	go e.resumeFromStage(context.Background(), wf, exec, nextStageID, lastOutput)
	return nil
}

// Drain waits for in-flight running workflow executions to complete gracefully within the timeout window.
func (e *Engine) Drain(ctx context.Context, timeout time.Duration) error {
	slog.Info("draining active workflow executions",
		slog.Duration("timeout", timeout),
	)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(e.semaphore) == 0 {
				slog.Info("all active workflow executions drained")
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("drain timeout exceeded with %d active executions", len(e.semaphore))
			}
		}
	}
}

// DryRun executes a fast, safe, in-memory simulation of a workflow without mutating external systems.
func (e *Engine) DryRun(ctx context.Context, workflowID string, input map[string]interface{}, mockOverrides map[string]interface{}) (*model.Execution, error) {
	wf, err := e.store.Workflows().Get(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("workflow not found: %w", err)
	}

	if input == nil {
		input = make(map[string]interface{})
	}
	if mockOverrides == nil {
		mockOverrides = make(map[string]interface{})
	}

	exec := &model.Execution{
		ID:           fmt.Sprintf("sim_%s_%x", wf.ID, time.Now().UnixNano()%0xFFFFFF),
		WorkflowID:   wf.ID,
		WorkflowName: wf.Name,
		TriggerType:  model.TriggerTypeManual,
		Status:       model.ExecutionStatusRunning,
		Input:        input,
		Variables:    make(map[string]interface{}),
		Steps:        make([]*model.StepExecution, 0),
		TraceID:      uuid.New().String(),
		StartedAt:    time.Now(),
	}

	currentStageID := wf.StartAt
	if currentStageID == "" && len(wf.Stages) > 0 {
		currentStageID = wf.Stages[0].ID
	}

	currentPayload := input
	visited := make(map[string]bool)

	for currentStageID != "" {
		if visited[currentStageID] {
			// Avoid infinite loop in cyclic diagrams during dry-run
			break
		}
		visited[currentStageID] = true

		stg, ok := wf.FindStage(currentStageID)
		if !ok {
			break
		}

		stepStart := time.Now()
		step := &model.StepExecution{
			ID:        fmt.Sprintf("step_%s_%d", stg.ID, len(exec.Steps)+1),
			StageID:   stg.ID,
			StageName: stg.Name,
			StageType: stg.Type,
			Status:    model.StepStatusRunning,
			Input:     currentPayload,
			StartedAt: stepStart,
			Attempts:  1,
		}

		// Check if mock override is provided for this stage
		var stepOutput map[string]interface{}
		if override, exists := mockOverrides[stg.ID]; exists {
			if overrideMap, ok := override.(map[string]interface{}); ok {
				stepOutput = overrideMap
			} else {
				stepOutput = map[string]interface{}{"data": override}
			}
		} else {
			// Simulate stage according to type
			switch stg.Type {
			case model.StageTypeTransform:
				execCtx := &connectors.ExecutionContext{
					WorkflowID:  wf.ID,
					ExecutionID: exec.ID,
					StageID:     stg.ID,
					Payload:     currentPayload,
					Variables:   exec.Variables,
				}
				if conn, err := e.registry.Get("transform"); err == nil {
					res, err := conn.Execute(ctx, execCtx, stg.Config, currentPayload)
					if err == nil && res != nil {
						stepOutput = res
					}
				}
				if stepOutput == nil {
					stepOutput = currentPayload
				}

			case model.StageTypeDelay:
				stepOutput = map[string]interface{}{
					"simulated_delay": true,
					"configured":      stg.Config["duration"],
					"input":           currentPayload,
				}

			case model.StageTypeWaitForSignal:
				stepOutput = map[string]interface{}{
					"simulated_signal": true,
					"signal_name":      stg.Config["signal_name"],
					"mock_delivered":   true,
				}

			case model.StageTypeChildWorkflow:
				stepOutput = map[string]interface{}{
					"simulated_child_workflow": true,
					"child_workflow_id":        stg.Config["workflow_id"],
					"status":                   "COMPLETED",
				}

			case model.StageTypeExclusiveXOR:
				stepOutput = currentPayload

			default:
				// Simulated external connector (HTTP, Kafka, Database, gRPC, RabbitMQ, etc.)
				stepOutput = map[string]interface{}{
					"simulated":   true,
					"stage_type":  stg.Type,
					"status_code": 200,
					"result":      "mock_success",
					"input":       currentPayload,
				}
			}
		}

		step.Status = model.StepStatusCompleted
		step.Output = stepOutput
		step.DurationMs = time.Since(stepStart).Milliseconds()
		if step.DurationMs == 0 {
			step.DurationMs = 1
		}
		exec.Steps = append(exec.Steps, step)
		currentPayload = stepOutput

		// Build environment for next stage resolution
		stepsMap := make(map[string]interface{})
		for _, s := range exec.Steps {
			stepsMap[s.StageID] = map[string]interface{}{
				"output": s.Output,
				"status": s.Status,
			}
		}
		env := map[string]interface{}{
			"payload":   currentPayload,
			"input":     exec.Input,
			"variables": exec.Variables,
			"steps":     stepsMap,
		}

		currentStageID = e.resolveNextStage(stg, env, currentPayload)
	}

	exec.Status = model.ExecutionStatusCompleted
	exec.DurationMs = time.Since(exec.StartedAt).Milliseconds()
	if exec.DurationMs == 0 {
		exec.DurationMs = 2
	}
	return exec, nil
}

// TaskQueues returns the distributed task queue manager.
func (e *Engine) TaskQueues() *TaskQueueManager {
	return e.taskQueues
}

// CronScheduler returns the distributed cron scheduler.
func (e *Engine) CronScheduler() *CronWorkflowScheduler {
	return e.cronScheduler
}

// ResumeTaskWorker resumes a workflow when an external worker completes an activity task.
func (e *Engine) ResumeTaskWorker(ctx context.Context, executionID, stageID string, output map[string]interface{}) error {
	exec, err := e.store.Executions().Get(ctx, executionID)
	if err != nil {
		return err
	}
	wf, err := e.store.Workflows().Get(ctx, exec.WorkflowID)
	if err != nil {
		return err
	}

	stage, found := wf.FindStage(stageID)
	if !found {
		return fmt.Errorf("stage '%s' not found", stageID)
	}

	now := time.Now()
	for _, s := range exec.Steps {
		if s.StageID == stageID && (s.Status == model.StepStatusWaitingWorker || s.Status == model.StepStatusRunning) {
			s.Status = model.StepStatusCompleted
			s.Output = output
			s.CompletedAt = &now
			s.DurationMs = now.Sub(s.StartedAt).Milliseconds()
			_ = e.store.Executions().AppendStep(ctx, exec.ID, s)
			break
		}
	}

	exec.Status = model.ExecutionStatusRunning
	_ = e.store.Executions().Update(ctx, exec)

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventType("task.completed"),
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     stageID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"output": output,
		},
	})

	// Resume workflow from next stage
	if len(stage.Next) > 0 {
		nextStageID := stage.Next[0]
		go func() {
			_ = e.resumeFromStage(context.Background(), wf, exec, nextStageID, output)
		}()
	} else {
		// Completed workflow
		finishTime := time.Now()
		exec.Status = model.ExecutionStatusCompleted
		exec.CompletedAt = &finishTime
		exec.Output = output
		exec.DurationMs = finishTime.Sub(exec.StartedAt).Milliseconds()
		_ = e.store.Executions().Update(ctx, exec)
	}
	return nil
}

// FailTaskWorker handles failure reported by external worker.
func (e *Engine) FailTaskWorker(ctx context.Context, executionID, stageID string, errMsg string) error {
	exec, err := e.store.Executions().Get(ctx, executionID)
	if err != nil {
		return err
	}
	wf, err := e.store.Workflows().Get(ctx, exec.WorkflowID)
	if err != nil {
		return err
	}

	now := time.Now()
	exec.Status = model.ExecutionStatusFailed
	exec.Error = fmt.Sprintf("worker task failed at stage '%s': %s", stageID, errMsg)
	exec.CompletedAt = &now
	exec.DurationMs = now.Sub(exec.StartedAt).Milliseconds()
	_ = e.store.Executions().Update(ctx, exec)

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventExecutionFailed,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		StageID:     stageID,
		Timestamp:   now,
		Payload: map[string]interface{}{
			"error": exec.Error,
		},
	})
	return nil
}

// CancelExecution cancels an in-progress workflow execution.
func (e *Engine) CancelExecution(ctx context.Context, executionID string) error {
	exec, err := e.store.Executions().Get(ctx, executionID)
	if err != nil {
		return err
	}
	if exec.Status == model.ExecutionStatusCompleted || exec.Status == model.ExecutionStatusFailed || exec.Status == model.ExecutionStatusCancelled {
		return nil
	}

	now := time.Now()
	exec.Status = model.ExecutionStatusCancelled
	exec.CompletedAt = &now
	exec.DurationMs = now.Sub(exec.StartedAt).Milliseconds()
	_ = e.store.Executions().Update(ctx, exec)

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventType("execution.cancelled"),
		ExecutionID: exec.ID,
		WorkflowID:  exec.WorkflowID,
		Timestamp:   now,
	})
	return nil
}

// Query evaluates a synchronous read-only projection against an in-flight or completed execution.
func (e *Engine) Query(ctx context.Context, executionID string, queryExpr string) (interface{}, error) {
	exec, err := e.store.Executions().Get(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("execution not found: %w", err)
	}

	stepsMap := make(map[string]interface{})
	for _, s := range exec.Steps {
		stepsMap[s.StageID] = map[string]interface{}{
			"status":   s.Status,
			"output":   s.Output,
			"input":    s.Input,
			"error":    s.Error,
			"attempts": s.Attempts,
		}
	}

	env := map[string]interface{}{
		"input":       exec.Input,
		"variables":   exec.Variables,
		"output":      exec.Output,
		"status":      string(exec.Status),
		"steps":       stepsMap,
		"duration_ms": exec.DurationMs,
	}

	if queryExpr == "" {
		return env, nil
	}

	res, err := e.evaluator.EvalExpression(queryExpr, env)
	if err != nil {
		return nil, fmt.Errorf("query evaluation failed: %w", err)
	}
	return res, nil
}

// RecoverRunningExecutions scans for in-flight executions upon server startup and resumes them cleanly.
func (e *Engine) RecoverRunningExecutions(ctx context.Context) (int, error) {
	runningList, _, err := e.store.Executions().List(ctx, storage.ExecutionFilter{
		Status: model.ExecutionStatusRunning,
		Limit:  1000,
	})
	if err != nil {
		return 0, err
	}

	recovered := 0
	for _, exec := range runningList {
		wf, err := e.store.Workflows().Get(ctx, exec.WorkflowID)
		if err != nil {
			continue
		}

		// Find completed stages to determine resumption point
		completedStages := make(map[string]bool)
		var lastOutput map[string]interface{} = exec.Input
		for _, s := range exec.Steps {
			if s.Status == model.StepStatusCompleted {
				completedStages[s.StageID] = true
				if s.Output != nil {
					lastOutput = s.Output
				}
			}
		}

		// Find first non-completed stage in topological order
		startStageID := wf.StartAt
		if startStageID == "" && len(wf.Stages) > 0 {
			startStageID = wf.Stages[0].ID
		}

		for startStageID != "" && completedStages[startStageID] {
			stg, found := wf.FindStage(startStageID)
			if !found || len(stg.Next) == 0 {
				startStageID = ""
				break
			}
			startStageID = stg.Next[0]
		}

		if startStageID != "" {
			recovered++
			slog.Info("Recovering interrupted workflow execution from checkpoint",
				slog.String("execution_id", exec.ID),
				slog.String("workflow_id", wf.ID),
				slog.String("resuming_at_stage", startStageID),
			)
			go func(w *model.Workflow, ex *model.Execution, stgID string, payload map[string]interface{}) {
				_ = e.resumeFromStage(context.Background(), w, ex, stgID, payload)
			}(wf, exec, startStageID, lastOutput)
		}
	}
	return recovered, nil
}
