package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// CompensationExecutor is the function signature for executing a single compensation step.
type CompensationExecutor func(ctx context.Context, comp *model.CompensationConfig, originalStep *model.StepExecution, env map[string]interface{}) (map[string]interface{}, error)

// SagaManager coordinates backward rollback (compensations) when a workflow execution fails.
type SagaManager struct {
	evaluator *Evaluator
}

// NewSagaManager creates a new saga rollback manager.
func NewSagaManager(evaluator *Evaluator) *SagaManager {
	return &SagaManager{evaluator: evaluator}
}

// defaultCompTimeout is the per-compensation deadline when none is specified in the workflow.
const defaultCompTimeout = 30 * time.Second

// Rollback executes compensation steps for all completed stages that defined a compensation.
//
// Strategy selection (via wf.SagaStrategy):
//   - "parallel" (default): all compensations run concurrently via goroutines + WaitGroup.
//     Fastest; appropriate when compensations are independent (most cases).
//   - "serial": compensations run one-by-one in reverse order (LIFO).
//     Use when compensations must happen in a specific sequence.
//
// Each compensation step honours its CompensationConfig.Timeout (default 30s).
func (s *SagaManager) Rollback(
	ctx context.Context,
	wf *model.Workflow,
	exec *model.Execution,
	env map[string]interface{},
	executor CompensationExecutor,
	onEvent func(event model.WorkflowEvent),
) error {
	exec.Status = model.ExecutionStatusCompensating
	if onEvent != nil {
		onEvent(model.WorkflowEvent{
			Type:        model.EventSagaStarted,
			ExecutionID: exec.ID,
			WorkflowID:  exec.WorkflowID,
			Timestamp:   time.Now(),
			Payload: map[string]interface{}{
				"reason":   exec.Error,
				"strategy": string(wf.SagaStrategy),
			},
		})
	}

	completedSteps := exec.CompletedStages()

	// Build the list of (compensation, step) pairs to execute
	type compTask struct {
		comp *model.CompensationConfig
		step *model.StepExecution
	}
	var tasks []compTask
	// Collect in reverse order for serial LIFO semantics; parallel order doesn't matter
	for i := len(completedSteps) - 1; i >= 0; i-- {
		step := completedSteps[i]
		stage, found := wf.FindStage(step.StageID)
		if !found || stage.Compensation == nil {
			continue
		}
		tasks = append(tasks, compTask{comp: stage.Compensation, step: step})
	}

	if len(tasks) == 0 {
		exec.Status = model.ExecutionStatusCompensated
		return nil
	}

	// runOne executes a single compensation task, adding the result step to exec.Steps.
	runOne := func(task compTask) error {
		comp := task.comp
		step := task.step

		compTimeout := defaultCompTimeout
		if comp.Timeout != "" {
			if d, err := time.ParseDuration(comp.Timeout); err == nil && d > 0 {
				compTimeout = d
			}
		}
		// Set per-compensation deadline
		compCtx, compCancel := context.WithTimeout(ctx, compTimeout)
		defer compCancel()

		compStepID := fmt.Sprintf("comp_%s_%s", step.StageID, comp.ID)
		now := time.Now()
		compStep := &model.StepExecution{
			ID:             compStepID,
			StageID:        comp.ID,
			StageName:      fmt.Sprintf("Compensate: %s", comp.Name),
			StageType:      comp.Type,
			Status:         model.StepStatusCompensating,
			Attempts:       1,
			StartedAt:      now,
			IsCompensation: true,
			CompensatesFor: step.StageID,
		}

		if onEvent != nil {
			onEvent(model.WorkflowEvent{
				Type:        model.EventStepStarted,
				ExecutionID: exec.ID,
				WorkflowID:  exec.WorkflowID,
				StageID:     comp.ID,
				Timestamp:   now,
				Payload: map[string]interface{}{
					"is_compensation": true,
					"compensates_for": step.StageID,
					"timeout":         compTimeout.String(),
				},
			})
		}

		// Interpolate compensation configuration
		interpolatedConfig, err := s.evaluator.InterpolateMap(comp.Config, env)
		if err != nil {
			errStr := fmt.Sprintf("failed to interpolate compensation config: %v", err)
			compStep.Status = model.StepStatusFailed
			compStep.Error = errStr
			finishTime := time.Now()
			compStep.CompletedAt = &finishTime
			compStep.DurationMs = finishTime.Sub(now).Milliseconds()
			exec.Steps = append(exec.Steps, compStep)
			return fmt.Errorf("%s", errStr)
		}

		compCopy := *comp
		compCopy.Config = interpolatedConfig

		output, err := executor(compCtx, &compCopy, step, env)
		finishTime := time.Now()
		compStep.CompletedAt = &finishTime
		compStep.DurationMs = finishTime.Sub(now).Milliseconds()

		if err != nil {
			compStep.Status = model.StepStatusFailed
			compStep.Error = err.Error()
			exec.Steps = append(exec.Steps, compStep)

			if onEvent != nil {
				onEvent(model.WorkflowEvent{
					Type:        model.EventSagaFailed,
					ExecutionID: exec.ID,
					WorkflowID:  exec.WorkflowID,
					StageID:     comp.ID,
					Timestamp:   finishTime,
					Payload:     map[string]interface{}{"error": err.Error()},
				})
			}
			return fmt.Errorf("compensation '%s' failed: %w", comp.ID, err)
		}

		compStep.Status = model.StepStatusCompensated
		compStep.Output = output
		exec.Steps = append(exec.Steps, compStep)

		if onEvent != nil {
			onEvent(model.WorkflowEvent{
				Type:        model.EventSagaStepCompleted,
				ExecutionID: exec.ID,
				WorkflowID:  exec.WorkflowID,
				StageID:     comp.ID,
				Timestamp:   finishTime,
				Payload:     map[string]interface{}{"output": output},
			})
		}
		return nil
	}

	var rollbackErrors []string

	if wf.SagaStrategy == model.SagaStrategySerial {
		// Serial LIFO: execute compensations one at a time in reverse order
		for _, task := range tasks {
			if err := runOne(task); err != nil {
				rollbackErrors = append(rollbackErrors, err.Error())
			}
		}
	} else {
		// Parallel (default): run all compensations concurrently
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			errList []string
		)
		for _, task := range tasks {
			wg.Add(1)
			go func(t compTask) {
				defer wg.Done()
				if err := runOne(t); err != nil {
					mu.Lock()
					errList = append(errList, err.Error())
					mu.Unlock()
				}
			}(task)
		}
		wg.Wait()
		rollbackErrors = errList
	}

	if len(rollbackErrors) > 0 {
		exec.Status = model.ExecutionStatusFailed
		return fmt.Errorf("saga rollback encountered errors: %v", rollbackErrors)
	}

	exec.Status = model.ExecutionStatusCompensated
	if onEvent != nil {
		onEvent(model.WorkflowEvent{
			Type:        model.EventSagaCompleted,
			ExecutionID: exec.ID,
			WorkflowID:  exec.WorkflowID,
			Timestamp:   time.Now(),
		})
	}

	return nil
}
