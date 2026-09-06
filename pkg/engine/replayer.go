package engine

import (
	"context"
	"fmt"
	"log/slog"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// ReplayState holds the reconstructed in-memory state of an interrupted workflow execution.
type ReplayState struct {
	Execution       *model.Execution
	Workflow        *model.Workflow
	CompletedStages map[string]*model.StepExecution
	StepsMap        map[string]interface{}
	LastOutput      map[string]interface{}
	NextStageID     string
}

// ReconstructExecutionState rebuilds the in-memory execution environment from the immutable event/step stream.
// Zero external network calls are performed during replay.
func ReconstructExecutionState(wf *model.Workflow, exec *model.Execution) (*ReplayState, error) {
	if exec == nil || wf == nil {
		return nil, fmt.Errorf("execution and workflow are required for replay")
	}

	state := &ReplayState{
		Execution:       exec,
		Workflow:        wf,
		CompletedStages: make(map[string]*model.StepExecution),
		StepsMap:        make(map[string]interface{}),
		LastOutput:      exec.Input,
	}

	for _, step := range exec.Steps {
		if step.Status == model.StepStatusCompleted || step.Status == model.StepStatusCompensated {
			state.CompletedStages[step.StageID] = step
			state.StepsMap[step.StageID] = map[string]interface{}{
				"output":   step.Output,
				"status":   step.Status,
				"duration": step.DurationMs,
			}
			if step.Output != nil {
				state.LastOutput = step.Output
			}
		}
	}

	// Trace workflow graph to find the first uncompleted stage
	currentID := wf.StartAt
	evaluator := NewEvaluator()

	for currentID != "" {
		if _, completed := state.CompletedStages[currentID]; !completed {
			state.NextStageID = currentID
			break
		}

		stage, found := wf.FindStage(currentID)
		if !found {
			break
		}

		env := map[string]interface{}{
			"payload":   exec.Input,
			"variables": exec.Variables,
			"steps":     state.StepsMap,
		}

		var nextStage string

		// Resolve next transition deterministically from recorded output
		if stage.Type == model.StageTypeExclusiveXOR && len(stage.Branches) > 0 {
			for _, b := range stage.Branches {
				if b.Default {
					nextStage = b.Target
				}
				if b.Condition != "" {
					matched, err := evaluator.EvalCondition(b.Condition, env)
					if err == nil && matched {
						nextStage = b.Target
						break
					}
				}
			}
		} else if len(stage.Next) > 0 {
			nextStage = stage.Next[0]
		}

		currentID = nextStage
	}

	return state, nil
}

// ResumeExecution recovers an interrupted workflow execution by replaying its history and continuing from the crash point.
func (e *Engine) ResumeExecution(ctx context.Context, executionID string) (*model.Execution, error) {
	exec, err := e.store.Executions().Get(ctx, executionID)
	if err != nil {
		return nil, fmt.Errorf("failed to load execution '%s' for resume: %w", executionID, err)
	}

	// Only recover stalled/interrupted executions
	if exec.Status != model.ExecutionStatusRunning {
		return exec, nil // already finalized
	}

	wf, err := e.store.Workflows().Get(ctx, exec.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workflow '%s': %w", exec.WorkflowID, err)
	}

	replayState, err := ReconstructExecutionState(wf, exec)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct execution state: %w", err)
	}

	slog.InfoContext(ctx, "resuming execution from replay",
		slog.String("execution_id", exec.ID),
		slog.String("workflow_id", wf.ID),
		slog.String("stage_id", replayState.NextStageID),
		slog.Int("replayed_stages", len(replayState.CompletedStages)),
	)

	e.emitEvent(model.WorkflowEvent{
		Type:        model.EventExecutionProgress,
		ExecutionID: exec.ID,
		WorkflowID:  wf.ID,
		Payload: map[string]interface{}{
			"action":            "RESUME_FROM_REPLAY",
			"replayed_stages":   len(replayState.CompletedStages),
			"resuming_at_stage": replayState.NextStageID,
		},
	})

	// Dispatch execution asynchronously via worker pool, continuing from NextStageID
	go func() {
		e.semaphore <- struct{}{}
		defer func() { <-e.semaphore }()
		e.runWorkflowFromStage(ctx, wf, exec, replayState.NextStageID, replayState.LastOutput, replayState.StepsMap)
	}()

	return exec, nil
}
