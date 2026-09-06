package engine

import (
	"fmt"
	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// ExecutionWave represents a set of stage IDs that can be executed concurrently
// because all their inbound dependencies were satisfied by previous waves.
type ExecutionWave []string

// PartitionWorkflowWaves partitions a DAG workflow into dependency waves.
// Each wave's stages can run concurrently without mutual dependencies.
func PartitionWorkflowWaves(wf *model.Workflow) ([]ExecutionWave, error) {
	if len(wf.Stages) == 0 {
		return nil, nil
	}

	inDegree := make(map[string]int)
	adj := make(map[string][]string)

	for _, s := range wf.Stages {
		inDegree[s.ID] = 0
	}

	for _, s := range wf.Stages {
		var targets []string
		targets = append(targets, s.Next...)
		for _, b := range s.Branches {
			if b.Target != "" {
				targets = append(targets, b.Target)
			}
		}
		adj[s.ID] = targets
		for _, target := range targets {
			inDegree[target]++
		}
	}

	// First wave: all stages with in-degree == 0 (roots)
	var currentWave ExecutionWave
	for _, s := range wf.Stages {
		if inDegree[s.ID] == 0 {
			currentWave = append(currentWave, s.ID)
		}
	}

	// If StartAt has in-degree > 0 or not included, ensure it leads the execution
	if len(currentWave) == 0 && wf.StartAt != "" {
		currentWave = append(currentWave, wf.StartAt)
	}

	var waves []ExecutionWave
	visitedCount := 0

	for len(currentWave) > 0 {
		waves = append(waves, currentWave)
		visitedCount += len(currentWave)

		var nextWave ExecutionWave
		for _, u := range currentWave {
			for _, v := range adj[u] {
				inDegree[v]--
				if inDegree[v] == 0 {
					nextWave = append(nextWave, v)
				}
			}
		}
		currentWave = nextWave
	}

	if visitedCount < len(wf.Stages) {
		return waves, fmt.Errorf("workflow graph contains unreachable stages or dependency cycles (visited %d of %d)", visitedCount, len(wf.Stages))
	}

	return waves, nil
}
