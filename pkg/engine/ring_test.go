package engine_test

import (
	"testing"
	"github.com/Lab-OpenFlow/openflow/pkg/engine"
)

func TestConsistentHashRing(t *testing.T) {
	ring := engine.NewHashRing(100)

	// Add 3 nodes
	ring.AddNode("node-alpha")
	ring.AddNode("node-bravo")
	ring.AddNode("node-charlie")

	if ring.NodeCount() != 3 {
		t.Fatalf("expected 3 physical nodes, got %d", ring.NodeCount())
	}

	// Deterministic routing
	node1, err := ring.GetNode("workflow_user_12345")
	if err != nil {
		t.Fatalf("unexpected hash routing error: %v", err)
	}
	node2, _ := ring.GetNode("workflow_user_12345")
	if node1 != node2 {
		t.Fatalf("expected same node for identical key, got %s vs %s", node1, node2)
	}

	// Remove a node
	ring.RemoveNode("node-alpha")
	if ring.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", ring.NodeCount())
	}

	nodeAfterRemoval, err := ring.GetNode("workflow_user_12345")
	if err != nil || nodeAfterRemoval == "" {
		t.Fatalf("expected valid node routing after node removal, got: %s (err: %v)", nodeAfterRemoval, err)
	}
}
