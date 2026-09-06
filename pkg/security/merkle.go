package security

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Lab-OpenFlow/openflow/pkg/model"
)

// CalculateLeafHash calculates the SHA-256 hash for a step execution.
func CalculateLeafHash(prevHash string, step *model.StepExecution) string {
	if step == nil {
		return ""
	}

	h := sha256.New()
	h.Write([]byte(prevHash))
	h.Write([]byte(":"))
	h.Write([]byte(step.ID))
	h.Write([]byte(":"))
	h.Write([]byte(step.StageID))
	h.Write([]byte(":"))
	h.Write([]byte(string(step.Status)))
	h.Write([]byte(":"))
	h.Write([]byte(fmt.Sprintf("%d", step.DurationMs)))
	h.Write([]byte(":"))

	if step.Output != nil {
		outputBytes, _ := json.Marshal(step.Output)
		h.Write(outputBytes)
	}

	return hex.EncodeToString(h.Sum(nil))
}

// computeRootFromLeaves calculates the binary Merkle root from a slice of leaf hashes.
func computeRootFromLeaves(leaves []string) string {
	if len(leaves) == 0 {
		return ""
	}

	currentLevel := make([]string, len(leaves))
	copy(currentLevel, leaves)

	for len(currentLevel) > 1 {
		var nextLevel []string
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				pair := sha256.Sum256([]byte(currentLevel[i] + currentLevel[i+1]))
				nextLevel = append(nextLevel, hex.EncodeToString(pair[:]))
			} else {
				// Odd number of nodes: promote unpaired node to next level
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}
		currentLevel = nextLevel
	}

	return currentLevel[0]
}

// BuildMerkleTree constructs a binary Merkle tree from the step leaves and returns the Merkle Root hash.
func BuildMerkleTree(steps []*model.StepExecution) (string, []string) {
	if len(steps) == 0 {
		return "", nil
	}

	leaves := make([]string, len(steps))
	prev := "0000000000000000000000000000000000000000000000000000000000000000"

	for i, s := range steps {
		s.PrevHash = prev
		s.MerkleHash = CalculateLeafHash(prev, s)
		leaves[i] = s.MerkleHash
		prev = s.MerkleHash
	}

	return computeRootFromLeaves(leaves), leaves
}

// RollingMerkleTree maintains a real-time cryptographic Merkle state as steps complete incrementally.
type RollingMerkleTree struct {
	mu       sync.RWMutex
	lastHash string
	leaves   []string
	root     string
}

// NewRollingMerkleTree initializes a new rolling Merkle tree.
func NewRollingMerkleTree() *RollingMerkleTree {
	return &RollingMerkleTree{
		lastHash: "0000000000000000000000000000000000000000000000000000000000000000",
		leaves:   make([]string, 0),
	}
}

// AppendStep appends a completed step, sets its PrevHash and MerkleHash, and updates the running root.
func (t *RollingMerkleTree) AppendStep(step *model.StepExecution) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if step == nil {
		return t.root
	}

	step.PrevHash = t.lastHash
	step.MerkleHash = CalculateLeafHash(t.lastHash, step)
	t.lastHash = step.MerkleHash
	t.leaves = append(t.leaves, step.MerkleHash)

	t.root = computeRootFromLeaves(t.leaves)
	return t.root
}

// CurrentRoot returns the latest computed Merkle root.
func (t *RollingMerkleTree) CurrentRoot() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root
}

// Leaves returns a copy of all leaf hashes in execution order.
func (t *RollingMerkleTree) Leaves() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	res := make([]string, len(t.leaves))
	copy(res, t.leaves)
	return res
}

// VerifyChainIntegrity verifies the execution steps against its recorded Merkle root.
func VerifyChainIntegrity(exec *model.Execution) (bool, string, error) {
	if exec == nil {
		return false, "", fmt.Errorf("execution is nil")
	}
	if len(exec.Steps) == 0 {
		return true, "", nil
	}

	root, _ := BuildMerkleTree(exec.Steps)
	if exec.MerkleRoot != "" && exec.MerkleRoot != root {
		return false, root, fmt.Errorf("tamper detected: recorded merkle root '%s' does not match recomputed tree root '%s'",
			exec.MerkleRoot, root)
	}

	return true, root, nil
}
