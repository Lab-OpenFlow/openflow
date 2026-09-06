package engine

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// HashRing implements a consistent hash ring with virtual nodes for workflow routing.
type HashRing struct {
	mu              sync.RWMutex
	virtualReplicas int
	ring            []uint32
	nodeMap         map[uint32]string
	nodes           map[string]bool
}

// NewHashRing creates a consistent hash ring with the specified virtual replicas per node.
func NewHashRing(virtualReplicas int) *HashRing {
	if virtualReplicas <= 0 {
		virtualReplicas = 150
	}
	return &HashRing{
		virtualReplicas: virtualReplicas,
		nodeMap:         make(map[uint32]string),
		nodes:           make(map[string]bool),
	}
}

func (h *HashRing) hashKey(key string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum32()
}

// AddNode adds a physical node and its virtual replicas to the consistent hash ring.
func (h *HashRing) AddNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.nodes[node] {
		return
	}
	h.nodes[node] = true

	for i := 0; i < h.virtualReplicas; i++ {
		vNodeKey := node + "#" + strconv.Itoa(i)
		hash := h.hashKey(vNodeKey)
		h.ring = append(h.ring, hash)
		h.nodeMap[hash] = node
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

// RemoveNode removes a physical node and its virtual replicas from the ring.
func (h *HashRing) RemoveNode(node string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.nodes[node] {
		return
	}
	delete(h.nodes, node)

	newRing := make([]uint32, 0, len(h.ring)-h.virtualReplicas)
	for _, hash := range h.ring {
		if h.nodeMap[hash] == node {
			delete(h.nodeMap, hash)
		} else {
			newRing = append(newRing, hash)
		}
	}
	h.ring = newRing
}

// GetNode returns the node assigned to the given key.
func (h *HashRing) GetNode(key string) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return "", fmt.Errorf("hash ring is empty: no nodes available")
	}

	hash := h.hashKey(key)

	// Binary search for the first node with hash >= key's hash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	// Wrap around to ring root (modulo circle) if beyond highest hash
	if idx == len(h.ring) {
		idx = 0
	}

	return h.nodeMap[h.ring[idx]], nil
}

// NodeCount returns the count of active physical nodes on the ring.
func (h *HashRing) NodeCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}
