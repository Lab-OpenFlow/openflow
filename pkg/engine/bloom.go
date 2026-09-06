package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// BloomFilter is a thread-safe probabilistic data structure for set membership testing.
type BloomFilter struct {
	mu        sync.RWMutex
	bits      []uint64
	sizeBits  uint64
	numHashes uint
}

// NewBloomFilter creates a new bloom filter with given bit capacity (default 131,072 bits / 16 KB) and hash count.
func NewBloomFilter(sizeBits uint64, numHashes uint) *BloomFilter {
	if sizeBits == 0 {
		sizeBits = 131072 // 16 KB bitset
	}
	if numHashes == 0 {
		numHashes = 5
	}
	numWords := (sizeBits + 63) / 64
	return &BloomFilter{
		bits:      make([]uint64, numWords),
		sizeBits:  sizeBits,
		numHashes: numHashes,
	}
}

// hashes calculates two 64-bit seed hashes for double-hashing: hi(x) = (h1 + i*h2) % m
func (bf *BloomFilter) hashes(key string) (uint64, uint64) {
	// Hash 1: FNV-1a 64-bit
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	h1 := h.Sum64()

	// Hash 2: DJB2 variant
	var h2 uint64 = 5381
	for i := 0; i < len(key); i++ {
		h2 = ((h2 << 5) + h2) + uint64(key[i])
	}
	if h2 == 0 {
		h2 = 1
	}

	return h1, h2
}

// Add inserts a key into the Bloom filter.
func (bf *BloomFilter) Add(key string) {
	if key == "" {
		return
	}
	h1, h2 := bf.hashes(key)

	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := uint(0); i < bf.numHashes; i++ {
		bitIndex := (h1 + uint64(i)*h2) % bf.sizeBits
		wordIndex := bitIndex / 64
		bitOffset := bitIndex % 64
		bf.bits[wordIndex] |= (1 << bitOffset)
	}
}

// Contains checks if key might be in the set.
// If it returns false, the key is GUARANTEED to have NEVER been added (zero false negatives).
// If it returns true, the key was likely added (small false positive probability <= 1%).
func (bf *BloomFilter) Contains(key string) bool {
	if key == "" {
		return false
	}
	h1, h2 := bf.hashes(key)

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	for i := uint(0); i < bf.numHashes; i++ {
		bitIndex := (h1 + uint64(i)*h2) % bf.sizeBits
		wordIndex := bitIndex / 64
		bitOffset := bitIndex % 64
		if (bf.bits[wordIndex] & (1 << bitOffset)) == 0 {
			return false // Definitively not seen!
		}
	}

	return true // Possibly in set, verify with store
}

// Reset clears the filter.
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

// Snapshot serialises the bitset into a byte slice (little-endian uint64 words).
// The resulting bytes can be stored in PostgreSQL and restored after a restart.
func (bf *BloomFilter) Snapshot() []byte {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	out := make([]byte, len(bf.bits)*8)
	for i, word := range bf.bits {
		binary.LittleEndian.PutUint64(out[i*8:], word)
	}
	return out
}

// Restore loads a previously saved snapshot into the filter, replacing current state.
// Returns an error if the snapshot size does not match the filter's bitset dimensions.
func (bf *BloomFilter) Restore(data []byte) error {
	expected := len(bf.bits) * 8
	if len(data) != expected {
		return fmt.Errorf("bloom snapshot size mismatch: expected %d bytes, got %d", expected, len(data))
	}
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = binary.LittleEndian.Uint64(data[i*8:])
	}
	return nil
}

// RotatingBloomFilter maintains a sliding window of Bloom filters (current + previous generation)
// to bound false-positive probability over long runtimes, avoid bit saturation, and enforce temporal TTL.
type RotatingBloomFilter struct {
	mu             sync.RWMutex
	current        *BloomFilter
	previous       *BloomFilter
	sizeBits       uint64
	numHashes      uint
	rotateInterval time.Duration
	stopChan       chan struct{}
}

// NewRotatingBloomFilter creates a sliding-window Bloom filter that rotates every interval.
func NewRotatingBloomFilter(sizeBits uint64, numHashes uint, rotateInterval time.Duration) *RotatingBloomFilter {
	if rotateInterval <= 0 {
		rotateInterval = 1 * time.Hour
	}
	return &RotatingBloomFilter{
		current:        NewBloomFilter(sizeBits, numHashes),
		sizeBits:       sizeBits,
		numHashes:      numHashes,
		rotateInterval: rotateInterval,
		stopChan:       make(chan struct{}),
	}
}

// Add inserts a key into the active generation filter.
func (r *RotatingBloomFilter) Add(key string) {
	if key == "" {
		return
	}
	r.mu.RLock()
	cur := r.current
	r.mu.RUnlock()
	cur.Add(key)
}

// Contains returns true if key might be in either the current or previous generation.
// Returns false ONLY if key is definitively not in either generation (zero false negatives).
func (r *RotatingBloomFilter) Contains(key string) bool {
	if key == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.current != nil && r.current.Contains(key) {
		return true
	}
	if r.previous != nil && r.previous.Contains(key) {
		return true
	}
	return false
}

// Rotate advances the window: drops previous generation, promotes current to previous, and allocates a clean current filter.
func (r *RotatingBloomFilter) Rotate() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.previous = r.current
	r.current = NewBloomFilter(r.sizeBits, r.numHashes)
}

// Reset clears both current and previous generation filters.
func (r *RotatingBloomFilter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current != nil {
		r.current.Reset()
	}
	r.previous = nil
}

// StartAutoRotation launches a background ticker that rotates generations periodically.
func (r *RotatingBloomFilter) StartAutoRotation(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(r.rotateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stopChan:
				return
			case <-ticker.C:
				r.Rotate()
			}
		}
	}()
}

// Stop stops the auto-rotation goroutine.
func (r *RotatingBloomFilter) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stopChan:
		// already stopped
	default:
		close(r.stopChan)
	}
}

// Snapshot serializes both active and previous generations into a single byte payload.
func (r *RotatingBloomFilter) Snapshot() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()

	curSnap := r.current.Snapshot()
	var prevSnap []byte
	if r.previous != nil {
		prevSnap = r.previous.Snapshot()
	}

	// Format: [4 bytes cur len][cur bytes][4 bytes prev len][prev bytes]
	out := make([]byte, 8+len(curSnap)+len(prevSnap))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(curSnap)))
	copy(out[4:4+len(curSnap)], curSnap)

	offset := 4 + len(curSnap)
	binary.LittleEndian.PutUint32(out[offset:offset+4], uint32(len(prevSnap)))
	copy(out[offset+4:], prevSnap)
	return out
}

// Restore deserializes both generations from snapshot bytes.
func (r *RotatingBloomFilter) Restore(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("invalid rotating bloom snapshot: too short")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	curLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if len(data) < 4+curLen+4 {
		return fmt.Errorf("corrupted rotating bloom snapshot: invalid current slice length")
	}
	curData := data[4 : 4+curLen]
	if err := r.current.Restore(curData); err != nil {
		return fmt.Errorf("failed to restore current bloom: %w", err)
	}

	offset := 4 + curLen
	prevLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if prevLen > 0 {
		if len(data) < offset+4+prevLen {
			return fmt.Errorf("corrupted rotating bloom snapshot: invalid previous slice length")
		}
		prevData := data[offset+4 : offset+4+prevLen]
		r.previous = NewBloomFilter(r.sizeBits, r.numHashes)
		if err := r.previous.Restore(prevData); err != nil {
			return fmt.Errorf("failed to restore previous bloom: %w", err)
		}
	} else {
		r.previous = nil
	}

	return nil
}
