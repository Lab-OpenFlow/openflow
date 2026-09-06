package engine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Lab-OpenFlow/openflow/pkg/observability"
)

var (
	// ErrCircuitOpen is returned when an execution is blocked because the circuit is open.
	ErrCircuitOpen = errors.New("circuit breaker is open: connector is temporarily failing")
)

// CircuitState represents the current state of a circuit breaker.
type CircuitState string

const (
	StateClosed   CircuitState = "CLOSED"
	StateHalfOpen CircuitState = "HALF_OPEN"
	StateOpen     CircuitState = "OPEN"
)

// CircuitBreakerConfig defines thresholds and cooldowns for a circuit breaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the consecutive failure count that triggers opening the circuit.
	FailureThreshold int
	// CooldownDuration is how long the circuit stays open before transitioning to half-open.
	CooldownDuration time.Duration
	// HalfOpenMaxRequests is the maximum number of probe requests allowed in half-open state.
	HalfOpenMaxRequests int
}

// DefaultCircuitBreakerConfig returns standard resilience defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold:    5,
		CooldownDuration:    15 * time.Second,
		HalfOpenMaxRequests: 2,
	}
}

// CircuitBreaker guards remote connectors from cascading failures under outage conditions.
type CircuitBreaker struct {
	mu               sync.RWMutex
	name             string
	config           CircuitBreakerConfig
	state            CircuitState
	consecutiveFails int
	openedAt         time.Time
	halfOpenRequests int
}

// NewCircuitBreaker creates a new circuit breaker instance.
func NewCircuitBreaker(name string, cfg ...CircuitBreakerConfig) *CircuitBreaker {
	config := DefaultCircuitBreakerConfig()
	if len(cfg) > 0 {
		config = cfg[0]
	}
	return &CircuitBreaker{
		name:   name,
		config: config,
		state:  StateClosed,
	}
}

// Allow checks if a request is permitted to proceed.
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return nil

	case StateOpen:
		// Check if cooldown has elapsed to probe in half-open
		if now.Sub(cb.openedAt) >= cb.config.CooldownDuration {
			cb.state = StateHalfOpen
			cb.halfOpenRequests = 1
			return nil
		}
		return fmt.Errorf("%w (name: %s, remaining cooldown: %v)",
			ErrCircuitOpen, cb.name, cb.config.CooldownDuration-now.Sub(cb.openedAt).Round(time.Millisecond))

	case StateHalfOpen:
		if cb.halfOpenRequests < cb.config.HalfOpenMaxRequests {
			cb.halfOpenRequests++
			return nil
		}
		return fmt.Errorf("%w (name: %s, probing in progress)", ErrCircuitOpen, cb.name)
	}

	return nil
}

// RecordSuccess records a successful invocation and closes the circuit if in half-open.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFails = 0
	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.halfOpenRequests = 0
	}
}

// RecordFailure records a failed invocation and opens the circuit if threshold is reached.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFails++
	if cb.state == StateHalfOpen || cb.consecutiveFails >= cb.config.FailureThreshold {
		cb.state = StateOpen
		cb.openedAt = time.Now()
		cb.halfOpenRequests = 0
		// Emit metric: circuit tripped to OPEN
		observability.GlobalMetrics.CircuitBreakerTrips.WithLabelValues(cb.name).Inc()
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Reset resets the breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.consecutiveFails = 0
	cb.halfOpenRequests = 0
}

// CircuitBreakerRegistry manages circuit breakers for various connector types or target stages.
type CircuitBreakerRegistry struct {
	breakers sync.Map // map[string]*CircuitBreaker
	config   CircuitBreakerConfig
}

// NewCircuitBreakerRegistry creates a registry of circuit breakers.
func NewCircuitBreakerRegistry(cfg ...CircuitBreakerConfig) *CircuitBreakerRegistry {
	config := DefaultCircuitBreakerConfig()
	if len(cfg) > 0 {
		config = cfg[0]
	}
	return &CircuitBreakerRegistry{config: config}
}

// GetOrCreate returns the circuit breaker for a key (e.g. stage type or stage ID).
func (r *CircuitBreakerRegistry) GetOrCreate(key string) *CircuitBreaker {
	if val, ok := r.breakers.Load(key); ok {
		return val.(*CircuitBreaker)
	}
	cb := NewCircuitBreaker(key, r.config)
	actual, _ := r.breakers.LoadOrStore(key, cb)
	return actual.(*CircuitBreaker)
}
