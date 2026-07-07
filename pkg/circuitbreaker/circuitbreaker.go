package circuitbreaker

import (
	"sync"
	"time"
)

// State represents the circuit breaker state
type State int

const (
	StateClosed State = iota  // Normal operation
	StateOpen                 // Failing, reject requests
	StateHalfOpen            // Testing if recovered
)

// CircuitBreaker implements a simple circuit breaker pattern
type CircuitBreaker struct {
	mu sync.Mutex

	maxFailures  uint
	resetTimeout time.Duration
	
	state        State
	failures     uint
	lastFailTime time.Time
}

// New creates a new circuit breaker
func New(maxFailures uint, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        StateClosed,
	}
}

// Call executes the function if circuit is not open
func (cb *CircuitBreaker) Call(fn func() error) error {
	if !cb.Allow() {
		return ErrCircuitOpen
	}

	err := fn()
	cb.RecordResult(err)
	return err
}

// Allow checks if a request can proceed
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
		if time.Since(cb.lastFailTime) > cb.resetTimeout {
			cb.state = StateHalfOpen
			return true
		}
		return false
	case StateHalfOpen:
		return true
	}
	return false
}

// RecordResult records the result of an operation
func (cb *CircuitBreaker) RecordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		// Success
		if cb.state == StateHalfOpen {
			// Recovered, close circuit
			cb.state = StateClosed
			cb.failures = 0
		}
		return
	}

	// Failure
	cb.failures++
	cb.lastFailTime = time.Now()

	if cb.failures >= cb.maxFailures {
		cb.state = StateOpen
	}
}

// Reset manually resets the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failures = 0
}

// GetState returns the current state
func (cb *CircuitBreaker) GetState() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

var ErrCircuitOpen = &CircuitOpenError{}

type CircuitOpenError struct{}

func (e *CircuitOpenError) Error() string {
	return "circuit breaker is open"
}
