package pipeline

import (
	"context"
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// CircuitBreaker tracks failures and opens the circuit when a threshold is exceeded.
type CircuitBreaker struct {
	mu           sync.Mutex
	state        CircuitState
	failures     int
	threshold    int
	resetTimeout time.Duration
	openedAt     time.Time
}

// NewCircuitBreaker creates a CircuitBreaker that opens after threshold consecutive
// failures and attempts to recover after resetTimeout.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		resetTimeout: resetTimeout,
		state:        CircuitClosed,
	}
}

// Allow reports whether a request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case CircuitOpen:
		if time.Since(cb.openedAt) >= cb.resetTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false
	default:
		return true
	}
}

// RecordSuccess resets the failure count and closes the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = CircuitClosed
}

// RecordFailure increments the failure count and opens the circuit if threshold is reached.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	if cb.failures >= cb.threshold {
		cb.state = CircuitOpen
		cb.openedAt = time.Now()
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// CircuitBreakerStage wraps a transform function with circuit-breaker logic.
// Items are dropped (not forwarded) when the circuit is open.
func CircuitBreakerStage[T any, U any](
	ctx context.Context,
	in <-chan T,
	cb *CircuitBreaker,
	fn func(T) (U, error),
) <-chan U {
	out := make(chan U)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				if !cb.Allow() {
					continue
				}
				result, err := fn(item)
				if err != nil {
					cb.RecordFailure()
					continue
				}
				cb.RecordSuccess()
				select {
				case out <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
