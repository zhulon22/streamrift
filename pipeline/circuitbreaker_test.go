package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/streamrift/pipeline"
)

func makeCBChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestCircuitBreaker_ClosedAllowsAll(t *testing.T) {
	cb := pipeline.NewCircuitBreaker(3, time.Second)
	for i := 0; i < 10; i++ {
		if !cb.Allow() {
			t.Fatal("expected Allow() == true when circuit is closed")
		}
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := pipeline.NewCircuitBreaker(3, time.Second)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != pipeline.CircuitOpen {
		t.Fatal("expected circuit to be open after threshold failures")
	}
	if cb.Allow() {
		t.Fatal("expected Allow() == false when circuit is open")
	}
}

func TestCircuitBreaker_ResetsAfterTimeout(t *testing.T) {
	cb := pipeline.NewCircuitBreaker(1, 20*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected Allow() == true after reset timeout")
	}
	if cb.State() != pipeline.CircuitHalfOpen {
		t.Fatal("expected half-open state after timeout")
	}
}

func TestCircuitBreaker_ClosesOnSuccess(t *testing.T) {
	cb := pipeline.NewCircuitBreaker(1, 10*time.Millisecond)
	cb.RecordFailure()
	time.Sleep(15 * time.Millisecond)
	cb.Allow() // transitions to half-open
	cb.RecordSuccess()
	if cb.State() != pipeline.CircuitClosed {
		t.Fatal("expected circuit to close after success")
	}
}

func TestCircuitBreakerStage_ForwardsOnSuccess(t *testing.T) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(3, time.Second)
	in := makeCBChan(1, 2, 3)
	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return v * 2, nil
	})
	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 3 || results[0] != 2 || results[1] != 4 || results[2] != 6 {
		t.Fatalf("unexpected results: %v", results)
	}
}

func TestCircuitBreakerStage_DropsWhenOpen(t *testing.T) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(2, time.Second)
	cb.RecordFailure()
	cb.RecordFailure() // circuit now open
	in := makeCBChan(10, 20, 30)
	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return v, nil
	})
	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results when circuit is open, got %v", results)
	}
}

func TestCircuitBreakerStage_DropsOnError(t *testing.T) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(5, time.Second)
	in := makeCBChan(1, 2, 3)
	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return 0, errors.New("fail")
	})
	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results on error, got %v", results)
	}
	if cb.State() != pipeline.CircuitOpen {
		t.Fatal("expected circuit to open after repeated errors")
	}
}

func TestCircuitBreakerStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cb := pipeline.NewCircuitBreaker(3, time.Second)
	in := make(chan int)
	close(in)
	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return v, nil
	})
	for range out {
	}
}
