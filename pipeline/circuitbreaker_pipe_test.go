package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/streamrift/pipeline"
)

func TestCircuitBreakerStage_WithPipe(t *testing.T) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(3, time.Second)

	src := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		src <- i
	}
	close(src)

	// Map then circuit-breaker in a Pipe.
	mapped := pipeline.MapStage(ctx, src, func(v int) int { return v * 3 })
	out := pipeline.CircuitBreakerStage(ctx, mapped, cb, func(v int) (string, error) {
		if v > 10 {
			return "", errors.New("too large")
		}
		return fmt.Sprintf("%d", v), nil
	})

	results, err := pipeline.Collect(ctx, out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1*3=3 ok, 2*3=6 ok, 3*3=9 ok, 4*3=12 fail, 5*3=15 fail
	// After 3 failures circuit opens — but only 2 failures occur here,
	// so circuit stays closed and items 4,5 are simply dropped.
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d: %v", len(results), results)
	}
	if results[0] != "3" || results[1] != "6" || results[2] != "9" {
		t.Fatalf("unexpected results: %v", results)
	}
}

func TestCircuitBreakerStage_OpensAndDropsRemainder(t *testing.T) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(2, time.Minute)

	src := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		src <- i
	}
	close(src)

	out := pipeline.CircuitBreakerStage(ctx, src, cb, func(v int) (int, error) {
		if v >= 3 {
			return 0, errors.New("fail")
		}
		return v, nil
	})

	results, err := pipeline.Collect(ctx, out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// items 1,2 succeed; items 3,4 fail (opens circuit); items 5,6 dropped
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
	if cb.State() != pipeline.CircuitOpen {
		t.Fatal("expected circuit to be open")
	}
}
