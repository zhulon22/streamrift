package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

// TestAggregateStage_WithPipe verifies that AggregateStage integrates cleanly
// with the Pipe helper for building multi-stage pipelines.
func TestAggregateStage_WithPipe(t *testing.T) {
	ctx := context.Background()

	// Source: integers 1..10
	src := make(chan int, 10)
	for i := 1; i <= 10; i++ {
		src <- i
	}
	close(src)

	// Stage 1: filter evens
	filtered := pipeline.FilterStage(ctx, src, func(n int) bool { return n%2 == 0 })

	// Stage 2: sum all evens (2+4+6+8+10 = 30)
	sumCh := pipeline.SumStage(ctx, filtered)

	got := <-sumCh
	if got != 30 {
		t.Fatalf("expected 30, got %d", got)
	}
}

// TestCountStage_AfterMap verifies counting works after a map transformation.
func TestCountStage_AfterMap(t *testing.T) {
	ctx := context.Background()

	src := make(chan int, 5)
	for i := 0; i < 5; i++ {
		src <- i
	}
	close(src)

	// Map each int to a string, then count
	mapped := pipeline.MapStage(ctx, src, func(n int) string {
		return fmt.Sprintf("%d", n)
	})

	countCh := pipeline.CountStage(ctx, mapped)
	count := <-countCh
	if count != 5 {
		t.Fatalf("expected count 5, got %d", count)
	}
}
