package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func intsChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestAggregateStage_Sum(t *testing.T) {
	ctx := context.Background()
	in := intsChan(1, 2, 3, 4, 5)
	out := pipeline.AggregateStage(ctx, in, 0, func(acc, item int) int { return acc + item })
	result := <-out
	if result != 15 {
		t.Fatalf("expected 15, got %d", result)
	}
}

func TestAggregateStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	in := intsChan()
	out := pipeline.AggregateStage(ctx, in, 42, func(acc, item int) int { return acc + item })
	result := <-out
	if result != 42 {
		t.Fatalf("expected initial value 42, got %d", result)
	}
}

func TestAggregateStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := make(chan int)
	close(in)
	out := pipeline.AggregateStage(ctx, in, 0, func(acc, item int) int { return acc + item })
	_, ok := <-out
	if ok {
		t.Fatal("expected channel to be closed after context cancellation")
	}
}

func TestCountStage_Basic(t *testing.T) {
	ctx := context.Background()
	in := intsChan(10, 20, 30)
	out := pipeline.CountStage(ctx, in)
	count := <-out
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}
}

func TestCountStage_Empty(t *testing.T) {
	ctx := context.Background()
	in := intsChan()
	out := pipeline.CountStage(ctx, in)
	count := <-out
	if count != 0 {
		t.Fatalf("expected count 0, got %d", count)
	}
}

func TestSumStage_Integers(t *testing.T) {
	ctx := context.Background()
	in := intsChan(1, 2, 3, 4)
	out := pipeline.SumStage(ctx, in)
	sum := <-out
	if sum != 10 {
		t.Fatalf("expected sum 10, got %d", sum)
	}
}

func TestSumStage_Floats(t *testing.T) {
	ctx := context.Background()
	ch := make(chan float64, 3)
	ch <- 1.5
	ch <- 2.5
	ch <- 1.0
	close(ch)
	out := pipeline.SumStage(ctx, ch)
	sum := <-out
	if sum != 5.0 {
		t.Fatalf("expected sum 5.0, got %f", sum)
	}
}
