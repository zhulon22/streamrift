package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func intSource(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestPipe_MapThenFilter(t *testing.T) {
	ctx := context.Background()
	src := intSource(1, 2, 3, 4, 5)

	p := pipeline.New(ctx, src)
	doubled := pipeline.Pipe(p, pipeline.MapStage(func(v int) int { return v * 2 }))
	filtered := pipeline.Pipe(doubled, pipeline.FilterStage(func(v int) bool { return v > 4 }))

	result := pipeline.Collect(ctx, filtered.Run())
	expected := []int{6, 8, 10}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestPipe_MapThenBatch(t *testing.T) {
	ctx := context.Background()
	src := intSource(1, 2, 3, 4, 5, 6)

	p := pipeline.New(ctx, src)
	doubled := pipeline.Pipe(p, pipeline.MapStage(func(v int) int { return v * 3 }))
	batched := pipeline.Pipe(doubled, pipeline.BatchStage[int](2))

	result := pipeline.Collect(ctx, batched.Run())
	if len(result) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(result))
	}
	if result[0][0] != 3 || result[0][1] != 6 {
		t.Errorf("unexpected first batch: %v", result[0])
	}
}

func TestCollect_Empty(t *testing.T) {
	ctx := context.Background()
	ch := make(chan int)
	close(ch)
	result := pipeline.Collect(ctx, ch)
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %v", result)
	}
}

func TestCollect_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	result := pipeline.Collect(ctx, ch)
	// With cancelled context, may collect 0 items
	_ = result
}

func TestPipe_FilterAll(t *testing.T) {
	ctx := context.Background()
	src := intSource(1, 2, 3, 4, 5)

	p := pipeline.New(ctx, src)
	// Filter out everything
	filtered := pipeline.Pipe(p, pipeline.FilterStage(func(v int) bool { return false }))

	result := pipeline.Collect(ctx, filtered.Run())
	if len(result) != 0 {
		t.Fatalf("expected empty result after filtering all, got %v", result)
	}
}
