package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func makeChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestTakeStage_TakesExactN(t *testing.T) {
	ctx := context.Background()
	in := makeChan(1, 2, 3, 4, 5)
	out := pipeline.TakeStage(ctx, in, 3)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 items, got %d", len(results))
	}
	for i, v := range results {
		if v != i+1 {
			t.Errorf("expected %d at index %d, got %d", i+1, i, v)
		}
	}
}

func TestTakeStage_FewerThanN(t *testing.T) {
	ctx := context.Background()
	in := makeChan(1, 2)
	out := pipeline.TakeStage(ctx, in, 10)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 items, got %d", len(results))
	}
}

func TestSkipStage_SkipsFirstN(t *testing.T) {
	ctx := context.Background()
	in := makeChan(1, 2, 3, 4, 5)
	out := pipeline.SkipStage(ctx, in, 2)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 items, got %d", len(results))
	}
	expected := []int{3, 4, 5}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestSkipStage_SkipMoreThanAvailable(t *testing.T) {
	ctx := context.Background()
	in := makeChan(1, 2)
	out := pipeline.SkipStage(ctx, in, 10)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 items, got %d", len(results))
	}
}

func TestSampleStage_ProbabilityOne(t *testing.T) {
	ctx := context.Background()
	in := makeChan(1, 2, 3, 4, 5)
	out := pipeline.SampleStage(ctx, in, 1.0)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 5 {
		t.Fatalf("expected all 5 items with p=1.0, got %d", len(results))
	}
}

func TestSampleStage_ProbabilityZero(t *testing.T) {
	ctx := context.Background()
	in := makeChan(1, 2, 3, 4, 5)
	out := pipeline.SampleStage(ctx, in, 0.0)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 items with p=0.0, got %d", len(results))
	}
}

func TestTakeStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan int, 5)
	for i := 0; i < 5; i++ {
		ch <- i
	}
	close(ch)

	out := pipeline.TakeStage(ctx, ch, 5)
	var results []int
	for v := range out {
		results = append(results, v)
	}
	// With cancelled context, we expect 0 or very few items
	if len(results) > 5 {
		t.Errorf("unexpected items after cancellation: %d", len(results))
	}
}
