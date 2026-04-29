package pipeline_test

import (
	"context"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamrift/pipeline"
)

func makeScatterChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestScatterStage_ProcessesAllItems(t *testing.T) {
	ctx := context.Background()
	in := makeScatterChan(1, 2, 3, 4, 5, 6)
	out := pipeline.ScatterStage(ctx, in, 3, func(v int) (int, bool) {
		return v * 2, true
	})
	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)
	expected := []int{2, 4, 6, 8, 10, 12}
	if len(results) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(results))
	}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestScatterStage_FiltersItems(t *testing.T) {
	ctx := context.Background()
	in := makeScatterChan(1, 2, 3, 4, 5)
	out := pipeline.ScatterStage(ctx, in, 2, func(v int) (int, bool) {
		return v, v%2 == 0
	})
	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 even results, got %d: %v", len(results), results)
	}
}

func TestScatterStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int, 100)
	for i := 0; i < 100; i++ {
		in <- i
	}
	close(in)
	var processed int64
	out := pipeline.ScatterStage(ctx, in, 4, func(v int) (int, bool) {
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt64(&processed, 1)
		return v, true
	})
	cancel()
	for range out {
	}
	if atomic.LoadInt64(&processed) == 100 {
		t.Error("expected cancellation to stop processing before all 100 items")
	}
}

func TestGatherStage_MergesAllChannels(t *testing.T) {
	ctx := context.Background()
	ch1 := makeScatterChan(1, 2, 3)
	ch2 := makeScatterChan(4, 5, 6)
	ch3 := makeScatterChan(7, 8, 9)
	out := pipeline.GatherStage(ctx, ch1, ch2, ch3)
	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 9 {
		t.Fatalf("expected 9 results, got %d", len(results))
	}
}

func TestGatherStage_EmptyInputs(t *testing.T) {
	ctx := context.Background()
	out := pipeline.GatherStage[int](ctx)
	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 0 {
		t.Errorf("expected no results, got %d", len(results))
	}
}
