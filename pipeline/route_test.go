package pipeline_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func collectAll[T any](ctx context.Context, chs []<-chan T) [][]T {
	results := make([][]T, len(chs))
	var wg sync.WaitGroup
	for i, ch := range chs {
		wg.Add(1)
		go func(idx int, c <-chan T) {
			defer wg.Done()
			for item := range c {
				results[idx] = append(results[idx], item)
			}
		}(i, ch)
	}
	wg.Wait()
	return results
}

func TestRouteStage_RoutesItemsByPredicate(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)

	outs := pipeline.RouteStage(ctx, in, 2, func(v int) int {
		if v%2 == 0 {
			return 0
		}
		return 1
	})

	results := collectAll(ctx, outs)

	sort.Ints(results[0])
	sort.Ints(results[1])

	if len(results[0]) != 3 {
		t.Fatalf("expected 3 evens, got %d", len(results[0]))
	}
	if len(results[1]) != 3 {
		t.Fatalf("expected 3 odds, got %d", len(results[1]))
	}
}

func TestRouteStage_DropsOutOfRangeIndex(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	// predicate always returns out-of-range
	outs := pipeline.RouteStage(ctx, in, 2, func(v int) int { return 99 })
	results := collectAll(ctx, outs)

	if len(results[0]) != 0 || len(results[1]) != 0 {
		t.Fatal("expected all items to be dropped")
	}
}

func TestRouteStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)

	outs := pipeline.RouteStage(ctx, in, 2, func(v int) int { return 0 })
	cancel()

	for _, ch := range outs {
		select {
		case <-ch:
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func TestBroadcastStage_SendsToAll(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 3)
	in <- 10
	in <- 20
	in <- 30
	close(in)

	outs := pipeline.BroadcastStage(ctx, in, 3)
	results := collectAll(ctx, outs)

	for i, branch := range results {
		if len(branch) != 3 {
			t.Fatalf("branch %d: expected 3 items, got %d", i, len(branch))
		}
	}
}
