package pipeline_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// collectFromChannel drains a channel into a slice, respecting context cancellation.
func collectFromChannel[T any](ctx context.Context, ch <-chan T) []T {
	var results []T
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return results
			}
			results = append(results, v)
		case <-ctx.Done():
			return results
		}
	}
}

func TestFanOutStage_DistributesItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		src <- i
	}
	close(src)

	outs := FanOutStage[int](ctx, src, 3)
	if len(outs) != 3 {
		t.Fatalf("expected 3 output channels, got %d", len(outs))
	}

	var mu sync.Mutex
	var all []int
	var wg sync.WaitGroup

	for _, out := range outs {
		wg.Add(1)
		go func(ch <-chan int) {
			defer wg.Done()
			for v := range ch {
				mu.Lock()
				all = append(all, v)
				mu.Unlock()
			}
		}(out)
	}

	wg.Wait()

	if len(all) != 6 {
		t.Fatalf("expected 6 total items across fans, got %d", len(all))
	}

	sort.Ints(all)
	for i, v := range all {
		if v != i+1 {
			t.Errorf("expected item %d, got %d", i+1, v)
		}
	}
}

func TestFanOutStage_SingleWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		src <- i
	}
	close(src)

	outs := FanOutStage[int](ctx, src, 1)
	if len(outs) != 1 {
		t.Fatalf("expected 1 output channel, got %d", len(outs))
	}

	results := collectFromChannel(ctx, outs[0])
	sort.Ints(results)

	if len(results) != 4 {
		t.Fatalf("expected 4 items, got %d", len(results))
	}
}

func TestMergeStage_CombinesChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch1 := make(chan int, 3)
	ch2 := make(chan int, 3)
	ch3 := make(chan int, 3)

	for _, v := range []int{1, 2, 3} {
		ch1 <- v
	}
	for _, v := range []int{4, 5, 6} {
		ch2 <- v
	}
	for _, v := range []int{7, 8, 9} {
		ch3 <- v
	}
	close(ch1)
	close(ch2)
	close(ch3)

	merged := MergeStage[int](ctx, ch1, ch2, ch3)
	results := collectFromChannel(ctx, merged)

	if len(results) != 9 {
		t.Fatalf("expected 9 merged items, got %d", len(results))
	}

	sort.Ints(results)
	for i, v := range results {
		if v != i+1 {
			t.Errorf("position %d: expected %d, got %d", i, i+1, v)
		}
	}
}

func TestMergeStage_EmptyInputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch1 := make(chan string)
	ch2 := make(chan string)
	close(ch1)
	close(ch2)

	merged := MergeStage[string](ctx, ch1, ch2)
	results := collectFromChannel(ctx, merged)

	if len(results) != 0 {
		t.Errorf("expected 0 items from empty inputs, got %d", len(results))
	}
}
