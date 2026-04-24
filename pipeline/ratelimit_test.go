package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func TestRateLimitStage_AllItemsPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		src <- i
	}
	close(src)

	tb := pipeline.NewTokenBucket(3, 30*time.Millisecond)
	out := pipeline.RateLimitStage[int](tb)(ctx, src)

	var results []int
	for v := range out {
		results = append(results, v)
	}

	if len(results) != 6 {
		t.Fatalf("expected 6 items, got %d", len(results))
	}
	for i, v := range results {
		if v != i+1 {
			t.Errorf("index %d: expected %d, got %d", i, i+1, v)
		}
	}
}

func TestRateLimitStage_RespectsBurst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		src <- i
	}
	close(src)

	// capacity=2 means first 2 items are free; next 2 must wait for refills.
	refill := 50 * time.Millisecond
	tb := pipeline.NewTokenBucket(2, refill)
	out := pipeline.RateLimitStage[int](tb)(ctx, src)

	start := time.Now()
	var results []int
	for v := range out {
		results = append(results, v)
	}
	elapsed := time.Since(start)

	if len(results) != 4 {
		t.Fatalf("expected 4 items, got %d", len(results))
	}
	// After the burst of 2, at least 2 more refill intervals are needed.
	if elapsed < 2*refill {
		t.Errorf("rate limit not enforced: elapsed %v, expected >= %v", elapsed, 2*refill)
	}
}

func TestRateLimitStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	src := make(chan int)
	// Never send anything — the stage should still close on cancel.
	go func() {
		<-ctx.Done()
		close(src)
	}()

	tb := pipeline.NewTokenBucket(1, 10*time.Millisecond)
	out := pipeline.RateLimitStage[int](tb)(ctx, src)

	time.Sleep(30 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		for range out {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("output channel did not close after context cancellation")
	}
}
