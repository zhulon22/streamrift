package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func TestThrottleStage_LimitsRate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		src <- i
	}
	close(src)

	interval := 50 * time.Millisecond
	out := pipeline.ThrottleStage[int](interval)(ctx, src)

	start := time.Now()
	var results []int
	for v := range out {
		results = append(results, v)
	}
	elapsed := time.Since(start)

	if len(results) != 5 {
		t.Fatalf("expected 5 items, got %d", len(results))
	}
	// 5 items at 50 ms each => at least ~200 ms total (first item waits for tick)
	if elapsed < 4*interval {
		t.Errorf("expected elapsed >= %v, got %v", 4*interval, elapsed)
	}
}

func TestThrottleStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	src := make(chan int)
	go func() {
		for i := 0; ; i++ {
			select {
			case src <- i:
			case <-ctx.Done():
				close(src)
				return
			}
		}
	}()

	out := pipeline.ThrottleStage[int](20 * time.Millisecond)(ctx, src)
	time.Sleep(80 * time.Millisecond)
	cancel()

	// Drain remaining items; channel must eventually close.
	done := make(chan struct{})
	go func() {
		for range out {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("channel did not close after context cancellation")
	}
}

func TestDebounceStage_EmitsLatest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := make(chan int, 10)
	// Rapid burst — only the last should be emitted.
	for i := 1; i <= 5; i++ {
		src <- i
	}
	close(src)

	quiet := 60 * time.Millisecond
	out := pipeline.DebounceStage[int](quiet)(ctx, src)

	var results []int
	for v := range out {
		results = append(results, v)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 debounced item, got %d: %v", len(results), results)
	}
	if results[0] != 5 {
		t.Errorf("expected last value 5, got %d", results[0])
	}
}

func TestDebounceStage_EmitsAfterQuiet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := make(chan int)
	quiet := 40 * time.Millisecond
	out := pipeline.DebounceStage[int](quiet)(ctx, src)

	// Send two separated bursts.
	go func() {
		src <- 1
		src <- 2 // second item in first burst
		time.Sleep(100 * time.Millisecond)
		src <- 3
		src <- 4 // second item in second burst
		time.Sleep(100 * time.Millisecond)
		close(src)
	}()

	var results []int
	for v := range out {
		results = append(results, v)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 debounced items, got %d: %v", len(results), results)
	}
	if results[0] != 2 || results[1] != 4 {
		t.Errorf("expected [2 4], got %v", results)
	}
}
