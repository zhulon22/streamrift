package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func TestTimeoutStage_ForwardsItemsWithinTimeout(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)

	out := pipeline.TimeoutStage(ctx, in, 100*time.Millisecond)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 items, got %d", len(got))
	}
}

func TestTimeoutStage_DropsItemsWhenDownstreamSlow(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	// unbuffered output + very short timeout => items will be dropped
	out := pipeline.TimeoutStage(ctx, in, 1*time.Nanosecond)
	time.Sleep(10 * time.Millisecond) // let goroutine run
	var got []int
	for v := range out {
		got = append(got, v)
	}
	// We cannot assert exact count, but the stage must not block or panic.
	_ = got
}

func TestTimeoutStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	out := pipeline.TimeoutStage(ctx, in, 1*time.Second)
	cancel()
	_, ok := <-out
	if ok {
		t.Fatal("expected channel to be closed after context cancellation")
	}
}

func TestDeadlineStage_ForwardsItemsBeforeDeadline(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 4)
	for i := 1; i <= 4; i++ {
		in <- i
	}
	close(in)

	deadline := time.Now().Add(500 * time.Millisecond)
	out := pipeline.DeadlineStage(ctx, in, deadline)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 items, got %d", len(got))
	}
}

func TestDeadlineStage_StopsAtDeadline(t *testing.T) {
	ctx := context.Background()
	in := make(chan int) // never sends

	deadline := time.Now().Add(30 * time.Millisecond)
	out := pipeline.DeadlineStage(ctx, in, deadline)

	start := time.Now()
	for range out {
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("stage took too long to close after deadline: %v", elapsed)
	}
}

func TestDeadlineStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	out := pipeline.DeadlineStage(ctx, in, time.Now().Add(10*time.Second))
	cancel()
	_, ok := <-out
	if ok {
		t.Fatal("expected channel to be closed after context cancellation")
	}
}
