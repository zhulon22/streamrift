package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

// ExampleThrottleStage demonstrates passing integers through a throttled
// pipeline stage that emits at most one item per 10 ms. Items are forwarded
// in order; the stage simply enforces a minimum interval between emissions.
func ExampleThrottleStage() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan int, 3)
	src <- 10
	src <- 20
	src <- 30
	close(src)

	out := pipeline.ThrottleStage[int](10 * time.Millisecond)(ctx, src)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 10
	// 20
	// 30
}

// ExampleRateLimitStage demonstrates a token-bucket rate limiter that allows
// a burst of 2 items before requiring token refills. With a capacity of 2 and
// a refill interval of 20 ms, the first two items pass immediately while
// subsequent items must wait for a new token to become available.
func ExampleRateLimitStage() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan string, 3)
	src <- "a"
	src <- "b"
	src <- "c"
	close(src)

	tb := pipeline.NewTokenBucket(2, 20*time.Millisecond)
	out := pipeline.RateLimitStage[string](tb)(ctx, src)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// a
	// b
	// c
}

// ExampleRateLimitStage_contextCancellation demonstrates that RateLimitStage
// respects context cancellation and stops emitting items when the context is
// done, even if the source channel still has items.
func ExampleRateLimitStage_contextCancellation() {
	ctx, cancel := context.WithCancel(context.Background())

	src := make(chan string, 3)
	src <- "x"
	src <- "y"
	src <- "z"
	close(src)

	// Cancel immediately so no items should be emitted.
	cancel()

	tb := pipeline.NewTokenBucket(1, 100*time.Millisecond)
	out := pipeline.RateLimitStage[string](tb)(ctx, src)
	count := 0
	for range out {
		count++
	}
	fmt.Println("items emitted after cancel:", count)
	// Output:
	// items emitted after cancel: 0
}
