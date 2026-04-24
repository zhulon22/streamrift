package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

// ExampleThrottleStage demonstrates passing integers through a throttled
// pipeline stage that emits at most one item per 10 ms.
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
// a burst of 2 items before requiring token refills.
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
