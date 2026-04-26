package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleTimeoutStage() {
	ctx := context.Background()

	in := make(chan int, 3)
	in <- 10
	in <- 20
	in <- 30
	close(in)

	// Allow 100 ms per item to be consumed downstream.
	out := pipeline.TimeoutStage(ctx, in, 100*time.Millisecond)

	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 10
	// 20
	// 30
}

func ExampleDeadlineStage() {
	ctx := context.Background()

	in := make(chan string, 2)
	in <- "hello"
	in <- "world"
	close(in)

	// Give the pipeline 500 ms total to forward items.
	out := pipeline.DeadlineStage(ctx, in, time.Now().Add(500*time.Millisecond))

	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// hello
	// world
}
