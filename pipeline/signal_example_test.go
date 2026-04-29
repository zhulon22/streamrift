package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleSignalStage() {
	ctx := context.Background()

	in := make(chan int, 3)
	in <- 10
	in <- 20
	in <- 30
	close(in)

	sig := make(chan struct{}) // no extra signals
	out := pipeline.SignalStage(ctx, in, sig)

	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 10
	// 20
	// 30
}

func ExampleGateStage() {
	ctx := context.Background()

	in := make(chan string, 4)
	in <- "a"
	in <- "b"
	in <- "c"
	in <- "d"
	close(in)

	// gate starts open, then closes, then re-opens
	gate := make(chan bool, 3)
	gate <- true
	gate <- false
	gate <- true

	out := pipeline.GateStage(ctx, in, gate)

	var results []string
	for v := range out {
		results = append(results, v)
	}
	fmt.Println("received", len(results), "items")
}
