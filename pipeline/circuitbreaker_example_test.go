package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/streamrift/pipeline"
)

func ExampleCircuitBreakerStage() {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(2, 500*time.Millisecond)

	// Simulate two failures to open the circuit.
	cb.RecordFailure()
	cb.RecordFailure()

	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return v * 10, nil
	})

	var results []int
	for v := range out {
		results = append(results, v)
	}

	// All items are dropped because the circuit is open.
	fmt.Println("items forwarded:", len(results))
	fmt.Println("circuit state open:", cb.State() == pipeline.CircuitOpen)
	// Output:
	// items forwarded: 0
	// circuit state open: true
}
