package pipeline_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleRouteStage() {
	ctx := context.Background()

	in := make(chan int, 6)
	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)

	// Route even numbers to branch 0, odd numbers to branch 1.
	outs := pipeline.RouteStage(ctx, in, 2, func(v int) int {
		if v%2 == 0 {
			return 0
		}
		return 1
	})

	evens := pipeline.Collect(ctx, outs[0])
	odds := pipeline.Collect(ctx, outs[1])

	sort.Ints(evens)
	sort.Ints(odds)

	fmt.Println("evens:", evens)
	fmt.Println("odds:", odds)
	// Output:
	// evens: [2 4 6]
	// odds: [1 3 5]
}

func ExampleBroadcastStage() {
	ctx := context.Background()

	in := make(chan string, 2)
	in <- "hello"
	in <- "world"
	close(in)

	// Broadcast every message to 2 consumers.
	outs := pipeline.BroadcastStage(ctx, in, 2)

	a := pipeline.Collect(ctx, outs[0])
	b := pipeline.Collect(ctx, outs[1])

	fmt.Println("consumer A:", a)
	fmt.Println("consumer B:", b)
	// Output:
	// consumer A: [hello world]
	// consumer B: [hello world]
}
