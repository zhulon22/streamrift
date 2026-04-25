package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleTakeStage() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	out := pipeline.TakeStage(ctx, ch, 3)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
}

func ExampleSkipStage() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	out := pipeline.SkipStage(ctx, ch, 3)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 4
	// 5
}
