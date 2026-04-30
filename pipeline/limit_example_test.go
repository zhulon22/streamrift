package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/pipeline"
)

func ExampleLimitStage() {
	ctx := context.Background()

	src := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		src <- i
	}
	close(src)

	out := pipeline.LimitStage[int](3)(ctx, src)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
}

func ExampleOffsetStage() {
	ctx := context.Background()

	src := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		src <- i
	}
	close(src)

	out := pipeline.OffsetStage[int](3)(ctx, src)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 4
	// 5
}
