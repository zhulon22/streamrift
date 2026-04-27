package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleInspectStage() {
	ctx := context.Background()

	in := make(chan int, 4)
	for _, v := range []int{10, 20, 30, 40} {
		in <- v
	}
	close(in)

	out := pipeline.InspectStage(ctx, in, func(v int) {
		fmt.Printf("saw: %d\n", v)
	})

	for range out {
	}
	// Output:
	// saw: 10
	// saw: 20
	// saw: 30
	// saw: 40
}

func ExampleTapStage() {
	ctx := context.Background()

	in := make(chan string, 3)
	in <- "hello"
	in <- "world"
	in <- "!"
	close(in)

	out := pipeline.TapStage(ctx, in, "logger", func(name, val string) {
		fmt.Printf("[%s] %s\n", name, val)
	})

	result, _ := pipeline.Collect(ctx, out)
	fmt.Println(result)
	// Output:
	// [logger] hello
	// [logger] world
	// [logger] !
	// [hello world !]
}
