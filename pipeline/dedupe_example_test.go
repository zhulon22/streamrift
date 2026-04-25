package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleDedupeStage() {
	ctx := context.Background()
	in := make(chan int, 6)
	for _, v := range []int{1, 1, 2, 2, 3, 1} {
		in <- v
	}
	close(in)

	out := pipeline.DedupeStage(ctx, in, func(i int) int { return i })
	items, _ := pipeline.Collect(ctx, out)
	fmt.Println(items)
	// Output: [1 2 3 1]
}

func ExampleDistinctStage() {
	ctx := context.Background()
	in := make(chan string, 5)
	for _, v := range []string{"a", "b", "a", "c", "b"} {
		in <- v
	}
	close(in)

	out := pipeline.DistinctStage(ctx, in, func(s string) string { return s })
	items, _ := pipeline.Collect(ctx, out)
	fmt.Println(items)
	// Output: [a b c]
}
