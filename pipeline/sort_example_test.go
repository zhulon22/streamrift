package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleSortStage() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for _, v := range []int{4, 2, 5, 1, 3} {
		ch <- v
	}
	close(ch)

	sorted := pipeline.SortStage[int](func(a, b int) bool {
		return a < b
	})(ctx, ch)

	for v := range sorted {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

func ExampleTopNStage() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for _, v := range []int{10, 30, 20, 50, 40} {
		ch <- v
	}
	close(ch)

	// Emit top 3 largest values
	top := pipeline.TopNStage[int](3, func(a, b int) bool {
		return a > b
	})(ctx, ch)

	for v := range top {
		fmt.Println(v)
	}
	// Output:
	// 50
	// 40
	// 30
}

func ExampleTopNStage_bottom() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for _, v := range []int{10, 30, 20, 50, 40} {
		ch <- v
	}
	close(ch)

	// Emit bottom 3 smallest values by reversing the comparator
	bottom := pipeline.TopNStage[int](3, func(a, b int) bool {
		return a < b
	})(ctx, ch)

	for v := range bottom {
		fmt.Println(v)
	}
	// Output:
	// 10
	// 20
	// 30
}
