package pipeline_test

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/streamrift/pipeline"
)

func ExampleScatterStage() {
	ctx := context.Background()

	in := make(chan int, 6)
	for _, v := range []int{1, 2, 3, 4, 5, 6} {
		in <- v
	}
	close(in)

	// Square each number using 3 concurrent workers.
	out := pipeline.ScatterStage(ctx, in, 3, func(v int) (int, bool) {
		return v * v, true
	})

	var results []int
	for v := range out {
		results = append(results, v)
	}
	sort.Ints(results)

	strs := make([]string, len(results))
	for i, v := range results {
		strs[i] = fmt.Sprintf("%d", v)
	}
	fmt.Println(strings.Join(strs, ","))
	// Output: 1,4,9,16,25,36
}

func ExampleGatherStage() {
	ctx := context.Background()

	ch1 := make(chan string, 2)
	ch1 <- "a"
	ch1 <- "b"
	close(ch1)

	ch2 := make(chan string, 2)
	ch2 <- "c"
	ch2 <- "d"
	close(ch2)

	out := pipeline.GatherStage(ctx, ch1, ch2)

	var results []string
	for v := range out {
		results = append(results, v)
	}
	sort.Strings(results)
	fmt.Println(strings.Join(results, ","))
	// Output: a,b,c,d
}
