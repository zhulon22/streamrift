package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/your-org/streamrift/pipeline"
)

func ExampleMeteredStage() {
	ctx := context.Background()
	metrics := &pipeline.StageMetrics{}

	// Wrap a filter stage with metrics instrumentation.
	filter := pipeline.FilterStage(func(n int) bool { return n%2 == 0 })
	metered := pipeline.MeteredStage(metrics, filter)

	ch := make(chan int, 6)
	for _, v := range []int{1, 2, 3, 4, 5, 6} {
		ch <- v
	}
	close(ch)

	out := metered(ctx, ch)
	var results []int
	for v := range out {
		results = append(results, v)
	}

	snap := metrics.Snapshot()
	fmt.Println("results:", results)
	fmt.Println("items in:", snap.ItemsIn)
	fmt.Println("items out:", snap.ItemsOut)
	// Output:
	// results: [2 4 6]
	// items in: 6
	// items out: 3
}

func ExampleLatencyStage() {
	ctx := context.Background()
	var total time.Duration
	count := 0

	passThrough := func(ctx context.Context, in <-chan string) <-chan string {
		out := make(chan string)
		go func() {
			defer close(out)
			for v := range in {
				out <- v
			}
		}()
		return out
	}

	latencyStage := pipeline.LatencyStage(func(d time.Duration) {
		total += d
		count++
	}, passThrough)

	ch := make(chan string, 3)
	ch <- "a"
	ch <- "b"
	ch <- "c"
	close(ch)

	out := latencyStage(ctx, ch)
	var items []string
	for v := range out {
		items = append(items, v)
	}

	fmt.Println("items:", items)
	fmt.Println("latency samples:", count)
	// Output:
	// items: [a b c]
	// latency samples: 3
}
