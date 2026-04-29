package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleWatermarkStage() {
	ctx := context.Background()

	in := make(chan int, 3)
	in <- 10
	in <- 20
	in <- 30
	close(in)

	// Stamp each item with the current time; tick every 10 seconds (won't fire
	// during this short example).
	wm := pipeline.WatermarkStage(ctx, in, 10*time.Second)

	var items []int
	for w := range wm {
		if w.Item != 0 {
			items = append(items, w.Item)
		}
	}
	fmt.Println(items)
	// Output: [10 20 30]
}

func ExampleLateFilterStage() {
	ctx := context.Background()
	now := time.Now()

	in := make(chan pipeline.Watermark[string], 3)
	in <- pipeline.Watermark[string]{Item: "on-time", EventTime: now}
	in <- pipeline.Watermark[string]{Item: "also-on-time", EventTime: now.Add(5 * time.Millisecond)}
	// Simulate a late arrival (500 ms behind the high watermark)
	in <- pipeline.Watermark[string]{Item: "late", EventTime: now.Add(-500 * time.Millisecond)}
	close(in)

	// Allow up to 100 ms of lateness
	out := pipeline.LateFilterStage(ctx, in, 100*time.Millisecond)

	for w := range out {
		fmt.Println(w.Item)
	}
	// Output:
	// on-time
	// also-on-time
}
