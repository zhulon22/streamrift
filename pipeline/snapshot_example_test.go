package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/pipeline"
)

func ExampleSnapshotStage() {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}

	in := make(chan int, 4)
	for _, v := range []int{1, 2, 3, 4} {
		in <- v
	}
	close(in)

	out := pipeline.SnapshotStage(ctx, in, snap)
	for range out {
	}

	fmt.Println("snapshot len:", snap.Len())
	fmt.Println("items:", snap.Items())
	// Output:
	// snapshot len: 4
	// items: [1 2 3 4]
}

func ExampleSnapshotStage_reset() {
	ctx := context.Background()
	snap := &pipeline.Snapshot[string]{}

	in := make(chan string, 3)
	for _, v := range []string{"a", "b", "c"} {
		in <- v
	}
	close(in)

	out := pipeline.SnapshotStage(ctx, in, snap)
	for range out {
	}

	fmt.Println("before reset:", snap.Len())
	snap.Reset()
	fmt.Println("after reset:", snap.Len())
	// Output:
	// before reset: 3
	// after reset: 0
}
