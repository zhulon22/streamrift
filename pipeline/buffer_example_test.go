package pipeline_test

import (
	"context"
	"fmt"

	"github.com/yourusername/streamrift/pipeline"
)

// ExampleBufferStage demonstrates buffering items between pipeline stages
// so that the producer can run ahead of the consumer up to the buffer size.
func ExampleBufferStage() {
	ctx := context.Background()

	in := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)

	out := pipeline.BufferStage[int](8)(ctx, in)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
	// 4
	// 5
}

// ExampleDropStage demonstrates that items are silently dropped when the
// output buffer is full, keeping the upstream producer unblocked.
func ExampleDropStage() {
	ctx := context.Background()

	// Only send a small number of items so the example is deterministic.
	in := make(chan string, 3)
	for _, s := range []string{"a", "b", "c"} {
		in <- s
	}
	close(in)

	// Buffer large enough to hold all items — nothing is dropped here.
	out := pipeline.DropStage[string](10)(ctx, in)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// a
	// b
	// c
}
