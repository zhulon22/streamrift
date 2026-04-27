package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func BenchmarkInspectStage(b *testing.B) {
	ctx := context.Background()

	in := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		in <- i
	}
	close(in)

	b.ResetTimer()
	out := pipeline.InspectStage(ctx, in, func(int) {})
	for range out {
	}
}

func BenchmarkTapStage(b *testing.B) {
	ctx := context.Background()

	in := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		in <- i
	}
	close(in)

	b.ResetTimer()
	out := pipeline.TapStage(ctx, in, "bench", func(string, int) {})
	for range out {
	}
}
