package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func BenchmarkDedupeStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ctx := context.Background()
		in := make(chan int, 1000)
		for j := 0; j < 1000; j++ {
			in <- j % 100 // some duplicates
		}
		close(in)
		b.StartTimer()

		out := pipeline.DedupeStage(ctx, in, func(v int) int { return v })
		pipeline.Collect(ctx, out) //nolint:errcheck
	}
}

func BenchmarkDistinctStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ctx := context.Background()
		in := make(chan int, 1000)
		for j := 0; j < 1000; j++ {
			in <- j % 200
		}
		close(in)
		b.StartTimer()

		out := pipeline.DistinctStage(ctx, in, func(v int) int { return v })
		pipeline.Collect(ctx, out) //nolint:errcheck
	}
}
