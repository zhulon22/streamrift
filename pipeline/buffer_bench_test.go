package pipeline_test

import (
	"context"
	"testing"

	"github.com/yourusername/streamrift/pipeline"
)

func BenchmarkBufferStage(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := make(chan int, b.N)
		for j := 0; j < b.N; j++ {
			in <- j
		}
		close(in)

		out := pipeline.BufferStage[int](b.N)(ctx, in)
		for range out {
		}
	}
}

func BenchmarkDropStage_NoDrop(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := make(chan int, b.N)
		for j := 0; j < b.N; j++ {
			in <- j
		}
		close(in)

		out := pipeline.DropStage[int](b.N)(ctx, in)
		for range out {
		}
	}
}

func BenchmarkDropStage_HighDrop(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := make(chan int, b.N)
		for j := 0; j < b.N; j++ {
			in <- j
		}
		close(in)

		// Very small buffer forces many drops
		out := pipeline.DropStage[int](4)(ctx, in)
		for range out {
		}
	}
}
