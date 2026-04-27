package pipeline_test

import (
	"context"
	"testing"

	"github.com/your-org/streamrift/pipeline"
)

func BenchmarkJoinStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		n := 1000

		left := make(chan int, n)
		right := make(chan int, n)
		for j := 0; j < n; j++ {
			left <- j
			right <- j
		}
		close(left)
		close(right)

		out := pipeline.JoinStage(ctx, left, right,
			func(v int) int { return v },
			func(v int) int { return v },
		)
		for range out {
		}
	}
}

func BenchmarkConcatStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		n := 500

		a := make(chan int, n)
		bb := make(chan int, n)
		for j := 0; j < n; j++ {
			a <- j
			bb <- j + n
		}
		close(a)
		close(bb)

		out := pipeline.ConcatStage(ctx, a, bb)
		for range out {
		}
	}
}
