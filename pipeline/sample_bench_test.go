package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func BenchmarkTakeStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ch := make(chan int, 1000)
		for j := 0; j < 1000; j++ {
			ch <- j
		}
		close(ch)
		out := pipeline.TakeStage(ctx, ch, 500)
		for range out {
		}
	}
}

func BenchmarkSkipStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ch := make(chan int, 1000)
		for j := 0; j < 1000; j++ {
			ch <- j
		}
		close(ch)
		out := pipeline.SkipStage(ctx, ch, 500)
		for range out {
		}
	}
}

func BenchmarkSampleStage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ch := make(chan int, 1000)
		for j := 0; j < 1000; j++ {
			ch <- j
		}
		close(ch)
		out := pipeline.SampleStage(ctx, ch, 0.5)
		for range out {
		}
	}
}
