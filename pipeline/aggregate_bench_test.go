package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func BenchmarkAggregateStage(b *testing.B) {
	for b.Loop() {
		ctx := context.Background()
		ch := make(chan int, b.N)
		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
		<-pipeline.AggregateStage(ctx, ch, 0, func(acc, item int) int { return acc + item })
	}
}

func BenchmarkCountStage(b *testing.B) {
	for b.Loop() {
		ctx := context.Background()
		ch := make(chan int, b.N)
		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
		<-pipeline.CountStage(ctx, ch)
	}
}

func BenchmarkSumStage(b *testing.B) {
	for b.Loop() {
		ctx := context.Background()
		ch := make(chan int, b.N)
		for i := 0; i < b.N; i++ {
			ch <- i
		}
		close(ch)
		<-pipeline.SumStage(ctx, ch)
	}
}
