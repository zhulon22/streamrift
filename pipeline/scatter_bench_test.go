package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/pipeline"
)

func BenchmarkScatterStage_1Worker(b *testing.B) {
	benchmarkScatter(b, 1)
}

func BenchmarkScatterStage_4Workers(b *testing.B) {
	benchmarkScatter(b, 4)
}

func BenchmarkScatterStage_8Workers(b *testing.B) {
	benchmarkScatter(b, 8)
}

func benchmarkScatter(b *testing.B, workers int) {
	b.Helper()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		in := make(chan int, 1000)
		for j := 0; j < 1000; j++ {
			in <- j
		}
		close(in)
		out := pipeline.ScatterStage(ctx, in, workers, func(v int) (int, bool) {
			return v * 2, true
		})
		for range out {
		}
	}
}

func BenchmarkGatherStage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		chs := make([]<-chan int, 4)
		for j := range chs {
			ch := make(chan int, 250)
			for k := 0; k < 250; k++ {
				ch <- k
			}
			close(ch)
			chs[j] = ch
		}
		out := pipeline.GatherStage(ctx, chs...)
		for range out {
		}
	}
}
