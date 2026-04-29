package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/pipeline"
)

func BenchmarkSnapshotStage(b *testing.B) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.Reset()
		in := make(chan int, 100)
		for j := 0; j < 100; j++ {
			in <- j
		}
		close(in)
		out := pipeline.SnapshotStage(ctx, in, snap)
		for range out {
		}
	}
}

func BenchmarkSnapshot_Items(b *testing.B) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}
	in := make(chan int, 1000)
	for j := 0; j < 1000; j++ {
		in <- j
	}
	close(in)
	out := pipeline.SnapshotStage(ctx, in, snap)
	for range out {
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = snap.Items()
	}
}
