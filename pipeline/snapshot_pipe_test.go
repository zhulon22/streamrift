package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/pipeline"
)

func TestSnapshotStage_WithPipe(t *testing.T) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}

	src := make(chan int, 5)
	for _, v := range []int{1, 2, 3, 4, 5} {
		src <- v
	}
	close(src)

	result, err := pipeline.Collect(ctx,
		pipeline.Pipe(ctx, src,
			pipeline.MapStage(func(v int) int { return v * 2 }),
			func(ctx context.Context, in <-chan int) <-chan int {
				return pipeline.SnapshotStage(ctx, in, snap)
			},
		),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 results, got %d", len(result))
	}
	if snap.Len() != 5 {
		t.Fatalf("expected snapshot len 5, got %d", snap.Len())
	}
	// values should be doubled
	expected := []int{2, 4, 6, 8, 10}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("result[%d]: expected %d got %d", i, v, result[i])
		}
	}
	// snapshot should also hold doubled values
	snapped := snap.Items()
	for i, v := range expected {
		if snapped[i] != v {
			t.Errorf("snap[%d]: expected %d got %d", i, v, snapped[i])
		}
	}
}
