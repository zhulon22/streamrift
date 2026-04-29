package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/pipeline"
)

func makeSnapChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestSnapshotStage_AccumulatesItems(t *testing.T) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}
	in := makeSnapChan(1, 2, 3, 4, 5)
	out := pipeline.SnapshotStage(ctx, in, snap)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 items forwarded, got %d", len(got))
	}
	if snap.Len() != 5 {
		t.Fatalf("expected snapshot len 5, got %d", snap.Len())
	}
}

func TestSnapshotStage_PassesThroughUnchanged(t *testing.T) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}
	in := makeSnapChan(10, 20, 30)
	out := pipeline.SnapshotStage(ctx, in, snap)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	expected := []int{10, 20, 30}
	for i, v := range expected {
		if got[i] != v {
			t.Errorf("index %d: expected %d got %d", i, v, got[i])
		}
	}
}

func TestSnapshotStage_Reset(t *testing.T) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}
	in := makeSnapChan(1, 2, 3)
	out := pipeline.SnapshotStage(ctx, in, snap)
	for range out {
	}
	if snap.Len() != 3 {
		t.Fatalf("expected 3 before reset, got %d", snap.Len())
	}
	snap.Reset()
	if snap.Len() != 0 {
		t.Fatalf("expected 0 after reset, got %d", snap.Len())
	}
}

func TestSnapshotStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	snap := &pipeline.Snapshot[int]{}
	in := make(chan int)
	out := pipeline.SnapshotStage(ctx, in, snap)
	cancel()
	_, ok := <-out
	if ok {
		t.Error("expected channel to be closed after context cancellation")
	}
}

func TestSnapshotStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	snap := &pipeline.Snapshot[int]{}
	in := makeSnapChan()
	out := pipeline.SnapshotStage(ctx, in, snap)
	for range out {
	}
	if snap.Len() != 0 {
		t.Fatalf("expected empty snapshot, got %d", snap.Len())
	}
}
