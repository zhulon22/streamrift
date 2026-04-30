package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourusername/streamrift/pipeline"
)

func makeAccumChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestAccumulateStage_GrowsWithEachItem(t *testing.T) {
	ctx := context.Background()
	in := makeAccumChan(1, 2, 3)
	out := pipeline.AccumulateStage(ctx, in)

	expected := [][]int{{1}, {1, 2}, {1, 2, 3}}
	for i, want := range expected {
		got, ok := <-out
		if !ok {
			t.Fatalf("channel closed early at index %d", i)
		}
		if len(got) != len(want) {
			t.Errorf("index %d: got len %d, want %d", i, len(got), len(want))
			continue
		}
		for j := range want {
			if got[j] != want[j] {
				t.Errorf("index %d[%d]: got %d, want %d", i, j, got[j], want[j])
			}
		}
	}
	if _, ok := <-out; ok {
		t.Error("expected channel to be closed")
	}
}

func TestAccumulateStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	in := makeAccumChan()
	out := pipeline.AccumulateStage(ctx, in)
	if _, ok := <-out; ok {
		t.Error("expected closed channel for empty input")
	}
}

func TestAccumulateStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int)
	out := pipeline.AccumulateStage(ctx, ch)
	cancel()
	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("stage did not close output after context cancellation")
	}
}

func TestScanStage_RunningSum(t *testing.T) {
	ctx := context.Background()
	in := makeAccumChan(1, 2, 3, 4)
	out := pipeline.ScanStage(ctx, in, 0, func(acc, v int) int { return acc + v })

	want := []int{1, 3, 6, 10}
	for i, w := range want {
		got, ok := <-out
		if !ok {
			t.Fatalf("channel closed early at index %d", i)
		}
		if got != w {
			t.Errorf("index %d: got %d, want %d", i, got, w)
		}
	}
	if _, ok := <-out; ok {
		t.Error("expected channel to be closed after all items")
	}
}

func TestScanStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int)
	out := pipeline.ScanStage(ctx, ch, 0, func(a, b int) int { return a + b })
	cancel()
	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("ScanStage did not close after context cancellation")
	}
}
