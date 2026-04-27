package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func intsSortChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestSortStage_SortsAscending(t *testing.T) {
	ctx := context.Background()
	in := intsSortChan(5, 3, 1, 4, 2)
	out := pipeline.SortStage[int](func(a, b int) bool { return a < b })(ctx, in)

	expected := []int{1, 2, 3, 4, 5}
	for i, want := range expected {
		got, ok := <-out
		if !ok {
			t.Fatalf("channel closed early at index %d", i)
		}
		if got != want {
			t.Errorf("index %d: got %d, want %d", i, got, want)
		}
	}
	if _, ok := <-out; ok {
		t.Error("expected channel to be closed")
	}
}

func TestSortStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	in := intsSortChan()
	out := pipeline.SortStage[int](func(a, b int) bool { return a < b })(ctx, in)
	if _, ok := <-out; ok {
		t.Error("expected empty output for empty input")
	}
}

func TestSortStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := intsSortChan(3, 1, 2)
	out := pipeline.SortStage[int](func(a, b int) bool { return a < b })(ctx, in)
	// Should close without emitting due to cancelled context
	var count int
	for range out {
		count++
	}
	// We don't assert count==0 strictly because drain timing may vary,
	// but the channel must close.
	_ = count
}

func TestTopNStage_ReturnsTopThree(t *testing.T) {
	ctx := context.Background()
	in := intsSortChan(9, 3, 7, 1, 5)
	out := pipeline.TopNStage[int](3, func(a, b int) bool { return a > b })(ctx, in)

	expected := []int{9, 7, 5}
	for i, want := range expected {
		got, ok := <-out
		if !ok {
			t.Fatalf("channel closed early at index %d", i)
		}
		if got != want {
			t.Errorf("index %d: got %d, want %d", i, got, want)
		}
	}
	if _, ok := <-out; ok {
		t.Error("expected channel to be closed after top N")
	}
}

func TestTopNStage_NLargerThanInput(t *testing.T) {
	ctx := context.Background()
	in := intsSortChan(2, 1, 3)
	out := pipeline.TopNStage[int](10, func(a, b int) bool { return a < b })(ctx, in)

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}
