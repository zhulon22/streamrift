package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func TestDedupeStage_RemovesConsecutiveDuplicates(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 7)
	for _, v := range []int{1, 1, 2, 3, 3, 3, 2} {
		in <- v
	}
	close(in)

	out := pipeline.DedupeStage(ctx, in, func(i int) int { return i })
	got, _ := pipeline.Collect(ctx, out)

	want := []int{1, 2, 3, 2}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: expected %d, got %d", i, want[i], got[i])
		}
	}
}

func TestDedupeStage_AllUnique(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 3)
	for _, v := range []int{1, 2, 3} {
		in <- v
	}
	close(in)

	out := pipeline.DedupeStage(ctx, in, func(i int) int { return i })
	got, _ := pipeline.Collect(ctx, out)

	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestDedupeStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan int, 3)
	for _, v := range []int{1, 2, 3} {
		in <- v
	}
	close(in)

	out := pipeline.DedupeStage(ctx, in, func(i int) int { return i })
	got, _ := pipeline.Collect(context.Background(), out)
	if len(got) > 1 {
		t.Errorf("expected at most 1 item after cancellation, got %d", len(got))
	}
}

func TestDistinctStage_RemovesAllDuplicates(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 7)
	for _, v := range []int{1, 2, 1, 3, 2, 4, 3} {
		in <- v
	}
	close(in)

	out := pipeline.DistinctStage(ctx, in, func(i int) int { return i })
	got, _ := pipeline.Collect(ctx, out)

	want := []int{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: expected %d, got %d", i, want[i], got[i])
		}
	}
}

func TestDistinctStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	in := make(chan string)
	close(in)

	out := pipeline.DistinctStage(ctx, in, func(s string) string { return s })
	got, _ := pipeline.Collect(ctx, out)
	if len(got) != 0 {
		t.Errorf("expected empty result, got %v", got)
	}
}
