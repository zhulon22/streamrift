package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/pipeline"
)

func makeLimitChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestLimitStage_ReturnsFirstN(t *testing.T) {
	ctx := context.Background()
	in := makeLimitChan(1, 2, 3, 4, 5)
	out := pipeline.LimitStage[int](3)(ctx, in)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
	for i, v := range got {
		if v != i+1 {
			t.Errorf("index %d: expected %d got %d", i, i+1, v)
		}
	}
}

func TestLimitStage_ZeroLimit(t *testing.T) {
	ctx := context.Background()
	in := makeLimitChan(1, 2, 3)
	out := pipeline.LimitStage[int](0)(ctx, in)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 items, got %d", len(got))
	}
}

func TestLimitStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := makeLimitChan(1, 2, 3)
	out := pipeline.LimitStage[int](10)(ctx, in)

	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("channel did not close after context cancellation")
	}
}

func TestOffsetStage_SkipsFirstN(t *testing.T) {
	ctx := context.Background()
	in := makeLimitChan(1, 2, 3, 4, 5)
	out := pipeline.OffsetStage[int](2)(ctx, in)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	expected := []int{3, 4, 5}
	if len(got) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
	for i, v := range got {
		if v != expected[i] {
			t.Errorf("index %d: expected %d got %d", i, expected[i], v)
		}
	}
}

func TestOffsetStage_OffsetBeyondLength(t *testing.T) {
	ctx := context.Background()
	in := makeLimitChan(1, 2, 3)
	out := pipeline.OffsetStage[int](10)(ctx, in)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 items, got %d: %v", len(got), got)
	}
}
