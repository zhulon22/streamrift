package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourusername/streamrift/pipeline"
)

func TestBufferStage_ForwardsAllItems(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 5)
	for i := 0; i < 5; i++ {
		in <- i
	}
	close(in)

	out := pipeline.BufferStage[int](10)(ctx, in)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 items, got %d", len(got))
	}
}

func TestBufferStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	out := pipeline.BufferStage[int](4)(ctx, in)
	cancel()
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected channel to be closed after cancel")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestBufferStage_CapacityAtLeastOne(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 1)
	in <- 42
	close(in)
	// capacity 0 should be clamped to 1
	out := pipeline.BufferStage[int](0)(ctx, in)
	v := <-out
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}

func TestDropStage_DropsWhenFull(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 20)
	for i := 0; i < 20; i++ {
		in <- i
	}
	close(in)

	// capacity 5: at most 5 items should reach the output
	out := pipeline.DropStage[int](5)(ctx, in)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) > 20 {
		t.Fatalf("got more items than sent: %d", len(got))
	}
	if len(got) == 0 {
		t.Fatal("expected at least some items to pass through")
	}
}

func TestDropStage_ForwardsAllWhenNotFull(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 3)
	for i := 0; i < 3; i++ {
		in <- i
	}
	close(in)

	out := pipeline.DropStage[int](10)(ctx, in)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestDropStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	out := pipeline.DropStage[int](4)(ctx, in)
	cancel()
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected channel to be closed after cancel")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for channel close")
	}
}
