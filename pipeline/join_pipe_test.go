package pipeline_test

import (
	"context"
	"testing"

	"github.com/your-org/streamrift/pipeline"
)

// TestConcatStage_WithPipe verifies ConcatStage integrates with Pipe and Collect.
func TestConcatStage_WithPipe(t *testing.T) {
	ctx := context.Background()

	src1 := make(chan int, 3)
	src1 <- 1
	src1 <- 2
	src1 <- 3
	close(src1)

	src2 := make(chan int, 2)
	src2 <- 4
	src2 <- 5
	close(src2)

	concatenated := pipeline.ConcatStage(ctx, src1, src2)

	// Double each value via MapStage
	doubled := pipeline.MapStage(ctx, concatenated, func(v int) int { return v * 2 })

	results, err := pipeline.Collect(ctx, doubled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []int{2, 4, 6, 8, 10}
	if len(results) != len(want) {
		t.Fatalf("expected %v, got %v", want, results)
	}
	for i := range want {
		if results[i] != want[i] {
			t.Errorf("index %d: want %d got %d", i, want[i], results[i])
		}
	}
}

// TestJoinStage_WithCollect verifies JoinStage output can be collected via Collect.
func TestJoinStage_WithCollect(t *testing.T) {
	ctx := context.Background()

	left := make(chan int, 4)
	right := make(chan int, 4)
	for _, v := range []int{10, 20, 30, 40} {
		left <- v
		right <- v
	}
	close(left)
	close(right)

	out := pipeline.JoinStage(ctx, left, right,
		func(v int) int { return v },
		func(v int) int { return v },
	)

	sums := pipeline.MapStage(ctx, out, func(j pipeline.Joined[int, int]) int {
		return j.Left + j.Right
	})

	results, err := pipeline.Collect(ctx, sums)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	for _, r := range results {
		if r%20 != 0 {
			t.Errorf("expected multiple of 20, got %d", r)
		}
	}
}
