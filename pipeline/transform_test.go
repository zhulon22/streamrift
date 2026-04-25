package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func TestSplitStage_SplitsItems(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	out := pipeline.SplitStage(ctx, in, func(n int) []int {
		return []int{n, n * 10}
	})

	var results []int
	for v := range out {
		results = append(results, v)
	}

	expected := []int{1, 10, 2, 20, 3, 30}
	if len(results) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(results))
	}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestSplitStage_EmptySlice(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 2)
	in <- 1
	in <- 2
	close(in)

	out := pipeline.SplitStage(ctx, in, func(n int) []int {
		return nil
	})

	var results []int
	for v := range out {
		results = append(results, v)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 items, got %d", len(results))
	}
}

func TestSplitStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	cancel()

	out := pipeline.SplitStage(ctx, in, func(n int) []int { return []int{n} })
	for range out {
	}
}

func TestZipStage_PairsItems(t *testing.T) {
	ctx := context.Background()

	left := make(chan int, 3)
	right := make(chan string, 3)
	left <- 1
	left <- 2
	left <- 3
	close(left)
	right <- "a"
	right <- "b"
	right <- "c"
	close(right)

	out := pipeline.ZipStage(ctx, left, right)

	var pairs []pipeline.ZipPair[int, string]
	for p := range out {
		pairs = append(pairs, p)
	}

	if len(pairs) != 3 {
		t.Fatalf("expected 3 pairs, got %d", len(pairs))
	}
	if pairs[0].Left != 1 || pairs[0].Right != "a" {
		t.Errorf("unexpected first pair: %+v", pairs[0])
	}
	if pairs[2].Left != 3 || pairs[2].Right != "c" {
		t.Errorf("unexpected last pair: %+v", pairs[2])
	}
}

func TestZipStage_StopsOnShorterChannel(t *testing.T) {
	ctx := context.Background()

	left := make(chan int, 1)
	right := make(chan int, 3)
	left <- 42
	close(left)
	right <- 1
	right <- 2
	right <- 3
	close(right)

	out := pipeline.ZipStage(ctx, left, right)

	var pairs []pipeline.ZipPair[int, int]
	for p := range out {
		pairs = append(pairs, p)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
}
