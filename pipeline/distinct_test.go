package pipeline_test

import (
	"context"
	"testing"

	"github.com/your-org/streamrift/pipeline"
)

func makeDistinctChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestUniqueStage_RemovesAllDuplicates(t *testing.T) {
	ctx := context.Background()
	in := makeDistinctChan(1, 2, 1, 3, 2, 4, 3)
	out := pipeline.UniqueStage(ctx, in, func(v int) int { return v })

	var got []int
	for v := range out {
		got = append(got, v)
	}

	want := []int{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, got[i])
		}
	}
}

func TestUniqueStage_AllUnique(t *testing.T) {
	ctx := context.Background()
	in := makeDistinctChan(10, 20, 30)
	out := pipeline.UniqueStage(ctx, in, func(v int) int { return v })

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestUniqueStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := makeDistinctChan(1, 2, 3)
	out := pipeline.UniqueStage(ctx, in, func(v int) int { return v })

	var got []int
	for v := range out {
		got = append(got, v)
	}
	// may receive 0 or more items depending on scheduling; channel must close
	_ = got
}

func TestEveryNthStage_EverySecond(t *testing.T) {
	ctx := context.Background()
	in := makeDistinctChan(1, 2, 3, 4, 5, 6)
	out := pipeline.EveryNthStage(ctx, in, 2)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	want := []int{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, got[i])
		}
	}
}

func TestEveryNthStage_N1PassesAll(t *testing.T) {
	ctx := context.Background()
	in := makeDistinctChan(7, 8, 9)
	out := pipeline.EveryNthStage(ctx, in, 1)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestEveryNthStage_NZeroTreatedAsOne(t *testing.T) {
	ctx := context.Background()
	in := makeDistinctChan(1, 2, 3)
	out := pipeline.EveryNthStage(ctx, in, 0)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected all 3 items for n=0, got %d", len(got))
	}
}
