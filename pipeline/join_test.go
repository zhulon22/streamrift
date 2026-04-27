package pipeline_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/your-org/streamrift/pipeline"
)

func makeJoinChan[T any](items ...T) <-chan T {
	ch := make(chan T, len(items))
	for _, v := range items {
		ch <- v
	}
	close(ch)
	return ch
}

func TestJoinStage_MatchingKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type record struct {
		ID  int
		Val string
	}

	left := makeJoinChan(record{1, "a"}, record{2, "b"}, record{3, "c"})
	right := makeJoinChan(record{2, "B"}, record{1, "A"}, record{3, "C"})

	out := pipeline.JoinStage(ctx, left, right,
		func(r record) int { return r.ID },
		func(r record) int { return r.ID },
	)

	var results []pipeline.Joined[record, record]
	for j := range out {
		results = append(results, j)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 joined pairs, got %d", len(results))
	}
	sort.Slice(results, func(i, k int) bool {
		return results[i].Left.ID < results[k].Left.ID
	})
	for i, r := range results {
		if r.Left.ID != i+1 || r.Right.ID != i+1 {
			t.Errorf("pair %d: unexpected ids left=%d right=%d", i, r.Left.ID, r.Right.ID)
		}
	}
}

func TestJoinStage_UnmatchedDropped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	left := makeJoinChan(1, 2, 3)
	right := makeJoinChan(2, 4)

	out := pipeline.JoinStage(ctx, left, right,
		func(v int) int { return v },
		func(v int) int { return v },
	)

	var results []pipeline.Joined[int, int]
	for j := range out {
		results = append(results, j)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 match, got %d", len(results))
	}
	if results[0].Left != 2 || results[0].Right != 2 {
		t.Errorf("unexpected join result: %+v", results[0])
	}
}

func TestJoinStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	left := make(chan int)
	right := make(chan int)
	out := pipeline.JoinStage(ctx, left, right,
		func(v int) int { return v },
		func(v int) int { return v },
	)

	select {
	case <-out:
	case <-time.After(time.Second):
		t.Fatal("expected output channel to close after context cancellation")
	}
}

func TestConcatStage_OrderPreserved(t *testing.T) {
	ctx := context.Background()

	a := makeJoinChan(1, 2, 3)
	b := makeJoinChan(4, 5)
	c := makeJoinChan(6)

	out := pipeline.ConcatStage(ctx, a, b, c)

	var got []int
	for v := range out {
		got = append(got, v)
	}

	want := []int{1, 2, 3, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: want %d got %d", i, want[i], got[i])
		}
	}
}

func TestConcatStage_EmptyInputs(t *testing.T) {
	ctx := context.Background()
	out := pipeline.ConcatStage[int](ctx)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty output, got %v", got)
	}
}
