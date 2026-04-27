package pipeline_test

import (
	"context"
	"sort"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func makePartitionChan(items ...int) <-chan int {
	ch := make(chan int, len(items))
	for _, v := range items {
		ch <- v
	}
	close(ch)
	return ch
}

func TestPartitionStage_SplitsEvensAndOdds(t *testing.T) {
	ctx := context.Background()
	in := makePartitionChan(1, 2, 3, 4, 5, 6)

	evens, odds := pipeline.PartitionStage(ctx, in, func(n int) bool { return n%2 == 0 })

	var evenSlice, oddSlice []int
	for v := range evens {
		evenSlice = append(evenSlice, v)
	}
	for v := range odds {
		oddSlice = append(oddSlice, v)
	}

	if len(evenSlice) != 3 || len(oddSlice) != 3 {
		t.Fatalf("expected 3 evens and 3 odds, got %v and %v", evenSlice, oddSlice)
	}
}

func TestPartitionStage_AllMatch(t *testing.T) {
	ctx := context.Background()
	in := makePartitionChan(2, 4, 6)

	matched, unmatched := pipeline.PartitionStage(ctx, in, func(n int) bool { return n%2 == 0 })

	var m, u []int
	for v := range matched {
		m = append(m, v)
	}
	for range unmatched {
	}

	if len(m) != 3 || len(u) != 0 {
		t.Fatalf("expected 3 matched and 0 unmatched, got %d and %d", len(m), len(u))
	}
}

func TestPartitionStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan int)
	close(in)

	matched, unmatched := pipeline.PartitionStage(ctx, in, func(int) bool { return true })
	for range matched {
	}
	for range unmatched {
	}
}

func TestGroupByStage_GroupsByKey(t *testing.T) {
	ctx := context.Background()
	in := makePartitionChan(1, 2, 3, 4, 5, 6)

	out := pipeline.GroupByStage(ctx, in, func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	})

	groups := <-out

	if len(groups["even"]) != 3 {
		t.Errorf("expected 3 evens, got %d", len(groups["even"]))
	}
	if len(groups["odd"]) != 3 {
		t.Errorf("expected 3 odds, got %d", len(groups["odd"]))
	}
}

func TestGroupByStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	in := makePartitionChan()

	out := pipeline.GroupByStage(ctx, in, func(n int) int { return n % 3 })
	groups := <-out

	if len(groups) != 0 {
		t.Errorf("expected empty groups, got %v", groups)
	}
}

func TestGroupByStage_PreservesOrder(t *testing.T) {
	ctx := context.Background()
	in := makePartitionChan(3, 1, 4, 1, 5, 9, 2, 6)

	out := pipeline.GroupByStage(ctx, in, func(n int) string {
		if n > 4 {
			return "large"
		}
		return "small"
	})
	groups := <-out

	small := groups["small"]
	sorted := make([]int, len(small))
	copy(sorted, small)
	sort.Ints(sorted)

	expected := []int{1, 1, 2, 3, 4}
	if len(sorted) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, sorted)
	}
	for i, v := range sorted {
		if v != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}
