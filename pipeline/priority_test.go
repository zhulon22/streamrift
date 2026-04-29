package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/your-org/streamrift/pipeline"
)

func makePriorityChan(items []pipeline.PriorityItem[int]) <-chan pipeline.PriorityItem[int] {
	ch := make(chan pipeline.PriorityItem[int], len(items))
	for _, it := range items {
		ch <- it
	}
	close(ch)
	return ch
}

func TestPriorityStage_EmitsInOrder(t *testing.T) {
	items := []pipeline.PriorityItem[int]{
		{Value: 30, Priority: 3},
		{Value: 10, Priority: 1},
		{Value: 20, Priority: 2},
	}
	ctx := context.Background()
	out := pipeline.PriorityStage(ctx, makePriorityChan(items), 10)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	expected := []int{10, 20, 30}
	if len(got) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
	for i, v := range expected {
		if got[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, got[i])
		}
	}
}

func TestPriorityStage_DropsLowestOnOverflow(t *testing.T) {
	items := []pipeline.PriorityItem[int]{
		{Value: 100, Priority: 10},
		{Value: 1, Priority: 1},
		{Value: 5, Priority: 5},
	}
	ctx := context.Background()
	// capacity=2: after inserting all three, lowest-priority (100) is dropped
	out := pipeline.PriorityStage(ctx, makePriorityChan(items), 2)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	for _, v := range got {
		if v == 100 {
			t.Errorf("expected value 100 to be dropped, but it was emitted")
		}
	}
}

func TestPriorityStage_EmptyInput(t *testing.T) {
	ch := make(chan pipeline.PriorityItem[int])
	close(ch)
	ctx := context.Background()
	out := pipeline.PriorityStage(ctx, ch, 5)
	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Errorf("expected empty output, got %v", got)
	}
}

func TestPriorityStage_ContextCancellation(t *testing.T) {
	ch := make(chan pipeline.PriorityItem[int])
	ctx, cancel := context.WithCancel(context.Background())
	out := pipeline.PriorityStage(ctx, ch, 5)
	cancel()
	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected channel to be closed after context cancellation")
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for channel close")
	}
}
