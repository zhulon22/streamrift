package pipeline_test

import (
	"context"
	"sync"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func TestInspectStage_CallsSideEffect(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 5)
	for _, v := range []int{1, 2, 3, 4, 5} {
		in <- v
	}
	close(in)

	var mu sync.Mutex
	var seen []int
	out := pipeline.InspectStage(ctx, in, func(v int) {
		mu.Lock()
		seen = append(seen, v)
		mu.Unlock()
	})

	var result []int
	for v := range out {
		result = append(result, v)
	}

	if len(result) != 5 {
		t.Fatalf("expected 5 items, got %d", len(result))
	}
	if len(seen) != 5 {
		t.Fatalf("expected side effect called 5 times, got %d", len(seen))
	}
	for i, v := range result {
		if seen[i] != v {
			t.Errorf("seen[%d]=%d, result[%d]=%d", i, seen[i], i, v)
		}
	}
}

func TestInspectStage_PassesThroughUnchanged(t *testing.T) {
	ctx := context.Background()
	in := make(chan string, 3)
	in <- "a"
	in <- "b"
	in <- "c"
	close(in)

	out := pipeline.InspectStage(ctx, in, func(s string) {})

	var got []string
	for s := range out {
		got = append(got, s)
	}
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("unexpected output: %v", got)
	}
}

func TestInspectStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)

	out := pipeline.InspectStage(ctx, in, func(int) {})
	cancel()

	_, ok := <-out
	if ok {
		t.Error("expected channel to be closed after context cancellation")
	}
}

func TestTapStage_IncludesName(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 2)
	in <- 10
	in <- 20
	close(in)

	var calls []string
	out := pipeline.TapStage(ctx, in, "debug", func(name string, v int) {
		calls = append(calls, name)
	})

	for range out {
	}

	if len(calls) != 2 {
		t.Fatalf("expected 2 tap calls, got %d", len(calls))
	}
	for _, c := range calls {
		if c != "debug" {
			t.Errorf("expected tap name 'debug', got %q", c)
		}
	}
}
