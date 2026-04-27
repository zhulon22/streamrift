package pipeline_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/yourusername/streamrift/pipeline"
)

func makeErrorChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestErrorStage_ForwardsSuccessItems(t *testing.T) {
	ctx := context.Background()
	in := makeErrorChan(1, 2, 3, 4)

	out, dead := pipeline.ErrorStage(ctx, in, func(n int) (string, error) {
		return strconv.Itoa(n), nil
	}, "stringify")

	var results []string
	for s := range out {
		results = append(results, s)
	}
	var deadItems []pipeline.ErrorRecord[int]
	for r := range dead {
		deadItems = append(deadItems, r)
	}

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	if len(deadItems) != 0 {
		t.Fatalf("expected 0 dead-letter items, got %d", len(deadItems))
	}
}

func TestErrorStage_RoutesErrorsToDeadLetter(t *testing.T) {
	ctx := context.Background()
	in := makeErrorChan(1, 2, 3, 4, 5)
	errBad := errors.New("odd not allowed")

	out, dead := pipeline.ErrorStage(ctx, in, func(n int) (int, error) {
		if n%2 != 0 {
			return 0, errBad
		}
		return n * 10, nil
	}, "evens-only")

	var results []int
	for v := range out {
		results = append(results, v)
	}
	var deadItems []pipeline.ErrorRecord[int]
	for r := range dead {
		deadItems = append(deadItems, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}
	if len(deadItems) != 3 {
		t.Fatalf("expected 3 dead-letter items, got %d", len(deadItems))
	}
	for _, rec := range deadItems {
		if rec.Stage != "evens-only" {
			t.Errorf("expected stage 'evens-only', got %q", rec.Stage)
		}
		if !errors.Is(rec.Err, errBad) {
			t.Errorf("unexpected error: %v", rec.Err)
		}
	}
}

func TestErrorStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan int)
	close(in)

	out, dead := pipeline.ErrorStage(ctx, in, func(n int) (int, error) {
		return n, nil
	}, "noop")

	for range out {
	}
	for range dead {
	}
}

func TestErrorRecord_Error(t *testing.T) {
	rec := pipeline.ErrorRecord[int]{
		Item:  42,
		Err:   errors.New("something failed"),
		Stage: "my-stage",
	}
	got := rec.Error()
	expected := `stage "my-stage": something failed`
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}
