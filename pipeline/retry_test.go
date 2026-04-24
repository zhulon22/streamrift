package pipeline_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/user/streamrift/pipeline"
)

func makeIntChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestRetryStage_SuccessOnFirstAttempt(t *testing.T) {
	ctx := context.Background()
	in := makeIntChan(1, 2, 3)
	cfg := pipeline.RetryConfig{MaxAttempts: 3}

	out := pipeline.RetryStage(ctx, in, cfg, nil, func(v int) (int, error) {
		return v * 10, nil
	})

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 || got[0] != 10 || got[1] != 20 || got[2] != 30 {
		t.Fatalf("unexpected results: %v", got)
	}
}

func TestRetryStage_RetriesAndSucceeds(t *testing.T) {
	ctx := context.Background()
	in := makeIntChan(42)
	cfg := pipeline.RetryConfig{MaxAttempts: 3, Delay: time.Millisecond}

	var calls atomic.Int32
	out := pipeline.RetryStage(ctx, in, cfg, nil, func(v int) (int, error) {
		if calls.Add(1) < 3 {
			return 0, errors.New("transient")
		}
		return v, nil
	})

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 1 || got[0] != 42 {
		t.Fatalf("expected [42], got %v", got)
	}
}

func TestRetryStage_DropsAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	in := makeIntChan(1, 2)
	errCh := make(chan error, 10)
	cfg := pipeline.RetryConfig{MaxAttempts: 2}

	out := pipeline.RetryStage(ctx, in, cfg, errCh, func(v int) (int, error) {
		return 0, errors.New("always fails")
	})

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Fatalf("expected no results, got %v", got)
	}
	if len(errCh) == 0 {
		t.Fatal("expected errors to be forwarded")
	}
}

func TestRetryStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := makeIntChan(1, 2, 3)
	cfg := pipeline.RetryConfig{MaxAttempts: 1}

	out := pipeline.RetryStage(ctx, in, cfg, nil, func(v int) (int, error) {
		return v, nil
	})

	var got []int
	for v := range out {
		got = append(got, v)
	}
	// With a cancelled context we may get 0 or some items; channel must close.
	_ = got
}
