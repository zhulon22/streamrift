package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/pipeline"
)

func makeClampChan[N interface {
	~int | ~float64
}](vals ...N) <-chan N {
	ch := make(chan N, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestClampStage_ClampsLow(t *testing.T) {
	ctx := context.Background()
	in := makeClampChan(-10, 0, 5, 10, 20)
	out := pipeline.ClampStage(ctx, in, 0, 10)

	expected := []int{0, 0, 5, 10, 10}
	for i, want := range expected {
		got, ok := <-out
		if !ok {
			t.Fatalf("channel closed early at index %d", i)
		}
		if got != want {
			t.Errorf("index %d: got %v, want %v", i, got, want)
		}
	}
	if _, ok := <-out; ok {
		t.Error("expected channel to be closed")
	}
}

func TestClampStage_AllWithinRange(t *testing.T) {
	ctx := context.Background()
	in := makeClampChan(3, 5, 7)
	out := pipeline.ClampStage(ctx, in, 0, 10)

	expected := []int{3, 5, 7}
	for i, want := range expected {
		got := <-out
		if got != want {
			t.Errorf("index %d: got %v, want %v", i, got, want)
		}
	}
}

func TestClampStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := makeClampChan(1, 2, 3)
	out := pipeline.ClampStage(ctx, in, 0, 10)

	var count int
	for range out {
		count++
	}
	if count > 1 {
		t.Errorf("expected at most 1 item after cancellation, got %d", count)
	}
}

func TestNormalizeStage_MapsRange(t *testing.T) {
	ctx := context.Background()
	ch := make(chan float64, 3)
	ch <- 0.0
	ch <- 50.0
	ch <- 100.0
	close(ch)

	out := pipeline.NormalizeStage(ctx, ch, 0.0, 100.0, 0.0, 1.0)

	expected := []float64{0.0, 0.5, 1.0}
	for i, want := range expected {
		got := <-out
		if got != want {
			t.Errorf("index %d: got %v, want %v", i, got, want)
		}
	}
}

func TestNormalizeStage_ZeroSrcRange(t *testing.T) {
	ctx := context.Background()
	ch := make(chan float64, 2)
	ch <- 5.0
	ch <- 5.0
	close(ch)

	out := pipeline.NormalizeStage(ctx, ch, 5.0, 5.0, 0.0, 10.0)

	for i := 0; i < 2; i++ {
		got := <-out
		if got != 0.0 {
			t.Errorf("index %d: got %v, want 0.0", i, got)
		}
	}
}
