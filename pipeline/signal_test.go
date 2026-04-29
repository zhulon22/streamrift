package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func makeSignalChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestSignalStage_ForwardsAllItems(t *testing.T) {
	ctx := context.Background()
	in := makeSignalChan(1, 2, 3)
	sig := make(chan struct{}) // never fires
	out := pipeline.SignalStage(ctx, in, sig)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestSignalStage_EmitsOnSignal(t *testing.T) {
	ctx := context.Background()
	in := make(chan int)
	sig := make(chan struct{}, 1)
	out := pipeline.SignalStage(ctx, in, sig)

	// send one item then trigger signal
	in <- 42
	time.Sleep(10 * time.Millisecond)
	sig <- struct{}{}
	close(in)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	// first from normal forward, second from signal re-emit
	if len(got) < 2 {
		t.Fatalf("expected at least 2 items, got %d: %v", len(got), got)
	}
	if got[len(got)-1] != 42 {
		t.Errorf("expected last item 42, got %d", got[len(got)-1])
	}
}

func TestSignalStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	sig := make(chan struct{})
	out := pipeline.SignalStage(ctx, in, sig)
	cancel()

	select {
	case <-out:
	case <-time.After(100 * time.Millisecond):
	}
}

func TestGateStage_ForwardsWhenOpen(t *testing.T) {
	ctx := context.Background()
	in := makeSignalChan(1, 2, 3)
	gate := make(chan bool, 1)
	gate <- true
	out := pipeline.GateStage(ctx, in, gate)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestGateStage_DropsWhenClosed(t *testing.T) {
	ctx := context.Background()
	in := make(chan int, 4)
	for i := 0; i < 4; i++ {
		in <- i
	}
	close(in)

	gate := make(chan bool, 2)
	gate <- false // gate closed initially
	gate <- true  // re-open mid-stream

	out := pipeline.GateStage(ctx, in, gate)

	var got []int
	for v := range out {
		got = append(got, v)
	}
	if len(got) == 4 {
		t.Error("expected some items to be dropped when gate is closed")
	}
}

func TestGateStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	gate := make(chan bool)
	out := pipeline.GateStage(ctx, in, gate)
	cancel()

	select {
	case <-out:
	case <-time.After(100 * time.Millisecond):
	}
}
