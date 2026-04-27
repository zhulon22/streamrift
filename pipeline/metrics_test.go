package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/your-org/streamrift/pipeline"
)

func makeMetricsChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestMeteredStage_CountsItemsInAndOut(t *testing.T) {
	ctx := context.Background()
	metrics := &pipeline.StageMetrics{}

	passThrough := func(ctx context.Context, in <-chan int) <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			for v := range in {
				out <- v
			}
		}()
		return out
	}

	metered := pipeline.MeteredStage(metrics, passThrough)
	out := metered(ctx, makeMetricsChan(1, 2, 3, 4, 5))

	var collected []int
	for v := range out {
		collected = append(collected, v)
	}

	if len(collected) != 5 {
		t.Fatalf("expected 5 items, got %d", len(collected))
	}

	snap := metrics.Snapshot()
	if snap.ItemsIn != 5 {
		t.Errorf("expected ItemsIn=5, got %d", snap.ItemsIn)
	}
	if snap.ItemsOut != 5 {
		t.Errorf("expected ItemsOut=5, got %d", snap.ItemsOut)
	}
}

func TestMeteredStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	metrics := &pipeline.StageMetrics{}

	slow := func(ctx context.Context, in <-chan int) <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			for range in {
				// intentionally slow / blocked
			}
		}()
		return out
	}

	metered := pipeline.MeteredStage(metrics, slow)
	ch := make(chan int, 10)
	ch <- 1
	ch <- 2
	cancel()
	close(ch)

	out := metered(ctx, ch)
	for range out {
	}
	// Should not hang; pass if we reach here.
}

func TestMeteredStage_SnapshotIsIndependent(t *testing.T) {
	metrics := &pipeline.StageMetrics{}
	metrics.ItemsIn.Add(10)
	metrics.ItemsOut.Add(7)
	metrics.Dropped.Add(3)

	s1 := metrics.Snapshot()
	metrics.ItemsIn.Add(5)
	s2 := metrics.Snapshot()

	if s1.ItemsIn != 10 {
		t.Errorf("snapshot 1 ItemsIn should be 10, got %d", s1.ItemsIn)
	}
	if s2.ItemsIn != 15 {
		t.Errorf("snapshot 2 ItemsIn should be 15, got %d", s2.ItemsIn)
	}
}

func TestLatencyStage_CallsCallback(t *testing.T) {
	ctx := context.Background()
	var latencies []time.Duration

	passThrough := func(ctx context.Context, in <-chan int) <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			for v := range in {
				out <- v
			}
		}()
		return out
	}

	latencyStage := pipeline.LatencyStage(func(d time.Duration) {
		latencies = append(latencies, d)
	}, passThrough)

	out := latencyStage(ctx, makeMetricsChan(10, 20, 30))
	var results []int
	for v := range out {
		results = append(results, v)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if len(latencies) != 3 {
		t.Errorf("expected 3 latency callbacks, got %d", len(latencies))
	}
}
