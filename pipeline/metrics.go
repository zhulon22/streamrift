package pipeline

import (
	"context"
	"sync/atomic"
	"time"
)

// StageMetrics holds runtime counters for a single pipeline stage.
type StageMetrics struct {
	ItemsIn  atomic.Int64
	ItemsOut atomic.Int64
	Dropped  atomic.Int64
	Errors   atomic.Int64
}

// Snapshot returns a point-in-time copy of the metrics.
type MetricsSnapshot struct {
	ItemsIn  int64
	ItemsOut int64
	Dropped  int64
	Errors   int64
}

func (m *StageMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		ItemsIn:  m.ItemsIn.Load(),
		ItemsOut: m.ItemsOut.Load(),
		Dropped:  m.Dropped.Load(),
		Errors:   m.Errors.Load(),
	}
}

// MeteredStage wraps any stage function and records throughput metrics.
// It instruments the input and output channels transparently.
func MeteredStage[T any](metrics *StageMetrics, stage func(context.Context, <-chan T) <-chan T) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		// Instrument input
		instrumented := make(chan T)
		go func() {
			defer close(instrumented)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					metrics.ItemsIn.Add(1)
					select {
					case instrumented <- v:
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		// Run the real stage
		rawOut := stage(ctx, instrumented)

		// Instrument output
		out := make(chan T)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-rawOut:
					if !ok {
						return
					}
					metrics.ItemsOut.Add(1)
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return out
	}
}

// LatencyStage measures the average processing latency per item through a stage.
// It calls onLatency with the duration for each item that passes through.
func LatencyStage[T any](onLatency func(time.Duration), stage func(context.Context, <-chan T) <-chan T) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		timed := make(chan T)
		go func() {
			defer close(timed)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					start := time.Now()
					select {
					case timed <- v:
						onLatency(time.Since(start))
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return stage(ctx, timed)
	}
}
