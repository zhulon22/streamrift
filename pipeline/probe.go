package pipeline

import (
	"context"
	"sync/atomic"
)

// ProbeStats holds counters captured by a ProbeStage.
type ProbeStats struct {
	In  uint64
	Out uint64
	Dropped uint64
}

// Probe is a handle returned by ProbeStage that exposes live counters.
type Probe struct {
	in      atomic.Uint64
	out     atomic.Uint64
	dropped atomic.Uint64
}

// Stats returns a snapshot of the current probe counters.
func (p *Probe) Stats() ProbeStats {
	return ProbeStats{
		In:      p.in.Load(),
		Out:     p.out.Load(),
		Dropped: p.dropped.Load(),
	}
}

// Reset zeroes all counters atomically.
func (p *Probe) Reset() {
	p.in.Store(0)
	p.out.Store(0)
	p.dropped.Store(0)
}

// ProbeStage wraps an input channel and counts items that pass through.
// It returns both the output channel and a *Probe for reading live stats.
// Items are forwarded without modification; if the downstream is full and
// dropOnFull is true the item is counted as dropped and discarded instead
// of blocking.
func ProbeStage[T any](ctx context.Context, in <-chan T, dropOnFull bool) (<-chan T, *Probe) {
	probe := &Probe{}
	out := make(chan T, cap(in))

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				probe.in.Add(1)
				if dropOnFull {
					select {
					case out <- item:
						probe.out.Add(1)
					default:
						probe.dropped.Add(1)
					}
				} else {
					select {
					case out <- item:
						probe.out.Add(1)
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out, probe
}
