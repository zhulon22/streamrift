package pipeline

import "context"

// SignalStage emits a value downstream whenever the provided signal channel fires.
// Items from the input channel are forwarded unchanged; additionally, each signal
// triggers emission of the most-recently-seen item (if any).
func SignalStage[T any](ctx context.Context, in <-chan T, signal <-chan struct{}) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		var last T
		var hasLast bool
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				last = item
				hasLast = true
				select {
				case out <- item:
				case <-ctx.Done():
					return
				}
			case _, ok := <-signal:
				if !ok {
					return
				}
				if hasLast {
					select {
					case out <- last:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// GateStage forwards items from in to out only while the gate channel holds true.
// When the gate emits false, items are dropped until the gate emits true again.
func GateStage[T any](ctx context.Context, in <-chan T, gate <-chan bool) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		open := true
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-gate:
				if !ok {
					return
				}
				open = v
			case item, ok := <-in:
				if !ok {
					return
				}
				if open {
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}
