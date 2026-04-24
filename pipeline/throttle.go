package pipeline

import (
	"context"
	"time"
)

// ThrottleStage limits the rate of items passing through the pipeline to at
// most one item per interval. Excess items are dropped if the downstream
// channel is not ready.
func ThrottleStage[T any](interval time.Duration) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					// Wait for the next tick before forwarding.
					select {
					case <-ticker.C:
					case <-ctx.Done():
						return
					}
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return out
	}
}

// DebounceStage suppresses items until no new item has arrived for the given
// quiet duration, then forwards the latest item. Useful for noisy event
// streams where only the final value matters.
func DebounceStage[T any](quiet time.Duration) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			var (
				pending T
				hasPending bool
				timer      *time.Timer
			)
			var timerC <-chan time.Time
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						// Flush any pending item before closing.
						if hasPending {
							if timer != nil {
								timer.Stop()
							}
							select {
							case out <- pending:
							case <-ctx.Done():
							}
						}
						return
					}
					pending = item
					hasPending = true
					if timer != nil {
						timer.Stop()
					}
					timer = time.NewTimer(quiet)
					timerC = timer.C
				case <-timerC:
					timerC = nil
					if hasPending {
						hasPending = false
						select {
						case out <- pending:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()
		return out
	}
}
