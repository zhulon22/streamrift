package pipeline

import (
	"context"
	"time"
)

// TimeoutStage wraps each item processing with a per-item deadline. If the
// downstream cannot accept an item within the given timeout, the item is
// dropped and processing continues.
func TimeoutStage[T any](ctx context.Context, in <-chan T, timeout time.Duration) <-chan T {
	out := make(chan T)
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
				timer := time.NewTimer(timeout)
				select {
				case out <- item:
				case <-timer.C:
					// item dropped — downstream too slow
				case <-ctx.Done():
					timer.Stop()
					return
				}
				timer.Stop()
			}
		}
	}()
	return out
}

// DeadlineStage closes the output channel and stops forwarding items once the
// given absolute deadline is reached, regardless of whether the input is
// exhausted.
func DeadlineStage[T any](ctx context.Context, in <-chan T, deadline time.Time) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- item:
				case <-timer.C:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
