package pipeline

import "context"

// BufferStage introduces a buffered channel between pipeline stages,
// allowing producers to continue emitting without blocking until the
// buffer is full. This is useful for absorbing short bursts of data.
func BufferStage[T any](capacity int) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		if capacity < 1 {
			capacity = 1
		}
		out := make(chan T, capacity)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
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

// DropStage forwards items from the input channel to the output channel
// but silently drops items when the internal buffer is full rather than
// blocking the upstream producer. Useful for lossy pipelines where
// recency matters more than completeness.
func DropStage[T any](capacity int) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		if capacity < 1 {
			capacity = 1
		}
		out := make(chan T, capacity)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					// Non-blocking send: drop if full
					select {
					case out <- v:
					default:
					}
				}
			}
		}()
		return out
	}
}
