package pipeline

import "context"

// InspectStage passes each item through unchanged, calling fn as a side effect.
// Useful for logging, debugging, or metrics collection without altering the stream.
func InspectStage[T any](ctx context.Context, in <-chan T, fn func(T)) <-chan T {
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
				fn(item)
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

// TapStage is an alias for InspectStage with a named tap function, allowing
// multiple named observation points to be inserted into a pipeline.
func TapStage[T any](ctx context.Context, in <-chan T, name string, fn func(string, T)) <-chan T {
	return InspectStage(ctx, in, func(item T) {
		fn(name, item)
	})
}
