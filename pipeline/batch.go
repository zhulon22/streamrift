package pipeline

import (
	"context"
)

// BatchStage groups items from the input channel into slices of the given size.
// When the input channel closes, any remaining items are emitted as a partial batch.
// This is useful for downstream stages that benefit from bulk processing (e.g. database inserts).
func BatchStage[T any](size int) Stage[T, []T] {
	if size <= 0 {
		panic("streamrift: BatchStage size must be greater than zero")
	}
	return func(ctx context.Context, in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)
			batch := make([]T, 0, size)
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						// Flush any remaining items as a partial batch.
						if len(batch) > 0 {
							select {
							case out <- batch:
							case <-ctx.Done():
							}
						}
						return
					}
					batch = append(batch, item)
					if len(batch) == size {
						// Send a copy so the slice is not reused.
						send := make([]T, size)
						copy(send, batch)
						select {
						case out <- send:
						case <-ctx.Done():
							return
						}
						batch = batch[:0]
					}
				}
			}
		}()
		return out
	}
}

// FlattenStage is the inverse of BatchStage — it takes a channel of slices and
// emits each element individually, effectively flattening one level of nesting.
func FlattenStage[T any]() Stage[[]T, T] {
	return func(ctx context.Context, in <-chan []T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case batch, ok := <-in:
					if !ok {
						return
					}
					for _, item := range batch {
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
}
