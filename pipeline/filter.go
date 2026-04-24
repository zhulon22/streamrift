package pipeline

import "context"

// FilterFunc is a predicate function that returns true if the item should pass through.
type FilterFunc[T any] func(T) bool

// FilterStage returns a Stage that filters items based on the provided predicate.
// Items for which the predicate returns false are dropped from the stream.
func FilterStage[T any](predicate FilterFunc[T]) Stage[T, T] {
	return func(ctx context.Context, in <-chan T) <-chan T {
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
					if predicate(item) {
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

// ReduceStage returns a Stage that accumulates items into a single result using
// the provided reducer function and an initial accumulator value.
// The output channel emits exactly one value: the final accumulated result.
func ReduceStage[T any, A any](initial A, reducer func(A, T) A) Stage[T, A] {
	return func(ctx context.Context, in <-chan T) <-chan A {
		out := make(chan A, 1)
		go func() {
			defer close(out)
			acc := initial
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						select {
						case out <- acc:
						case <-ctx.Done():
						}
						return
					}
					acc = reducer(acc, item)
				}
			}
		}()
		return out
	}
}
