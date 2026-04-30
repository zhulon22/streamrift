package pipeline

import "context"

// AccumulateStage buffers incoming items and emits a running slice each time
// a new item arrives. The emitted slice grows with every item received.
// When the context is cancelled the stage closes its output channel.
func AccumulateStage[T any](ctx context.Context, in <-chan T) <-chan []T {
	out := make(chan []T)
	go func() {
		defer close(out)
		var acc []T
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				acc = append(acc, item)
				copy := make([]T, len(acc))
				copy[len(copy)-1] = item
				_ = builtin_copy(copy, acc)
				select {
				case out <- copy:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// builtin_copy is an alias kept here so the file compiles without importing
// an extra package — the real copy built-in is used directly below.
func builtin_copy[T any](dst, src []T) int { return copy(dst, src) }

// ScanStage is like ReduceStage but emits every intermediate accumulation
// rather than only the final result, making it useful for running totals.
func ScanStage[T any, A any](ctx context.Context, in <-chan T, seed A, fn func(A, T) A) <-chan A {
	out := make(chan A)
	go func() {
		defer close(out)
		acc := seed
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				acc = fn(acc, item)
				select {
				case out <- acc:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
