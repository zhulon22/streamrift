package pipeline

import "context"

// SequenceStage emits items from the input channel with an incrementing index
// attached, producing Indexed[T] values. Useful for tracking order or
// correlating items downstream.
type Indexed[T any] struct {
	Index int
	Value T
}

// IndexStage wraps each incoming item with its zero-based position index.
func IndexStage[T any](ctx context.Context, in <-chan T) <-chan Indexed[T] {
	out := make(chan Indexed[T])
	go func() {
		defer close(out)
		idx := 0
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- Indexed[T]{Index: idx, Value: v}:
					idx++
				}
			}
		}
	}()
	return out
}

// EnumerateStage is an alias-style helper that extracts the value from an
// Indexed channel, applying a transform that receives both index and value.
func EnumerateStage[T any, U any](ctx context.Context, in <-chan Indexed[T], fn func(int, T) U) <-chan U {
	out := make(chan U)
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
				result := fn(item.Index, item.Value)
				select {
				case <-ctx.Done():
					return
				case out <- result:
				}
			}
		}
	}()
	return out
}
