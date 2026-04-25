package pipeline

import "context"

// SplitStage splits each item into multiple items using a splitter function.
// Each item from the input channel is transformed into zero or more output items.
func SplitStage[T any](ctx context.Context, in <-chan T, splitter func(T) []T) <-chan T {
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
				for _, part := range splitter(item) {
					select {
					case out <- part:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// ZipStage combines items from two channels pairwise into a struct.
// It stops when either input channel is closed or context is cancelled.
func ZipStage[A any, B any](ctx context.Context, left <-chan A, right <-chan B) <-chan ZipPair[A, B] {
	out := make(chan ZipPair[A, B])
	go func() {
		defer close(out)
		for {
			var a A
			var b B
			var ok bool

			select {
			case <-ctx.Done():
				return
			case a, ok = <-left:
				if !ok {
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case b, ok = <-right:
				if !ok {
					return
				}
			}

			select {
			case out <- ZipPair[A, B]{Left: a, Right: b}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// ZipPair holds a pair of values from ZipStage.
type ZipPair[A any, B any] struct {
	Left  A
	Right B
}
