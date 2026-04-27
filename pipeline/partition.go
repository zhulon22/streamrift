package pipeline

import "context"

// PartitionStage splits incoming items into two output channels based on a
// predicate. Items for which the predicate returns true go to the first
// channel (matched), and the rest go to the second (unmatched).
func PartitionStage[T any](ctx context.Context, in <-chan T, predicate func(T) bool) (<-chan T, <-chan T) {
	matched := make(chan T)
	unmatched := make(chan T)

	go func() {
		defer close(matched)
		defer close(unmatched)

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
					case matched <- item:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case unmatched <- item:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return matched, unmatched
}

// GroupByStage groups incoming items into a map of slices keyed by the result
// of the key function. It consumes the entire input channel and emits a single
// map once the input is exhausted or the context is cancelled.
func GroupByStage[T any, K comparable](ctx context.Context, in <-chan T, key func(T) K) <-chan map[K][]T {
	out := make(chan map[K][]T, 1)

	go func() {
		defer close(out)

		groups := make(map[K][]T)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					select {
					case out <- groups:
					case <-ctx.Done():
					}
					return
				}
				k := key(item)
				groups[k] = append(groups[k], item)
			}
		}
	}()

	return out
}
