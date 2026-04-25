package pipeline

import "context"

// DedupeStage filters out consecutive duplicate items from the stream.
// Two items are considered equal if the key function returns the same value.
func DedupeStage[T any, K comparable](ctx context.Context, in <-chan T, key func(T) K) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		var lastKey K
		first := true
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				k := key(item)
				if first || k != lastKey {
					first = false
					lastKey = k
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

// DistinctStage filters out all duplicate items seen so far in the stream.
// It maintains a set of seen keys and only forwards items with unseen keys.
func DistinctStage[T any, K comparable](ctx context.Context, in <-chan T, key func(T) K) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		seen := make(map[K]struct{})
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				k := key(item)
				if _, exists := seen[k]; !exists {
					seen[k] = struct{}{}
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
