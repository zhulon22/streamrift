package pipeline

import "context"

// UniqueStage emits only items that have never been seen before, using a
// user-supplied key function. Unlike DedupeStage (which only removes
// consecutive duplicates), UniqueStage tracks all previously seen keys for
// the lifetime of the stream.
//
// Note: the internal seen-set grows without bound; use DistinctStage from
// dedupe.go when you only need consecutive deduplication.
func UniqueStage[T any, K comparable](ctx context.Context, in <-chan T, key func(T) K) <-chan T {
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
				if _, exists := seen[k]; exists {
					continue
				}
				seen[k] = struct{}{}
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

// EveryNthStage forwards only every nth item from the stream, starting with
// the first item (index 0). Items in between are discarded.
// If n < 1, n is treated as 1 (all items pass).
func EveryNthStage[T any](ctx context.Context, in <-chan T, n int) <-chan T {
	if n < 1 {
		n = 1
	}
	out := make(chan T)
	go func() {
		defer close(out)
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				if i%n == 0 {
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
				i++
			}
		}
	}()
	return out
}
