package pipeline

import (
	"context"
	"sync"
)

// LookupStage enriches each item by joining it with a value from a lookup table.
// The keyFn extracts a key from each item, and the lookupFn retrieves the
// enriched value. Items for which lookupFn returns (_, false) are dropped.
func LookupStage[T any, K comparable, U any](
	ctx context.Context,
	in <-chan T,
	keyFn func(T) K,
	lookupFn func(K) (U, bool),
) <-chan U {
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
				key := keyFn(item)
				if val, found := lookupFn(key); found {
					select {
					case out <- val:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// MapLookupStage is like LookupStage but the merge function receives both the
// original item and the looked-up value, allowing the caller to combine them
// into a new output type. Items for which lookupFn returns (_, false) are
// dropped.
func MapLookupStage[T any, K comparable, V any, U any](
	ctx context.Context,
	in <-chan T,
	keyFn func(T) K,
	lookupFn func(K) (V, bool),
	mergeFn func(T, V) U,
) <-chan U {
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
				key := keyFn(item)
				if val, found := lookupFn(key); found {
					merged := mergeFn(item, val)
					select {
					case out <- merged:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// CacheLookup returns a thread-safe lookup function backed by an in-memory map.
// It is useful for building lookup tables that can be passed to LookupStage or
// MapLookupStage.
func CacheLookup[K comparable, V any](initial map[K]V) (lookup func(K) (V, bool), set func(K, V), del func(K)) {
	var mu sync.RWMutex
	cache := make(map[K]V, len(initial))
	for k, v := range initial {
		cache[k] = v
	}

	lookup = func(k K) (V, bool) {
		mu.RLock()
		defer mu.RUnlock()
		v, ok := cache[k]
		return v, ok
	}

	set = func(k K, v V) {
		mu.Lock()
		defer mu.Unlock()
		cache[k] = v
	}

	del = func(k K) {
		mu.Lock()
		defer mu.Unlock()
		delete(cache, k)
	}

	return lookup, set, del
}
