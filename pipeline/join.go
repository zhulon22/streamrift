package pipeline

import (
	"context"
	"sync"
)

// Joined holds two correlated values from a left and right channel.
type Joined[L any, R any] struct {
	Left  L
	Right R
}

// JoinStage pairs items from two input channels by applying a key function to
// each side and emitting a Joined value whenever keys match. Items from the
// left channel are buffered until a matching right-side key is seen.
// Unmatched items are silently dropped.
func JoinStage[L any, R any, K comparable](
	ctx context.Context,
	left <-chan L,
	right <-chan R,
	leftKey func(L) K,
	rightKey func(R) K,
) <-chan Joined[L, R] {
	out := make(chan Joined[L, R])

	go func() {
		defer close(out)
		buf := make(map[K]L)

		var wg sync.WaitGroup
		var mu sync.Mutex

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-left:
					if !ok {
						return
					}
					mu.Lock()
					buf[leftKey(item)] = item
					mu.Unlock()
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-right:
					if !ok {
						return
					}
					key := rightKey(item)
					mu.Lock()
					l, found := buf[key]
					if found {
						delete(buf, key)
					}
					mu.Unlock()
					if found {
						select {
						case out <- Joined[L, R]{Left: l, Right: item}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()

		wg.Wait()
	}()

	return out
}

// ConcatStage drains each input channel in order, emitting all items from the
// first channel before moving on to subsequent channels.
func ConcatStage[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, in := range inputs {
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						goto next
					}
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
			}
			next:
		}
	}()
	return out
}
