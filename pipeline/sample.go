package pipeline

import (
	"context"
	"math/rand"
)

// SampleStage passes each item downstream with probability p (0.0–1.0).
// Items are dropped randomly based on the given probability.
func SampleStage[T any](ctx context.Context, in <-chan T, p float64) <-chan T {
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
				if rand.Float64() < p {
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

// TakeStage emits at most n items from the input channel, then closes.
func TakeStage[T any](ctx context.Context, in <-chan T, n int) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		count := 0
		for {
			if count >= n {
				return
			}
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- item:
					count++
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// SkipStage discards the first n items and forwards the rest.
func SkipStage[T any](ctx context.Context, in <-chan T, n int) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		skipped := 0
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				if skipped < n {
					skipped++
					continue
				}
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
