package pipeline

import (
	"context"
	"time"
)

// TokenBucket holds the state for a simple token-bucket rate limiter.
type TokenBucket struct {
	capacity int
	tokens   int
	refillAt time.Duration // interval between token refills
}

// NewTokenBucket creates a TokenBucket with the given capacity and refill
// interval. A token is added every refillAt duration up to capacity.
func NewTokenBucket(capacity int, refillAt time.Duration) *TokenBucket {
	return &TokenBucket{
		capacity: capacity,
		tokens:   capacity,
		refillAt: refillAt,
	}
}

// RateLimitStage wraps a TokenBucket and blocks upstream items until a token
// is available, providing smooth backpressure-aware rate limiting.
func RateLimitStage[T any](tb *TokenBucket) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			ticker := time.NewTicker(tb.refillAt)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if tb.tokens < tb.capacity {
						tb.tokens++
					}
				case item, ok := <-in:
					if !ok {
						return
					}
					// Block until a token is available.
					for tb.tokens == 0 {
						select {
						case <-ticker.C:
							tb.tokens++
						case <-ctx.Done():
							return
						}
					}
					tb.tokens--
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
}
