package pipeline

import (
	"context"
	"time"
)

// RetryConfig holds configuration for the RetryStage.
type RetryConfig struct {
	// MaxAttempts is the maximum number of times to attempt processing an item.
	// A value of 0 or 1 means no retries (single attempt).
	MaxAttempts int
	// Delay is the duration to wait between retry attempts.
	Delay time.Duration
	// ShouldRetry is an optional predicate; if nil, all errors trigger a retry.
	ShouldRetry func(err error) bool
}

// RetryStage wraps a fallible transform function and retries on failure according
// to the provided RetryConfig. Items that ultimately fail are dropped and the
// error is sent to the errCh channel (if non-nil).
func RetryStage[T any, U any](
	ctx context.Context,
	in <-chan T,
	cfg RetryConfig,
	errCh chan<- error,
	fn func(T) (U, error),
) <-chan U {
	out := make(chan U)

	maxAttempts := cfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	shouldRetry := cfg.ShouldRetry
	if shouldRetry == nil {
		shouldRetry = func(error) bool { return true }
	}

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
				var (
					result U
					err    error
				)
				for attempt := 0; attempt < maxAttempts; attempt++ {
					result, err = fn(item)
					if err == nil {
						break
					}
					if !shouldRetry(err) {
						break
					}
					if attempt < maxAttempts-1 && cfg.Delay > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(cfg.Delay):
						}
					}
				}
				if err != nil {
					if errCh != nil {
						select {
						case errCh <- err:
						default:
						}
					}
					continue
				}
				select {
				case out <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}
