package pipeline

import "context"

// LimitStage forwards at most n items then closes the output channel.
// It is similar to TakeStage but operates as a standalone stage function
// compatible with Pipe.
func LimitStage[T any](n int) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			if n <= 0 {
				return
			}
			count := 0
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
					count++
					if count >= n {
						return
					}
				}
			}
		}()
		return out
	}
}

// OffsetStage skips the first n items then forwards all remaining items.
func OffsetStage[T any](n int) func(context.Context, <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
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
}
