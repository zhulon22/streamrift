package pipeline

import (
	"context"
	"sync"
)

// ScatterStage fans out items from a single input channel to N worker functions
// running concurrently, collecting their results into a single output channel.
// Order of output is not guaranteed.
func ScatterStage[T any, U any](ctx context.Context, in <-chan T, workers int, fn func(T) (U, bool)) <-chan U {
	if workers < 1 {
		workers = 1
	}
	out := make(chan U, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					if result, keep := fn(item); keep {
						select {
						case out <- result:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// GatherStage reads from multiple input channels concurrently and merges all
// items into a single output channel. It closes the output when all inputs
// are exhausted or the context is cancelled.
func GatherStage[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	out := make(chan T, len(inputs))
	var wg sync.WaitGroup
	for _, ch := range inputs {
		wg.Add(1)
		go func(src <-chan T) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-src:
					if !ok {
						return
					}
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
			}
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
