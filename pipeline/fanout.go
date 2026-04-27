package pipeline

import (
	"context"
	"sync"
)

// FanOutStage broadcasts each item from the input channel to all provided
// output stage functions, running each in its own goroutine. It returns a
// single merged output channel that collects results from all branches.
//
// If any branch's context is cancelled, that branch stops processing but
// other branches continue until the input is exhausted or their context
// is also cancelled.
func FanOutStage[T any](branches ...func(ctx context.Context, in <-chan T) <-chan T) func(ctx context.Context, in <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)

		if len(branches) == 0 {
			close(out)
			return out
		}

		// Create a per-branch input channel so each branch receives every item.
		branchInputs := make([]chan T, len(branches))
		for i := range branchInputs {
			branchInputs[i] = make(chan T)
		}

		// Distribute items from the shared input to each branch input channel.
		go func() {
			defer func() {
				for _, ch := range branchInputs {
					close(ch)
				}
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					for _, ch := range branchInputs {
						select {
						case ch <- item:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()

		// Start each branch and collect its output channel.
		branchOutputs := make([]<-chan T, len(branches))
		for i, branch := range branches {
			branchOutputs[i] = branch(ctx, branchInputs[i])
		}

		// Merge all branch outputs into the single output channel.
		var wg sync.WaitGroup
		for _, bOut := range branchOutputs {
			wg.Add(1)
			go func(ch <-chan T) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case item, ok := <-ch:
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
			}(bOut)
		}

		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}
}

// MergeStage combines multiple input channels into a single output channel,
// forwarding items as they arrive. It closes the output once all inputs are
// exhausted or the context is cancelled.
func MergeStage[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	out := make(chan T)

	if len(inputs) == 0 {
		close(out)
		return out
	}

	var wg sync.WaitGroup
	for _, in := range inputs {
		wg.Add(1)
		go func(ch <-chan T) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-ch:
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
		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
