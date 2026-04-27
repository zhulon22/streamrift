package pipeline

import (
	"context"
	"fmt"
)

// ErrorHandler is a function that decides what to do with a processing error.
// It returns true if the item should be retried, false to drop it.
type ErrorHandler[T any] func(item T, err error) bool

// ErrorRecord wraps an item and the error that occurred while processing it.
type ErrorRecord[T any] struct {
	Item  T
	Err   error
	Stage string
}

func (e ErrorRecord[T]) Error() string {
	return fmt.Sprintf("stage %q: %v", e.Stage, e.Err)
}

// ErrorStage routes items that cause errors to a dead-letter channel while
// forwarding successfully processed items downstream.
//
// The process function transforms each item; if it returns an error the item
// is sent to the returned dead-letter channel instead of the output channel.
func ErrorStage[T any, U any](
	ctx context.Context,
	in <-chan T,
	process func(T) (U, error),
	stageName string,
) (<-chan U, <-chan ErrorRecord[T]) {
	out := make(chan U)
	dead := make(chan ErrorRecord[T])

	go func() {
		defer close(out)
		defer close(dead)

		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				result, err := process(item)
				if err != nil {
					select {
					case dead <- ErrorRecord[T]{Item: item, Err: err, Stage: stageName}:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case out <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out, dead
}
