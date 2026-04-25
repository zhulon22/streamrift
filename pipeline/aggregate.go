package pipeline

import "context"

// AggregateStage collects items from in and emits a single aggregated result
// using the provided combine function. The zero value of A is used as the
// initial accumulator. The result is emitted when the input channel closes.
func AggregateStage[T any, A any](ctx context.Context, in <-chan T, initial A, combine func(A, T) A) <-chan A {
	out := make(chan A, 1)
	go func() {
		defer close(out)
		acc := initial
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					select {
					case out <- acc:
					case <-ctx.Done():
					}
					return
				}
				acc = combine(acc, item)
			}
		}
	}()
	return out
}

// CountStage counts the number of items passing through and emits the total
// count as an int64 once the input channel is closed.
func CountStage[T any](ctx context.Context, in <-chan T) <-chan int64 {
	return AggregateStage(ctx, in, int64(0), func(acc int64, _ T) int64 {
		return acc + 1
	})
}

// SumStage sums numeric items from the input channel and emits the total.
// The type N must be an integer or float type.
func SumStage[N interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}](ctx context.Context, in <-chan N) <-chan N {
	return AggregateStage(ctx, in, N(0), func(acc, item N) N {
		return acc + item
	})
}
