package pipeline

import "context"

// SortStage collects all items from the input channel, sorts them using the
// provided less function, and emits them in sorted order.
func SortStage[T any](less func(a, b T) bool) func(ctx context.Context, in <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			var items []T
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						goto emit
					}
					items = append(items, v)
				}
			}
		emit:
			insertionSort(items, less)
			for _, item := range items {
				select {
				case <-ctx.Done():
					return
				case out <- item:
				}
			}
		}()
		return out
	}
}

// TopNStage collects all items and emits only the top N items according to the
// provided less function (items for which less returns true rank higher).
func TopNStage[T any](n int, less func(a, b T) bool) func(ctx context.Context, in <-chan T) <-chan T {
	return func(ctx context.Context, in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			var items []T
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						goto emit
					}
					items = append(items, v)
				}
			}
		emit:
			insertionSort(items, less)
			count := n
			if count > len(items) {
				count = len(items)
			}
			for i := 0; i < count; i++ {
				select {
				case <-ctx.Done():
					return
				case out <- items[i]:
				}
			}
		}()
		return out
	}
}

func insertionSort[T any](items []T, less func(a, b T) bool) {
	for i := 1; i < len(items); i++ {
		key := items[i]
		j := i - 1
		for j >= 0 && less(key, items[j]) {
			items[j+1] = items[j]
			j--
		}
		items[j+1] = key
	}
}
