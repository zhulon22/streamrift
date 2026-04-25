package pipeline

import "context"

// RoutePredicate is a function that routes an item to a branch index.
type RoutePredicate[T any] func(T) int

// RouteStage splits a single input channel into n output channels based on a
// predicate function. Items that return an index out of range are dropped.
func RouteStage[T any](ctx context.Context, in <-chan T, n int, pred RoutePredicate[T]) []<-chan T {
	outs := make([]chan T, n)
	result := make([]<-chan T, n)
	for i := 0; i < n; i++ {
		outs[i] = make(chan T)
		result[i] = outs[i]
	}

	go func() {
		defer func() {
			for _, ch := range outs {
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
				idx := pred(item)
				if idx < 0 || idx >= n {
					continue
				}
				select {
				case outs[idx] <- item:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return result
}

// BroadcastStage sends every item from in to all n output channels.
// It blocks until every branch has consumed the item.
func BroadcastStage[T any](ctx context.Context, in <-chan T, n int) []<-chan T {
	outs := make([]chan T, n)
	result := make([]<-chan T, n)
	for i := 0; i < n; i++ {
		outs[i] = make(chan T)
		result[i] = outs[i]
	}

	go func() {
		defer func() {
			for _, ch := range outs {
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
				for _, ch := range outs {
					select {
					case ch <- item:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return result
}
