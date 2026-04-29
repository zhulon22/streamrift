package pipeline

import (
	"context"
	"sort"
	"sync"
)

// PriorityItem wraps a value with an integer priority (lower = higher priority).
type PriorityItem[T any] struct {
	Value    T
	Priority int
}

// PriorityStage buffers up to capacity items and emits them in priority order
// (lowest Priority value first). When the buffer is full, the lowest-priority
// item is dropped to make room for a higher-priority arrival.
func PriorityStage[T any](ctx context.Context, in <-chan PriorityItem[T], capacity int) <-chan T {
	if capacity < 1 {
		capacity = 1
	}
	out := make(chan T)
	go func() {
		defer close(out)
		var mu sync.Mutex
		buf := make([]PriorityItem[T], 0, capacity)

		pop := func() (PriorityItem[T], bool) {
			mu.Lock()
			defer mu.Unlock()
			if len(buf) == 0 {
				var zero PriorityItem[T]
				return zero, false
			}
			item := buf[0]
			buf = buf[1:]
			return item, true
		}

		push := func(item PriorityItem[T]) {
			mu.Lock()
			defer mu.Unlock()
			if len(buf) >= capacity {
				// drop the last (lowest priority) item
				buf = buf[:len(buf)-1]
			}
			buf = append(buf, item)
			sort.Slice(buf, func(i, j int) bool {
				return buf[i].Priority < buf[j].Priority
			})
		}

		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					// drain remaining buffer
					for {
						pi, ok := pop()
						if !ok {
							return
						}
						select {
						case out <- pi.Value:
						case <-ctx.Done():
							return
						}
					}
				}
				push(item)
				if pi, ok := pop(); ok {
					select {
					case out <- pi.Value:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}
