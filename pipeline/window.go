package pipeline

import (
	"context"
	"time"
)

// WindowStage groups items into time-based windows. Each window collects
// items for the given duration, then emits the collected slice downstream.
func WindowStage[T any](duration time.Duration) Stage[T, []T] {
	return func(ctx context.Context, in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)
			ticker := time.NewTicker(duration)
			defer ticker.Stop()
			var buf []T
			for {
				select {
				case <-ctx.Done():
					if len(buf) > 0 {
						select {
						case out <- buf:
						default:
						}
					}
					return
				case item, ok := <-in:
					if !ok {
						if len(buf) > 0 {
							select {
							case out <- buf:
							case <-ctx.Done():
							}
						}
						return
					}
					buf = append(buf, item)
				case <-ticker.C:
					if len(buf) > 0 {
						select {
						case out <- buf:
						case <-ctx.Done():
							return
						}
						buf = nil
					}
				}
			}
		}()
		return out
	}
}

// SlidingWindowStage emits overlapping windows of a fixed size, advancing
// by the given step count on each emission.
func SlidingWindowStage[T any](size, step int) Stage[T, []T] {
	return func(ctx context.Context, in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)
			var buf []T
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-in:
					if !ok {
						return
					}
					buf = append(buf, item)
					if len(buf) == size {
						win := make([]T, size)
						copy(win, buf)
						select {
						case out <- win:
						case <-ctx.Done():
							return
						}
						if step >= size {
							buf = nil
						} else {
							buf = buf[step:]
						}
					}
				}
			}
		}()
		return out
	}
}
