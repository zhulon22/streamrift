package pipeline

import (
	"context"
	"sync"
)

// Snapshot holds a point-in-time copy of items seen by SnapshotStage.
type Snapshot[T any] struct {
	mu    sync.RWMutex
	items []T
}

// Items returns a copy of the current snapshot contents.
func (s *Snapshot[T]) Items() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]T, len(s.items))
	copy(out, s.items)
	return out
}

// Len returns the number of items currently held in the snapshot.
func (s *Snapshot[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// Reset clears all items from the snapshot.
func (s *Snapshot[T]) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = s.items[:0]
}

// SnapshotStage passes items through unchanged while accumulating them
// into a shared Snapshot that can be read at any time.
func SnapshotStage[T any](ctx context.Context, in <-chan T, snap *Snapshot[T]) <-chan T {
	out := make(chan T)
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
				snap.mu.Lock()
				snap.items = append(snap.items, item)
				snap.mu.Unlock()
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
