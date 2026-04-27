package pipeline

import (
	"context"
	"sync"
)

// Checkpoint represents a saved position in a stream, allowing resumption
// after failure or restart.
type Checkpoint[T any] struct {
	mu       sync.Mutex
	last     T
	hasValue bool
	save     func(T) error
	load     func() (T, bool, error)
}

// CheckpointOptions configures the behaviour of CheckpointStage.
type CheckpointOptions[T any] struct {
	// Save is called every time a new item passes through the stage.
	// It persists the item so it can be recovered on restart.
	Save func(T) error

	// Load is called once when the stage starts. If it returns a valid item
	// (ok == true) the stage emits that item first so downstream can resume
	// from the last known position.
	Load func() (T, bool, error)

	// OnError is called when Save or Load returns an error.
	// If nil, errors are silently ignored.
	OnError func(err error)
}

// CheckpointStage wraps a channel and records each item that passes through
// using a caller-supplied Save function. On startup it optionally replays the
// last saved item via Load, enabling at-least-once delivery semantics.
//
// Example — persist the last processed integer to an in-memory slot:
//
//	var stored int
//	out := CheckpointStage(ctx, in, CheckpointOptions[int]{
//	    Save: func(v int) error { stored = v; return nil },
//	    Load: func() (int, bool, error) { return stored, stored != 0, nil },
//	})
func CheckpointStage[T any](ctx context.Context, in <-chan T, opts CheckpointOptions[T]) <-chan T {
	out := make(chan T)

	handleErr := func(err error) {
		if err != nil && opts.OnError != nil {
			opts.OnError(err)
		}
	}

	go func() {
		defer close(out)

		// Replay the last checkpoint before processing new items.
		if opts.Load != nil {
			if item, ok, err := opts.Load(); err != nil {
				handleErr(err)
			} else if ok {
				select {
				case out <- item:
				case <-ctx.Done():
					return
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				if opts.Save != nil {
					handleErr(opts.Save(item))
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

// InMemoryCheckpoint returns a Save/Load pair backed by a simple in-memory
// variable protected by a mutex. Useful for testing or single-process
// pipelines that only need crash-recovery within a run.
func InMemoryCheckpoint[T any]() (save func(T) error, load func() (T, bool, error)) {
	var (
		mu       sync.Mutex
		stored   T
		hasValue bool
	)

	save = func(v T) error {
		mu.Lock()
		defer mu.Unlock()
		stored = v
		hasValue = true
		return nil
	}

	load = func() (T, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return stored, hasValue, nil
	}

	return save, load
}
