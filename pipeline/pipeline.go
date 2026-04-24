package pipeline

import (
	"context"
)

// Stage is a function that transforms a channel of type In into a channel of type Out.
type Stage[In any, Out any] func(ctx context.Context, in <-chan In) <-chan Out

// Pipeline holds a source channel and a context for orchestrating stages.
type Pipeline[T any] struct {
	ctx context.Context
	src <-chan T
}

// New creates a new Pipeline from the given context and source channel.
func New[T any](ctx context.Context, src <-chan T) *Pipeline[T] {
	return &Pipeline[T]{ctx: ctx, src: src}
}

// Run executes the pipeline and returns the output channel.
func (p *Pipeline[T]) Run() <-chan T {
	return p.src
}

// MapStage applies a transformation function to each element in the stream.
func MapStage[T any, U any](fn func(T) U) Stage[T, U] {
	return func(ctx context.Context, in <-chan T) <-chan U {
		out := make(chan U)
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
					select {
					case out <- fn(item):
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return out
	}
}

// Pipe connects a Stage to a Pipeline, returning a new Pipeline with the transformed type.
func Pipe[In any, Out any](p *Pipeline[In], stage Stage[In, Out]) *Pipeline[Out] {
	return &Pipeline[Out]{
		ctx: p.ctx,
		src: stage(p.ctx, p.src),
	}
}

// Collect drains the pipeline output channel into a slice.
func Collect[T any](ctx context.Context, ch <-chan T) []T {
	var result []T
	for {
		select {
		case <-ctx.Done():
			return result
		case item, ok := <-ch:
			if !ok {
				return result
			}
			result = append(result, item)
		}
	}
}
