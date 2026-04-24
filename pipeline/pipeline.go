package pipeline

import (
	"context"
	"fmt"
	"sync"
)

// Message represents a unit of data flowing through the pipeline.
type Message struct {
	Payload interface{}
	Meta    map[string]string
}

// Stage is a processing unit that transforms messages.
type Stage func(ctx context.Context, in <-chan Message) (<-chan Message, error)

// Pipeline orchestrates a series of stages with backpressure support.
type Pipeline struct {
	mu     sync.Mutex
	stages []Stage
	bufSize int
}

// New creates a new Pipeline with the given buffer size per stage channel.
func New(bufSize int) *Pipeline {
	if bufSize <= 0 {
		bufSize = 1
	}
	return &Pipeline{bufSize: bufSize}
}

// AddStage appends a processing stage to the pipeline.
func (p *Pipeline) AddStage(s Stage) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stages = append(p.stages, s)
	return p
}

// Run executes the pipeline starting from the given source channel.
// It wires stages sequentially and returns the final output channel.
func (p *Pipeline) Run(ctx context.Context, source <-chan Message) (<-chan Message, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.stages) == 0 {
		return nil, fmt.Errorf("pipeline: no stages defined")
	}

	current := source
	for i, stage := range p.stages {
		out, err := stage(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("pipeline: stage %d failed to start: %w", i, err)
		}
		current = out
	}
	return current, nil
}

// MapStage returns a Stage that applies fn to every message payload.
func MapStage(bufSize int, fn func(Message) (Message, error)) Stage {
	return func(ctx context.Context, in <-chan Message) (<-chan Message, error) {
		out := make(chan Message, bufSize)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-in:
					if !ok {
						return
					}
					result, err := fn(msg)
					if err != nil {
						continue // skip errored messages; real impl could send to dead-letter
					}
					select {
					case out <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return out, nil
	}
}
