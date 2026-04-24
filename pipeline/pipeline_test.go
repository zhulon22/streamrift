package pipeline_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func sourceChan(msgs ...pipeline.Message) <-chan pipeline.Message {
	ch := make(chan pipeline.Message, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	return ch
}

func TestPipeline_NoStages(t *testing.T) {
	p := pipeline.New(4)
	_, err := p.Run(context.Background(), sourceChan())
	if err == nil {
		t.Fatal("expected error when no stages defined")
	}
}

func TestPipeline_MapStage(t *testing.T) {
	double := pipeline.MapStage(4, func(m pipeline.Message) (pipeline.Message, error) {
		v := m.Payload.(int)
		return pipeline.Message{Payload: v * 2}, nil
	})

	src := sourceChan(
		pipeline.Message{Payload: 1},
		pipeline.Message{Payload: 2},
		pipeline.Message{Payload: 3},
	)

	p := pipeline.New(4)
	p.AddStage(double)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := p.Run(ctx, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []int{2, 4, 6}
	for i, want := range expected {
		msg, ok := <-out
		if !ok {
			t.Fatalf("channel closed early at index %d", i)
		}
		got := msg.Payload.(int)
		if got != want {
			t.Errorf("index %d: got %d, want %d", i, got, want)
		}
	}
}

func TestPipeline_ChainedStages(t *testing.T) {
	addOne := pipeline.MapStage(4, func(m pipeline.Message) (pipeline.Message, error) {
		return pipeline.Message{Payload: m.Payload.(int) + 1}, nil
	})
	toString := pipeline.MapStage(4, func(m pipeline.Message) (pipeline.Message, error) {
		return pipeline.Message{Payload: fmt.Sprintf("val:%d", m.Payload.(int))}, nil
	})

	src := sourceChan(pipeline.Message{Payload: 9})

	p := pipeline.New(4)
	p.AddStage(addOne).AddStage(toString)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := p.Run(ctx, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg := <-out
	if msg.Payload.(string) != "val:10" {
		t.Errorf("unexpected payload: %v", msg.Payload)
	}
}

func TestPipeline_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	blocking := pipeline.MapStage(0, func(m pipeline.Message) (pipeline.Message, error) {
		return m, nil
	})

	src := make(chan pipeline.Message) // never sends
	p := pipeline.New(1)
	p.AddStage(blocking)

	out, err := p.Run(ctx, src)
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected channel to be closed after context cancellation")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timed out waiting for pipeline to stop")
	}
}
