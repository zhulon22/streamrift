package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/pipeline"
)

func BenchmarkCircuitBreakerStage_Closed(b *testing.B) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(1000, time.Minute)

	in := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		in <- i
	}
	close(in)

	b.ResetTimer()
	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return v, nil
	})
	for range out {
	}
}

func BenchmarkCircuitBreakerStage_Open(b *testing.B) {
	ctx := context.Background()
	cb := pipeline.NewCircuitBreaker(1, time.Minute)
	cb.RecordFailure() // open the circuit

	in := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		in <- i
	}
	close(in)

	b.ResetTimer()
	out := pipeline.CircuitBreakerStage(ctx, in, cb, func(v int) (int, error) {
		return v, nil
	})
	for range out {
	}
}

func BenchmarkCircuitBreaker_Allow(b *testing.B) {
	cb := pipeline.NewCircuitBreaker(1000, time.Minute)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Allow()
	}
}
