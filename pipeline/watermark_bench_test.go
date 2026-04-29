package pipeline

import (
	"context"
	"testing"
	"time"
)

func BenchmarkWatermarkStage(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := make(chan int, 64)
		go func() {
			for j := 0; j < 64; j++ {
				in <- j
			}
			close(in)
		}()
		out := WatermarkStage(ctx, in, 10*time.Second)
		for range out {
		}
	}
}

func BenchmarkLateFilterStage_NoDrops(b *testing.B) {
	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := make(chan Watermark[int], 64)
		go func() {
			for j := 0; j < 64; j++ {
				in <- Watermark[int]{Item: j, EventTime: now.Add(time.Duration(j) * time.Millisecond)}
			}
			close(in)
		}()
		out := LateFilterStage(ctx, in, 10*time.Second)
		for range out {
		}
	}
}

func BenchmarkLateFilterStage_AllDrops(b *testing.B) {
	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in := make(chan Watermark[int], 64)
		go func() {
			// First item advances the watermark far ahead
			in <- Watermark[int]{Item: 0, EventTime: now.Add(10 * time.Second)}
			for j := 1; j < 64; j++ {
				in <- Watermark[int]{Item: j, EventTime: now}
			}
			close(in)
		}()
		out := LateFilterStage(ctx, in, 100*time.Millisecond)
		for range out {
		}
	}
}
