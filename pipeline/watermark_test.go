package pipeline

import (
	"context"
	"testing"
	"time"
)

func makeWatermarkChan(vals ...int) <-chan int {
	ch := make(chan int, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestWatermarkStage_StampsAllItems(t *testing.T) {
	ctx := context.Background()
	in := makeWatermarkChan(1, 2, 3)
	before := time.Now()
	out := WatermarkStage(ctx, in, 10*time.Second)

	var results []Watermark[int]
	for wm := range out {
		// skip ticker ticks (zero value)
		if wm.Item != 0 {
			results = append(results, wm)
		}
	}
	after := time.Now()

	if len(results) != 3 {
		t.Fatalf("expected 3 items, got %d", len(results))
	}
	for _, r := range results {
		if r.EventTime.Before(before) || r.EventTime.After(after) {
			t.Errorf("unexpected event time %v", r.EventTime)
		}
	}
}

func TestWatermarkStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := makeWatermarkChan(1, 2, 3)
	out := WatermarkStage(ctx, in, 10*time.Second)
	var count int
	for range out {
		count++
	}
	// some items may slip through before goroutine notices cancellation
	if count > 3 {
		t.Errorf("expected at most 3 items, got %d", count)
	}
}

func TestLateFilterStage_DropsLateItems(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	in := make(chan Watermark[int], 4)
	in <- Watermark[int]{Item: 1, EventTime: now}
	in <- Watermark[int]{Item: 2, EventTime: now.Add(100 * time.Millisecond)}
	// this item is older than the high watermark minus allowedLateness
	in <- Watermark[int]{Item: 3, EventTime: now.Add(-200 * time.Millisecond)}
	in <- Watermark[int]{Item: 4, EventTime: now.Add(200 * time.Millisecond)}
	close(in)

	out := LateFilterStage(ctx, in, 50*time.Millisecond)
	var items []int
	for wm := range out {
		items = append(items, wm.Item)
	}
	for _, item := range items {
		if item == 3 {
			t.Error("late item 3 should have been dropped")
		}
	}
}

func TestLateFilterStage_AllowsOnTimeItems(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	in := make(chan Watermark[int], 3)
	in <- Watermark[int]{Item: 1, EventTime: now}
	in <- Watermark[int]{Item: 2, EventTime: now.Add(10 * time.Millisecond)}
	in <- Watermark[int]{Item: 3, EventTime: now.Add(20 * time.Millisecond)}
	close(in)

	out := LateFilterStage(ctx, in, 1*time.Second)
	var count int
	for range out {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 items, got %d", count)
	}
}

func TestLateFilterStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	in := make(chan Watermark[int])
	close(in)
	out := LateFilterStage(ctx, in, time.Second)
	for range out {
	}
}
