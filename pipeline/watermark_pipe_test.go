package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestWatermarkStage_WithPipe verifies that WatermarkStage can be composed
// inside a Pipe chain using MapStage to extract the underlying value.
func TestWatermarkStage_WithPipe(t *testing.T) {
	ctx := context.Background()

	src := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		src <- i
	}
	close(src)

	// Stage 1: attach watermarks
	wm := WatermarkStage(ctx, src, 10*time.Second)

	// Stage 2: filter out ticker ticks (zero-value items) and unwrap
	unwrapped := make(chan int, 5)
	go func() {
		defer close(unwrapped)
		for w := range wm {
			if w.Item != 0 {
				select {
				case unwrapped <- w.Item:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Stage 3: double each value via MapStage
	doubled := MapStage(ctx, unwrapped, func(v int) int { return v * 2 })

	results, err := Collect(ctx, doubled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{2, 4, 6, 8, 10}
	if len(results) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, results)
	}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

// TestLateFilterStage_WithWatermarkStage verifies end-to-end that items
// produced by WatermarkStage are correctly filtered by LateFilterStage.
func TestLateFilterStage_WithWatermarkStage(t *testing.T) {
	ctx := context.Background()

	src := make(chan int, 3)
	src <- 1
	src <- 2
	src <- 3
	close(src)

	// Stamp items; no ticks during this test
	wm := WatermarkStage(ctx, src, 10*time.Second)

	// Allow generous lateness — all items should pass
	filtered := LateFilterStage(ctx, wm, 5*time.Second)

	var items []int
	for w := range filtered {
		if w.Item != 0 {
			items = append(items, w.Item)
		}
	}
	if len(items) != 3 {
		t.Errorf("expected 3 items, got %d: %v", len(items), items)
	}
}
