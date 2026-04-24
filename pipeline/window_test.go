package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/streamrift/streamrift/pipeline"
)

func TestWindowStage_CollectsItems(t *testing.T) {
	ctx := context.Background()
	ch := make(chan int, 6)
	for _, v := range []int{1, 2, 3, 4, 5} {
		ch <- v
	}
	close(ch)

	win := pipeline.WindowStage[int](50 * time.Millisecond)
	out := win(ctx, ch)

	var all []int
	for window := range out {
		all = append(all, window...)
	}
	if len(all) != 5 {
		t.Fatalf("expected 5 items across windows, got %d", len(all))
	}
}

func TestWindowStage_EmitsOnTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 0; i < 4; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
			time.Sleep(40 * time.Millisecond)
		}
	}()

	win := pipeline.WindowStage[int](60 * time.Millisecond)
	out := win(ctx, ch)

	windowCount := 0
	for range out {
		windowCount++
	}
	if windowCount < 1 {
		t.Fatal("expected at least one window to be emitted")
	}
}

func TestSlidingWindowStage_Basic(t *testing.T) {
	ctx := context.Background()
	ch := make(chan int, 5)
	for _, v := range []int{1, 2, 3, 4, 5} {
		ch <- v
	}
	close(ch)

	stage := pipeline.SlidingWindowStage[int](3, 1)
	out := stage(ctx, ch)

	var windows [][]int
	for w := range out {
		win := make([]int, len(w))
		copy(win, w)
		windows = append(windows, win)
	}
	// expect windows: [1,2,3], [2,3,4], [3,4,5]
	if len(windows) != 3 {
		t.Fatalf("expected 3 sliding windows, got %d", len(windows))
	}
	expected := [][]int{{1, 2, 3}, {2, 3, 4}, {3, 4, 5}}
	for i, w := range windows {
		for j, v := range w {
			if v != expected[i][j] {
				t.Errorf("window[%d][%d] = %d, want %d", i, j, v, expected[i][j])
			}
		}
	}
}

func TestSlidingWindowStage_StepEqualsSize(t *testing.T) {
	ctx := context.Background()
	ch := make(chan int, 6)
	for _, v := range []int{1, 2, 3, 4} {
		ch <- v
	}
	close(ch)

	stage := pipeline.SlidingWindowStage[int](2, 2)
	out := stage(ctx, ch)

	var windows [][]int
	for w := range out {
		win := make([]int, len(w))
		copy(win, w)
		windows = append(windows, win)
	}
	if len(windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(windows))
	}
}
