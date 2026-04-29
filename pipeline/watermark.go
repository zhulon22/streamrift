package pipeline

import (
	"context"
	"time"
)

// Watermark attaches an event-time timestamp to each item.
type Watermark[T any] struct {
	Item      T
	EventTime time.Time
}

// WatermarkStage stamps each item with the current time and emits it downstream.
// It also periodically emits a sentinel zero-value item with a watermark
// timestamp so downstream stages can reason about event-time progress.
//
// The stage closes its output channel when the input is exhausted or ctx is
// cancelled.
func WatermarkStage[T any](ctx context.Context, in <-chan T, interval time.Duration) <-chan Watermark[T] {
	out := make(chan Watermark[T])
	go func() {
		defer close(out)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- Watermark[T]{Item: item, EventTime: time.Now()}:
				case <-ctx.Done():
					return
				}
			case t := <-ticker.C:
				// emit a watermark tick with zero-value item
				var zero T
				select {
				case out <- Watermark[T]{Item: zero, EventTime: t}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// LateFilterStage drops items whose EventTime is older than the given
// allowedLateness relative to the latest watermark seen so far.
func LateFilterStage[T any](ctx context.Context, in <-chan Watermark[T], allowedLateness time.Duration) <-chan Watermark[T] {
	out := make(chan Watermark[T])
	go func() {
		defer close(out)
		var highWatermark time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case wm, ok := <-in:
				if !ok {
					return
				}
				if wm.EventTime.After(highWatermark) {
					highWatermark = wm.EventTime
				}
				cutoff := highWatermark.Add(-allowedLateness)
				if wm.EventTime.Before(cutoff) {
					continue // drop late item
				}
				select {
				case out <- wm:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
