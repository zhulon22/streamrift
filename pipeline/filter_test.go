package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func TestFilterStage_KeepEvens(t *testing.T) {
	ctx := context.Background()
	src := sourceChan(1, 2, 3, 4, 5, 6)

	p := pipeline.New(src)
	filtered := pipeline.Connect(p, pipeline.FilterStage[int](func(n int) bool {
		return n%2 == 0
	}))

	var got []int
	for v := range filtered.Run(ctx) {
		got = append(got, v)
	}

	expected := []int{2, 4, 6}
	if len(got) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
	for i, v := range expected {
		if got[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, got[i])
		}
	}
}

func TestFilterStage_NonePass(t *testing.T) {
	ctx := context.Background()
	src := sourceChan(1, 3, 5)

	p := pipeline.New(src)
	filtered := pipeline.Connect(p, pipeline.FilterStage[int](func(n int) bool {
		return n%2 == 0
	}))

	var got []int
	for v := range filtered.Run(ctx) {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Errorf("expected no items, got %v", got)
	}
}

func TestReduceStage_Sum(t *testing.T) {
	ctx := context.Background()
	src := sourceChan(1, 2, 3, 4, 5)

	p := pipeline.New(src)
	reduced := pipeline.Connect(p, pipeline.ReduceStage[int, int](0, func(acc, v int) int {
		return acc + v
	}))

	var results []int
	for v := range reduced.Run(ctx) {
		results = append(results, v)
	}

	if len(results) != 1 {
		t.Fatalf("expected exactly 1 result, got %d", len(results))
	}
	if results[0] != 15 {
		t.Errorf("expected sum 15, got %d", results[0])
	}
}

func TestReduceStage_Concat(t *testing.T) {
	ctx := context.Background()
	src := sourceChan("a", "b", "c")

	p := pipeline.New(src)
	reduced := pipeline.Connect(p, pipeline.ReduceStage[string, string]("", func(acc, v string) string {
		return acc + v
	}))

	var results []string
	for v := range reduced.Run(ctx) {
		results = append(results, v)
	}

	if len(results) != 1 {
		t.Fatalf("expected exactly 1 result, got %d", len(results))
	}
	if results[0] != "abc" {
		t.Errorf("expected 'abc', got %q", results[0])
	}
}
