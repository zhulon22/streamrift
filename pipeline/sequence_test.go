package pipeline_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

func strSource(vals ...string) <-chan string {
	ch := make(chan string, len(vals))
	for _, v := range vals {
		ch <- v
	}
	close(ch)
	return ch
}

func TestIndexStage_AttachesIndex(t *testing.T) {
	ctx := context.Background()
	in := strSource("a", "b", "c")
	out := pipeline.IndexStage(ctx, in)

	var results []pipeline.Indexed[string]
	for item := range out {
		results = append(results, item)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 items, got %d", len(results))
	}
	for i, r := range results {
		if r.Index != i {
			t.Errorf("item %d: expected index %d, got %d", i, i, r.Index)
		}
	}
	if results[0].Value != "a" || results[1].Value != "b" || results[2].Value != "c" {
		t.Error("unexpected values in indexed output")
	}
}

func TestIndexStage_EmptyInput(t *testing.T) {
	ctx := context.Background()
	in := make(chan string)
	close(in)
	out := pipeline.IndexStage(ctx, in)
	var count int
	for range out {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 items, got %d", count)
	}
}

func TestIndexStage_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan string)
	out := pipeline.IndexStage(ctx, in)
	cancel()
	for range out {
	}
}

func TestEnumerateStage_TransformsWithIndex(t *testing.T) {
	ctx := context.Background()
	in := strSource("x", "y", "z")
	indexed := pipeline.IndexStage(ctx, in)
	out := pipeline.EnumerateStage(ctx, indexed, func(i int, v string) string {
		return fmt.Sprintf("%d:%s", i, v)
	})

	expected := []string{"0:x", "1:y", "2:z"}
	var got []string
	for v := range out {
		got = append(got, v)
	}
	if len(got) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(got))
	}
	for i, e := range expected {
		if got[i] != e {
			t.Errorf("index %d: expected %q, got %q", i, e, got[i])
		}
	}
}
