package pipeline_test

import (
	"context"
	"testing"

	"github.com/streamrift/streamrift/pipeline"
)

// TestBatchStage_ExactMultiple verifies that items are grouped into full batches
// when the total count is an exact multiple of the batch size.
func TestBatchStage_ExactMultiple(t *testing.T) {
	ctx := context.Background()
	src := sourceChan(ctx, 1, 2, 3, 4, 5, 6)

	batched := pipeline.BatchStage[int](3)(ctx, src)

	var results [][]int
	for batch := range batched {
		results = append(results, batch)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(results))
	}
	expectBatch(t, results[0], 1, 2, 3)
	expectBatch(t, results[1], 4, 5, 6)
}

// TestBatchStage_Remainder verifies that a partial batch is emitted at the end
// when the total count is not an exact multiple of the batch size.
func TestBatchStage_Remainder(t *testing.T) {
	ctx := context.Background()
	src := sourceChan(ctx, 1, 2, 3, 4, 5)

	batched := pipeline.BatchStage[int](3)(ctx, src)

	var results [][]int
	for batch := range batched {
		results = append(results, batch)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(results))
	}
	expectBatch(t, results[0], 1, 2, 3)
	expectBatch(t, results[1], 4, 5)
}

// TestBatchStage_Empty verifies that no batches are emitted from an empty source.
func TestBatchStage_Empty(t *testing.T) {
	ctx := context.Background()
	src := sourceChan[int](ctx)

	batched := pipeline.BatchStage[int](3)(ctx, src)

	var results [][]int
	for batch := range batched {
		results = append(results, batch)
	}

	if len(results) != 0 {
		t.Fatalf("expected 0 batches, got %d", len(results))
	}
}

// TestBatchStage_SingleItem verifies that a single item forms a batch of size one.
func TestBatchStage_SingleItem(t *testing.T) {
	ctx := context.Background()
	src := sourceChan(ctx, 42)

	batched := pipeline.BatchStage[int](5)(ctx, src)

	var results [][]int
	for batch := range batched {
		results = append(results, batch)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(results))
	}
	expectBatch(t, results[0], 42)
}

// TestFlattenStage verifies that slices are flattened back into individual items.
func TestFlattenStage(t *testing.T) {
	ctx := context.Background()

	sliceSrc := func(ctx context.Context, slices ...[]int) <-chan []int {
		out := make(chan []int)
		go func() {
			defer close(out)
			for _, s := range slices {
				select {
				case <-ctx.Done():
					return
				case out <- s:
				}
			}
		}()
		return out
	}

	src := sliceSrc(ctx, []int{1, 2}, []int{3}, []int{4, 5, 6})
	flat := pipeline.FlattenStage[int]()(ctx, src)

	var results []int
	for v := range flat {
		results = append(results, v)
	}

	expected := []int{1, 2, 3, 4, 5, 6}
	if len(results) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, results)
	}
	for i, v := range expected {
		if results[i] != v {
			t.Errorf("index %d: expected %d, got %d", i, v, results[i])
		}
	}
}

// expectBatch is a helper that asserts a batch contains exactly the given values.
func expectBatch(t *testing.T, batch []int, expected ...int) {
	t.Helper()
	if len(batch) != len(expected) {
		t.Fatalf("batch length mismatch: expected %v, got %v", expected, batch)
	}
	for i, v := range expected {
		if batch[i] != v {
			t.Errorf("batch[%d]: expected %d, got %d", i, v, batch[i])
		}
	}
}
