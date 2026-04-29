package pipeline_test

import (
	"context"
	"testing"

	"github.com/your-org/streamrift/pipeline"
)

func BenchmarkPriorityStage(b *testing.B) {
	const n = 1000
	items := make([]pipeline.PriorityItem[int], n)
	for i := 0; i < n; i++ {
		items[i] = pipeline.PriorityItem[int]{Value: i, Priority: n - i}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := make(chan pipeline.PriorityItem[int], n)
		for _, it := range items {
			ch <- it
		}
		close(ch)
		out := pipeline.PriorityStage(context.Background(), ch, n)
		for range out {
		}
	}
}
