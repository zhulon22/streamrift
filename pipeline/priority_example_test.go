package pipeline_test

import (
	"context"
	"fmt"

	"github.com/your-org/streamrift/pipeline"
)

func ExamplePriorityStage() {
	items := []pipeline.PriorityItem[string]{
		{Value: "low", Priority: 10},
		{Value: "critical", Priority: 1},
		{Value: "medium", Priority: 5},
	}
	ch := make(chan pipeline.PriorityItem[string], len(items))
	for _, it := range items {
		ch <- it
	}
	close(ch)

	out := pipeline.PriorityStage(context.Background(), ch, 10)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// critical
	// medium
	// low
}
