package pipeline_test

import (
	"context"
	"fmt"

	"github.com/your-org/streamrift/pipeline"
)

func ExampleJoinStage() {
	ctx := context.Background()

	type Order struct {
		ID    int
		Total float64
	}
	type Customer struct {
		ID   int
		Name string
	}

	orders := make(chan Order, 3)
	orders <- Order{1, 9.99}
	orders <- Order{2, 19.99}
	orders <- Order{3, 4.99}
	close(orders)

	customers := make(chan Customer, 2)
	customers <- Customer{2, "Alice"}
	customers <- Customer{1, "Bob"}
	close(customers)

	out := pipeline.JoinStage(ctx, orders, customers,
		func(o Order) int { return o.ID },
		func(c Customer) int { return c.ID },
	)

	type result struct {
		name  string
		total float64
	}
	var results []result
	for j := range out {
		results = append(results, result{j.Right.Name, j.Left.Total})
	}
	// Sort for deterministic output
	if len(results) == 2 && results[0].name > results[1].name {
		results[0], results[1] = results[1], results[0]
	}
	for _, r := range results {
		fmt.Printf("%s: $%.2f\n", r.name, r.total)
	}
	// Output:
	// Alice: $19.99
	// Bob: $9.99
}

func ExampleConcatStage() {
	ctx := context.Background()

	a := make(chan string, 2)
	a <- "first"
	a <- "second"
	close(a)

	b := make(chan string, 1)
	b <- "third"
	close(b)

	out := pipeline.ConcatStage(ctx, a, b)
	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// first
	// second
	// third
}
