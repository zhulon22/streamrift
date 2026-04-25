package pipeline_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleAggregateStage() {
	ctx := context.Background()

	words := make(chan string, 3)
	words <- "hello"
	words <- "world"
	words <- "go"
	close(words)

	out := pipeline.AggregateStage(ctx, words, []string{}, func(acc []string, w string) []string {
		return append(acc, strings.ToUpper(w))
	})

	result := <-out
	fmt.Println(strings.Join(result, ", "))
	// Output: HELLO, WORLD, GO
}

func ExampleCountStage() {
	ctx := context.Background()

	ch := make(chan int, 4)
	ch <- 10
	ch <- 20
	ch <- 30
	ch <- 40
	close(ch)

	count := <-pipeline.CountStage(ctx, ch)
	fmt.Println(count)
	// Output: 4
}

func ExampleSumStage() {
	ctx := context.Background()

	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	total := <-pipeline.SumStage(ctx, ch)
	fmt.Println(total)
	// Output: 15
}
