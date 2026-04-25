package pipeline_test

import (
	"context"
	"fmt"

	"github.com/streamrift/streamrift/pipeline"
)

func ExampleSplitStage() {
	ctx := context.Background()

	in := make(chan string, 2)
	in <- "hello world"
	in <- "foo bar"
	close(in)

	// Split each sentence into individual words.
	words := pipeline.SplitStage(ctx, in, func(s string) []string {
		result := []string{}
		word := ""
		for _, c := range s {
			if c == ' ' {
				if word != "" {
					result = append(result, word)
					word = ""
				}
			} else {
				word += string(c)
			}
		}
		if word != "" {
			result = append(result, word)
		}
		return result
	})

	for w := range words {
		fmt.Println(w)
	}
	// Output:
	// hello
	// world
	// foo
	// bar
}

func ExampleZipStage() {
	ctx := context.Background()

	ids := make(chan int, 2)
	names := make(chan string, 2)
	ids <- 1
	ids <- 2
	close(ids)
	names <- "Alice"
	names <- "Bob"
	close(names)

	pairs := pipeline.ZipStage(ctx, ids, names)
	for p := range pairs {
		fmt.Printf("%d: %s\n", p.Left, p.Right)
	}
	// Output:
	// 1: Alice
	// 2: Bob
}
