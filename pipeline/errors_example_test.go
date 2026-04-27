package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/yourusername/streamrift/pipeline"
)

func ExampleErrorStage() {
	ctx := context.Background()

	in := make(chan string, 5)
	in <- "42"
	in <- "bad"
	in <- "7"
	in <- "nope"
	in <- "100"
	close(in)

	out, dead := pipeline.ErrorStage(ctx, in, func(s string) (int, error) {
		n, err := strconv.Atoi(s)
		if err != nil {
			return 0, errors.New("not a number: " + s)
		}
		return n, nil
	}, "parse-int")

	var good []int
	for v := range out {
		good = append(good, v)
	}
	var bad []string
	for rec := range dead {
		bad = append(bad, rec.Item)
	}

	fmt.Println("parsed:", good)
	fmt.Println("rejected:", bad)
	// Output:
	// parsed: [42 7 100]
	// rejected: [bad nope]
}
