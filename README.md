# streamrift

A lightweight Go library for building backpressure-aware data stream pipelines with pluggable connectors.

---

## Installation

```bash
go get github.com/yourusername/streamrift
```

## Usage

```go
package main

import (
    "fmt"
    "github.com/yourusername/streamrift"
)

func main() {
    pipeline := streamrift.NewPipeline(
        streamrift.WithBufferSize(64),
        streamrift.WithBackpressure(true),
    )

    source := streamrift.NewSource(func(emit streamrift.EmitFunc) {
        for i := 0; i < 100; i++ {
            emit(i)
        }
    })

    sink := streamrift.NewSink(func(val any) {
        fmt.Println("received:", val)
    })

    pipeline.Connect(source).
        Through(streamrift.Map(func(v any) any {
            return v.(int) * 2
        })).
        To(sink)

    if err := pipeline.Run(); err != nil {
        panic(err)
    }
}
```

## Features

- Backpressure-aware stream processing
- Composable pipeline stages (`Map`, `Filter`, `Reduce`)
- Pluggable source and sink connectors
- Lightweight with no external dependencies

## License

MIT © 2024 streamrift contributors