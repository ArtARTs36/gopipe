# gopipe

```shell
go get github.com/artarts36/gopipe
```

**gopipe** is lightweight library for building simple linear pipeline for Go

## Usage examples

### Simple without conditions

```go
package main

import (
	"context"
	"fmt"

	"github.com/artarts36/gopipe"
)

func main() {
	type payload struct {
		firstName  string
		lastName string
	}

	pipeline := gopipe.NewPipeline[*payload]()

	pipeline.Add(gopipe.Step[*payload]{
		Run: func(ctx context.Context, pl *payload) error {
			pl.firstName = "John"
			return nil
		},
	})

	pipeline.Add(gopipe.Step[*payload]{
		Run: func(ctx context.Context, pl *payload) error {
			pl.lastName = "Doe"
			return nil
		},
	})

	pl := &payload{}

	_ = pipeline.Run(context.Background(), pl)
	fmt.Println(pl.firstName, pl.lastName)
}
```
