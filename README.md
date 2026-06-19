# gopipe

```shell
go get github.com/artarts36/gopipe
```

**gopipe** is lightweight library for building simple linear pipeline for Go

Features
- Continue on step error
- Retry failed step with delay
- Run step by conditions
- When step panicked pipeline aborted and return error
- Collect metrics

## Usage examples

### Simple without conditions

```go
package main

import (
	"context"
	"fmt"

	"github.com/artarts36/gopipe"
	"github.com/artarts36/gopipe/pkg/prometheus"
)

func main() {
	prometheus.MustRegister()

	type payload struct {
		firstName string
		lastName  string
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

### Run step by condition

```go
package main

import (
	"context"
	"fmt"
	
	"github.com/artarts36/gopipe"
	"github.com/artarts36/gopipe/pkg/prometheus"
)

func main() {
	prometheus.MustRegister()

	type payload struct {
		firstName string
		lastName  string
	}

	pipeline := gopipe.NewPipeline[*payload]()

	pipeline.Add(gopipe.Step[*payload]{
		Name: "first",
		Run: func(ctx context.Context, pl *payload) error {
			pl.firstName = "John"
			return nil
		},
	})

	pipeline.Add(gopipe.Step[*payload]{
		When: func(pl *payload, run gopipe.Run) bool {
			return pl.firstName == "John" || run.StepSucceed("first")
		},
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

### Continue when step failed with error

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/artarts36/gopipe"
	"github.com/artarts36/gopipe/pkg/prometheus"
)

func main() {
	prometheus.MustRegister()

	type payload struct {
		firstName string
		lastName  string
	}

	pipeline := gopipe.NewPipeline[*payload]()

	pipeline.Add(gopipe.Step[*payload]{
		ContinueOnError: true,
		Run: func(ctx context.Context, pl *payload) error {
			return errors.New("name not found")
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

### Retry failed step

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/artarts36/gopipe"
	"github.com/artarts36/gopipe/pkg/prometheus"
)

func main() {
	prometheus.MustRegister()

	type payload struct {
		attempts int
	}

	pipeline := gopipe.NewPipeline[*payload]()

	pipeline.Add(gopipe.Step[*payload]{
		Retries:    2,
		RetryDelay: time.Second,
		Run: func(ctx context.Context, pl *payload) error {
			pl.attempts++
			if pl.attempts < 3 {
				return errors.New("temporary error")
			}

			return nil
		},
	})

	pl := &payload{}

	_ = pipeline.Run(context.Background(), pl)
	fmt.Println(pl.attempts)
}
```
