# Monitoring

**gopipe** records pipeline execution metrics with OpenTelemetry Metrics.
`gopipe.Config.MeterProvider` defaults to `otel.GetMeterProvider()`, so you can
configure the global provider once or pass a provider per pipeline.

## Metrics

| OTEL instrument           | Prometheus metric name          | Type    | Attributes                   | Description                                |
|---------------------------|---------------------------------|---------|------------------------------|--------------------------------------------|
| `gopipe.pipeline.started` | `gopipe_pipeline_started_total` | Counter | `pipeline_name`              | Total number of started pipeline runs.     |
| `gopipe.step.started`     | `gopipe_step_started_total`     | Counter | `pipeline_name`, `step_name` | Total number of started pipeline steps.    |
| `gopipe.step.succeed`     | `gopipe_step_succeed_total`     | Counter | `pipeline_name`, `step_name` | Total number of successful pipeline steps. |
| `gopipe.step.failed`      | `gopipe_step_failed_total`      | Counter | `pipeline_name`, `step_name` | Total number of failed pipeline steps.     |

## Exporting to Prometheus

Install the Prometheus exporter dependencies in your application:

```shell
go get go.opentelemetry.io/otel/exporters/prometheus \
	go.opentelemetry.io/otel/sdk/metric \
	github.com/prometheus/client_golang/prometheus/promhttp
```

Configure an OTEL meter provider and expose `/metrics`:

```go
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/artarts36/gopipe"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type payload struct{}

func main() {
	exporter, err := prometheus.New()
	if err != nil {
		log.Fatal(err)
	}

	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			log.Printf("shutdown meter provider: %v", err)
		}
	}()

	otel.SetMeterProvider(meterProvider)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("serve metrics: %v", err)
		}
	}()

	pipeline := gopipe.NewPipelineWithConfig[*payload](gopipe.Config{
		PipelineName:  "deploy",
		MeterProvider: meterProvider,
	})

	pipeline.Add(gopipe.Step[*payload]{
		Name: "example",
		Run: func(ctx context.Context, pl *payload) error {
			return nil
		},
	})

	if err := pipeline.Run(context.Background(), &payload{}); err != nil {
		log.Fatal(err)
	}
}
```

## Semantics

- `pipeline_name` is taken from `gopipe.Config.PipelineName`. If it is empty,
  metrics are emitted with an empty `pipeline_name` attribute value.
- `step_name` is taken from `gopipe.Step.Name`. If it is empty, metrics are
  emitted with an empty `step_name` attribute value.
- `gopipe.pipeline.started` is incremented once at the beginning of every
  pipeline run.
- `gopipe.step.started` is incremented once after a step passes its `When`
  condition and before the step's `Run` function is called.
- Steps skipped by `When` are not counted as started, succeeded, or failed.
- Retries do not increment `gopipe.step.started` per attempt. A retried step is
  counted as one started step, then either one succeeded step or one failed step
  after the final outcome.
- A step that panics is recorded as failed.
- A step with `ContinueOnError` enabled is recorded as failed when its final
  attempt returns an error, and the pipeline continues.
