# Monitoring

**gopipe** exposes pipeline execution metrics through the `Metrics` interface.
The core package uses a no-op implementation by default, so metrics are only
exported after an implementation is registered or passed in the pipeline
configuration.

## Prometheus metrics

The `github.com/artarts36/gopipe/pkg/prometheus` package provides a Prometheus
implementation. All metric names use the `gopipe` namespace.

| Metric                          | Type    | Labels                       | Description                                |
|---------------------------------|---------|------------------------------|--------------------------------------------|
| `gopipe_pipeline_started_total` | Counter | `pipeline_name`              | Total number of started pipeline runs.     |
| `gopipe_step_started_total`     | Counter | `pipeline_name`, `step_name` | Total number of started pipeline steps.    |
| `gopipe_step_succeed_total`     | Counter | `pipeline_name`, `step_name` | Total number of successful pipeline steps. |
| `gopipe_step_failed_total`      | Counter | `pipeline_name`, `step_name` | Total number of failed pipeline steps.     |

## Registration

Register the Prometheus collector before creating or running pipelines:

```go
package main

import (
	"github.com/artarts36/gopipe"
	"github.com/artarts36/gopipe/pkg/prometheus"
)

func main() {
	prometheus.MustRegister()

	pipeline := gopipe.NewPipelineWithConfig[*payload](gopipe.Config{
		PipelineName: "deploy",
	})

	// Add steps and run the pipeline.
}
```

`prometheus.MustRegister()` registers metrics in the default Prometheus
registry and sets them as the default gopipe metrics implementation.
Use `prometheus.Register()` if registration errors should be handled manually,
or `prometheus.RegisterIn(registry)` to register metrics in a custom
Prometheus registry.

## Semantics

- `pipeline_name` is taken from `gopipe.Config.PipelineName`. If it is empty,
  metrics are emitted with an empty `pipeline_name` label value.
- `step_name` is taken from `gopipe.Step.Name`. If it is empty, metrics are
  emitted with an empty `step_name` label value.
- `gopipe_pipeline_started_total` is incremented once at the beginning of every
  pipeline run.
- `gopipe_step_started_total` is incremented once after a step passes its
  `When` condition and before the step's `Run` function is called.
- Steps skipped by `When` are not counted as started, succeeded, or failed.
- Retries do not increment `gopipe_step_started_total` per attempt. A retried
  step is counted as one started step, then either one succeeded step or one
  failed step after the final outcome.
- A step that panics is recorded as failed.
- A step with `ContinueOnError` enabled is recorded as failed when its final
  attempt returns an error, and the pipeline continues.
