package gopipe

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type Step[pt any] struct {
	Name            string
	Attributes      Attributes[pt]
	When            func(payload pt, run Run) bool
	Run             func(ctx context.Context, payload pt) error
	ContinueOnError bool

	Retries    uint
	RetryDelay time.Duration
}

type Run struct {
	result *pipelineRunResult
}

type Attributes[pt any] struct {
	Trace func(pt) []attribute.KeyValue
}

func When[pt any](when func(payload pt) bool) func(payload pt, run Run) bool {
	return func(payload pt, _ Run) bool {
		return when(payload)
	}
}

func (p *Run) StepSucceed(stepName string) bool {
	_, ok := p.result.succeed[stepName]
	return ok
}

func (p *Run) StepFailed(stepName string) bool {
	_, ok := p.result.failed[stepName]
	return ok
}
