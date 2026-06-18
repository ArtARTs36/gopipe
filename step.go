package gopipe

import "context"

type Step[pt any] struct {
	Name            string
	When            func(payload pt, run Run) bool
	Run             func(ctx context.Context, payload pt) error
	ContinueOnError bool
}

type Run struct {
	result *pipelineRunResult
}

func (p *Run) StepSucceed(stepName string) bool {
	_, ok := p.result.succeed[stepName]
	return ok
}

func (p *Run) StepFailed(stepName string) bool {
	_, ok := p.result.failed[stepName]
	return ok
}
