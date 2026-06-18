package gopipe

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type pipelineRun[pt any] struct {
	log   *slog.Logger
	steps []Step[pt]

	result pipelineRunResult
}

type pipelineRunResult struct {
	succeed map[string]struct{}
	failed  map[string]struct{}
}

func newPipelineRun[pt any](log *slog.Logger, steps []Step[pt]) *pipelineRun[pt] {
	return &pipelineRun[pt]{
		log:   log,
		steps: steps,
		result: pipelineRunResult{
			succeed: make(map[string]struct{}),
			failed:  make(map[string]struct{}),
		},
	}
}

func (p *pipelineRun[pt]) run(ctx context.Context, payload pt) error {
	log := p.log.With(slog.String("pipeline.run_id", uuid.Must(uuid.NewV7()).String()))

	log.DebugContext(ctx, "[gopipe] running pipeline")

	for i, step := range p.steps {
		if err := ctx.Err(); err != nil {
			if i > 0 {
				err = fmt.Errorf("context canceled after step %q: %w", p.steps[i-1].Name, err)
			}

			return &StepError{
				StepName: step.Name,
				Err:      err,
			}
		}

		err := p.runStep(ctx, log, step, payload)
		if err != nil {
			return &StepError{
				StepName: step.Name,
				Err:      err,
			}
		}
	}

	return nil
}

func (p *pipelineRun[pt]) runStep(
	ctx context.Context,
	log *slog.Logger,
	step Step[pt],
	payload pt,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("step panicked: %v", r)

			log.ErrorContext(ctx, "[gopipe] step panicked", slog.Any("err", err))
		}
	}()

	log.With(slog.String("pipeline.step_name", step.Name))

	if step.When != nil {
		if !step.When(payload, Run{
			result: &p.result,
		}) {
			log.DebugContext(ctx, "[gopipe] skip step")

			return nil
		}
	}

	log.DebugContext(ctx, "[gopipe] running step")

	attempts := step.Retries + 1

	for attempt := uint(1); attempt <= attempts; attempt++ {
		err = step.Run(ctx, payload)
		if err == nil {
			p.result.succeed[step.Name] = struct{}{}
			return nil
		}

		if attempt == attempts {
			p.result.failed[step.Name] = struct{}{}

			if step.ContinueOnError {
				log.WarnContext(ctx, "[gopipe] step failed but continue", slog.Any("err", err))
				return nil
			}

			log.ErrorContext(ctx, "[gopipe] step failed", slog.Any("err", err))
			return err
		}

		log.WarnContext(ctx, "[gopipe] step failed, retrying",
			slog.Uint64("attempt", uint64(attempt)),
			slog.Uint64("max_attempts", uint64(attempts)),
			slog.Duration("retry_delay", step.RetryDelay),
			slog.Any("err", err),
		)

		if step.RetryDelay <= 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(step.RetryDelay):
		}
	}

	return nil
}
