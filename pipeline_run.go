package gopipe

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

type PipelineRun[pt any] struct {
	log   *slog.Logger
	steps []Step[pt]
}

func (p *PipelineRun[pt]) run(ctx context.Context, payload pt) error {
	log := p.log.With(slog.String("pipeline.run_id", uuid.Must(uuid.NewV7()).String()))

	log.DebugContext(ctx, "[gopipe] running pipeline")

	for _, step := range p.steps {
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

func (p *PipelineRun[pt]) runStep(
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

	if !step.When(payload) {
		log.DebugContext(ctx, "[gopipe] skip step")

		return nil
	}

	log.DebugContext(ctx, "[gopipe] running step")

	err = step.Run(ctx, payload)
	if err != nil {
		if step.ContinueOnError {
			log.WarnContext(ctx, "[gopipe] step failed but continue", slog.Any("err", err))
			return nil
		}

		log.ErrorContext(ctx, "[gopipe] step failed", slog.Any("err", err))

		return err
	}

	return nil
}
