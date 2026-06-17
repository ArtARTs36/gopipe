package gopipe

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

type Pipeline[pt any] struct {
	pipeline []Step[pt]
	cfg      Config
}

type Config struct {
	PipelineName string

	Logger *slog.Logger
}

func NewPipeline[pt any]() *Pipeline[pt] {
	return NewPipelineWithConfig[pt](Config{})
}

func NewPipelineWithConfig[pt any](cfg Config) *Pipeline[pt] {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if cfg.PipelineName != "" {
		cfg.Logger.With(slog.String("pipeline.name", cfg.PipelineName))
	}

	return &Pipeline[pt]{
		pipeline: make([]Step[pt], 0),
		cfg:      cfg,
	}
}

func (p *Pipeline[pt]) Add(step Step[pt]) {
	if step.When == nil {
		step.When = always[pt]()
	}

	p.pipeline = append(p.pipeline, step)
}

func (p *Pipeline[pt]) Run(ctx context.Context, payload pt) error {
	log := p.cfg.Logger.With(slog.String("pipeline.run_id", uuid.Must(uuid.NewV7()).String()))

	log.DebugContext(ctx, "[gopipe] running pipeline")

	for _, step := range p.pipeline {
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

func (p *Pipeline[pt]) runStep(
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
		log.ErrorContext(ctx, "[gopipe] step failed", slog.Any("err", err))

		return err
	}

	return nil
}
