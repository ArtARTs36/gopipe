package gopipe

import (
	"context"
	"log/slog"
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
		cfg.Logger = cfg.Logger.With(slog.String("pipeline.name", cfg.PipelineName))
	}

	return &Pipeline[pt]{
		pipeline: make([]Step[pt], 0),
		cfg:      cfg,
	}
}

func (p *Pipeline[pt]) Add(step Step[pt]) {
	p.pipeline = append(p.pipeline, step)
}

func (p *Pipeline[pt]) Run(ctx context.Context, payload pt) error {
	run := newPipelineRun[pt](p.cfg.Logger, p.pipeline)

	return run.run(ctx, payload)
}
