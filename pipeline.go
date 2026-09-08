package gopipe

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Pipeline[pt any] struct {
	pipeline []Step[pt]
	cfg      Config
	tracer   trace.Tracer
}

type Config struct {
	PipelineName string

	Logger  *slog.Logger
	Metrics Metrics

	TraceDisabled  bool
	TracerProvider trace.TracerProvider
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

	if cfg.Metrics == nil {
		cfg.Metrics = defaultMetrics
	}

	if cfg.TracerProvider == nil {
		if cfg.TraceDisabled {
			cfg.TracerProvider = noop.NewTracerProvider()
		} else {
			cfg.TracerProvider = otel.GetTracerProvider()
		}
	}

	return &Pipeline[pt]{
		pipeline: make([]Step[pt], 0),
		cfg:      cfg,
		tracer:   newTracer(cfg.TracerProvider),
	}
}

func (p *Pipeline[pt]) Add(step Step[pt]) {
	p.pipeline = append(p.pipeline, step)
}

func (p *Pipeline[pt]) Run(ctx context.Context, payload pt) error {
	run := newPipelineRun[pt](p.cfg.Logger, p.cfg.PipelineName, p.pipeline, p.cfg.Metrics, p.tracer)

	return run.run(ctx, payload)
}
