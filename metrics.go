package gopipe

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	instrumentationName    = "github.com/artarts36/gopipe"
	instrumentationVersion = "v0.2.0"
)

type pipelineMetrics struct {
	pipelineStarted metric.Int64Counter
	stepStarted     metric.Int64Counter
	stepSucceed     metric.Int64Counter
	stepFailed      metric.Int64Counter
}

func newMetrics(mp metric.MeterProvider) *pipelineMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	meter := mp.Meter(
		instrumentationName,
		metric.WithInstrumentationVersion(instrumentationVersion),
	)

	return &pipelineMetrics{
		pipelineStarted: newCounter(meter, "gopipe.pipeline.started", "Total number of started pipeline runs."),
		stepStarted:     newCounter(meter, "gopipe.step.started", "Total number of started pipeline steps."),
		stepSucceed:     newCounter(meter, "gopipe.step.succeed", "Total number of successful pipeline steps."),
		stepFailed:      newCounter(meter, "gopipe.step.failed", "Total number of failed pipeline steps."),
	}
}

func newCounter(meter metric.Meter, name, description string) metric.Int64Counter {
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		otel.Handle(err)

		return nil
	}

	return counter
}

func (m *pipelineMetrics) recordPipelineStarted(ctx context.Context, pipelineName string) {
	if m.pipelineStarted == nil {
		return
	}

	m.pipelineStarted.Add(ctx, 1, metric.WithAttributes(attribute.String("pipeline_name", pipelineName)))
}

func (m *pipelineMetrics) recordStepStarted(ctx context.Context, pipelineName, stepName string) {
	if m.stepStarted == nil {
		return
	}

	m.stepStarted.Add(ctx, 1, metric.WithAttributes(stepAttrs(pipelineName, stepName)...))
}

func (m *pipelineMetrics) recordStepSucceed(ctx context.Context, pipelineName, stepName string) {
	if m.stepSucceed == nil {
		return
	}

	m.stepSucceed.Add(ctx, 1, metric.WithAttributes(stepAttrs(pipelineName, stepName)...))
}

func (m *pipelineMetrics) recordStepFailed(ctx context.Context, pipelineName, stepName string) {
	if m.stepFailed == nil {
		return
	}

	m.stepFailed.Add(ctx, 1, metric.WithAttributes(stepAttrs(pipelineName, stepName)...))
}

func stepAttrs(pipelineName, stepName string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("pipeline_name", pipelineName),
		attribute.String("step_name", stepName),
	}
}
