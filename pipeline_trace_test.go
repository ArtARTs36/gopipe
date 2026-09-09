package gopipe

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestPipelineTraceStatuses(t *testing.T) {
	t.Run("successful pipeline and step", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		pipeline := NewPipelineWithConfig[struct{}](Config{
			PipelineName:   "test-pipeline",
			TracerProvider: provider,
		})
		pipeline.Add(Step[struct{}]{
			Name: "successful-step",
			Run: func(context.Context, struct{}) error {
				return nil
			},
		})

		require.NoError(t, pipeline.Run(context.Background(), struct{}{}))

		spans := exporter.GetSpans()
		require.Len(t, spans, 2)
		assert.Equal(t, codes.Ok, spanByName(t, spans, "successful-step").Status.Code)
		assert.Equal(t, codes.Ok, spanByName(t, spans, "test-pipeline").Status.Code)
	})

	t.Run("failed pipeline and step", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		pipeline := NewPipelineWithConfig[struct{}](Config{
			PipelineName:   "test-pipeline",
			TracerProvider: provider,
		})
		pipeline.Add(Step[struct{}]{
			Name: "failed-step",
			Run: func(context.Context, struct{}) error {
				return errors.New("test error")
			},
		})

		require.EqualError(t, pipeline.Run(context.Background(), struct{}{}), "failed-step: test error")

		spans := exporter.GetSpans()
		require.Len(t, spans, 2)
		assert.Equal(t, codes.Error, spanByName(t, spans, "failed-step").Status.Code)
		assert.Equal(t, codes.Error, spanByName(t, spans, "test-pipeline").Status.Code)
	})

	t.Run("skipped step remains unset", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		pipeline := NewPipelineWithConfig[struct{}](Config{
			PipelineName:   "test-pipeline",
			TracerProvider: provider,
		})
		pipeline.Add(Step[struct{}]{
			Name: "skipped-step",
			When: func(struct{}, Run) bool {
				return false
			},
			Run: func(context.Context, struct{}) error {
				return nil
			},
		})

		require.NoError(t, pipeline.Run(context.Background(), struct{}{}))

		spans := exporter.GetSpans()
		require.Len(t, spans, 2)
		assert.Equal(t, codes.Unset, spanByName(t, spans, "skipped-step").Status.Code)
		assert.Equal(t, codes.Ok, spanByName(t, spans, "test-pipeline").Status.Code)
	})

	t.Run("continued failure marks step error and pipeline ok", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		pipeline := NewPipelineWithConfig[struct{}](Config{
			PipelineName:   "test-pipeline",
			TracerProvider: provider,
		})
		pipeline.Add(Step[struct{}]{
			Name:            "continued-step",
			ContinueOnError: true,
			Run: func(context.Context, struct{}) error {
				return errors.New("test error")
			},
		})

		require.NoError(t, pipeline.Run(context.Background(), struct{}{}))

		spans := exporter.GetSpans()
		require.Len(t, spans, 2)
		assert.Equal(t, codes.Error, spanByName(t, spans, "continued-step").Status.Code)
		assert.Equal(t, codes.Ok, spanByName(t, spans, "test-pipeline").Status.Code)
	})
}

func spanByName(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()

	for _, span := range spans {
		if span.Name == name {
			return span
		}
	}

	require.FailNow(t, "span not found", name)

	return tracetest.SpanStub{}
}
