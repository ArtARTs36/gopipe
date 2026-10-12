package gopipe

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPipelineMetrics(t *testing.T) {
	tests := []struct {
		name        string
		steps       []Step[*struct{}]
		expectedErr string
		expected    map[string]int64
	}{
		{
			name: "success skip and fail with continue",
			steps: []Step[*struct{}]{
				{
					Name: "first",
					Run: func(ctx context.Context, payload *struct{}) error {
						return nil
					},
				},
				{
					Name: "skipped",
					When: func(payload *struct{}, run Run) bool {
						return false
					},
					Run: func(ctx context.Context, payload *struct{}) error {
						return nil
					},
				},
				{
					Name:            "failed",
					ContinueOnError: true,
					Run: func(ctx context.Context, payload *struct{}) error {
						return errors.New("boom")
					},
				},
			},
			expected: map[string]int64{
				"gopipe.pipeline.started|pipeline_name=deploy":              1,
				"gopipe.step.started|pipeline_name=deploy|step_name=first":  1,
				"gopipe.step.started|pipeline_name=deploy|step_name=failed": 1,
				"gopipe.step.succeed|pipeline_name=deploy|step_name=first":  1,
				"gopipe.step.failed|pipeline_name=deploy|step_name=failed":  1,
			},
		},
		{
			name: "panic marks failed",
			steps: []Step[*struct{}]{
				{
					Name: "panic",
					Run: func(ctx context.Context, payload *struct{}) error {
						panic("boom")
					},
				},
			},
			expectedErr: "panic: step panicked: boom",
			expected: map[string]int64{
				"gopipe.pipeline.started|pipeline_name=deploy":             1,
				"gopipe.step.started|pipeline_name=deploy|step_name=panic": 1,
				"gopipe.step.failed|pipeline_name=deploy|step_name=panic":  1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() {
				require.NoError(t, provider.Shutdown(context.Background()))
			}()

			pipeline := NewPipelineWithConfig[*struct{}](Config{
				PipelineName:  "deploy",
				MeterProvider: provider,
			})

			for _, step := range tt.steps {
				pipeline.Add(step)
			}

			err := pipeline.Run(context.Background(), &struct{}{})
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.expectedErr)
			}

			assert.Equal(t, tt.expected, collectInt64MetricSums(t, reader))
		})
	}
}

func collectInt64MetricSums(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &metrics))

	values := map[string]int64{}

	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, m := range scopeMetrics.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}

			for _, point := range sum.DataPoints {
				values[metricKey(m.Name, point.Attributes)] += point.Value
			}
		}
	}

	return values
}

func metricKey(name string, attrs attribute.Set) string {
	values := attrs.ToSlice()
	sort.Slice(values, func(i, j int) bool {
		return values[i].Key < values[j].Key
	})

	parts := []string{name}
	for _, attr := range values {
		parts = append(parts, fmt.Sprintf("%s=%s", attr.Key, attr.Value.AsString()))
	}

	return strings.Join(parts, "|")
}
