package gopipe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMetrics struct {
	pipelineStarted map[string]int
	stepStarted     map[string]int
	stepSucceed     map[string]int
	stepFailed      map[string]int
}

func newTestMetrics() *testMetrics {
	return &testMetrics{
		pipelineStarted: make(map[string]int),
		stepStarted:     make(map[string]int),
		stepSucceed:     make(map[string]int),
		stepFailed:      make(map[string]int),
	}
}

func (m *testMetrics) IncPipelineStarted(pipelineName string) {
	m.pipelineStarted[pipelineName]++
}

func (m *testMetrics) IncStepStarted(pipelineName, stepName string) {
	m.stepStarted[m.stepKey(pipelineName, stepName)]++
}

func (m *testMetrics) IncStepSucceed(pipelineName, stepName string) {
	m.stepSucceed[m.stepKey(pipelineName, stepName)]++
}

func (m *testMetrics) IncStepFailed(pipelineName, stepName string) {
	m.stepFailed[m.stepKey(pipelineName, stepName)]++
}

func (m *testMetrics) stepKey(pipelineName, stepName string) string {
	return pipelineName + "/" + stepName
}

func TestPipeline(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	t.Run("test successfully simple pipeline without conditions", func(t *testing.T) {
		type payload struct {
			firstStepCalled  bool
			secondStepCalled bool
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			Run: func(ctx context.Context, pl *payload) error {
				pl.firstStepCalled = true
				return nil
			},
		})

		pipeline.Add(Step[*payload]{
			Run: func(ctx context.Context, pl *payload) error {
				pl.secondStepCalled = true
				return nil
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.NoError(t, err)

		assert.True(t, pl.firstStepCalled, "first step must be called")
		assert.True(t, pl.secondStepCalled, "second step must be called")
	})

	t.Run("test successfully pipeline with skip first step", func(t *testing.T) {
		type payload struct {
			firstStepCalled  bool
			secondStepCalled bool
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			When: func(payload *payload, _ Run) bool {
				return false
			},
			Run: func(ctx context.Context, pl *payload) error {
				pl.firstStepCalled = true
				return nil
			},
		})

		pipeline.Add(Step[*payload]{
			Run: func(ctx context.Context, pl *payload) error {
				pl.secondStepCalled = true
				return nil
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.NoError(t, err)

		assert.False(t, pl.firstStepCalled, "first step must be not called")
		assert.True(t, pl.secondStepCalled, "second step must be called")
	})

	t.Run("test with recovery panicked step", func(t *testing.T) {
		type payload struct {
			firstStepCalled bool
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			Run: func(ctx context.Context, pl *payload) error {
				pl.firstStepCalled = true
				return nil
			},
		})

		pipeline.Add(Step[*payload]{
			Name: "second",
			Run: func(ctx context.Context, pl *payload) error {
				panic("test panic")
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.Equal(t, &StepError{
			StepName: "second",
			Err:      fmt.Errorf("step panicked: test panic"),
		}, err)

		assert.True(t, pl.firstStepCalled, "first step must be called")
	})

	t.Run("test with failed step and continue-on-error=true", func(t *testing.T) {
		type payload struct {
			firstStepCalled  bool
			secondStepCalled bool
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			ContinueOnError: true,
			Run: func(ctx context.Context, pl *payload) error {
				pl.firstStepCalled = true
				return errors.New("test error")
			},
		})

		pipeline.Add(Step[*payload]{
			Name: "second",
			Run: func(ctx context.Context, pl *payload) error {
				pl.secondStepCalled = true
				return nil
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.NoError(t, err)

		assert.True(t, pl.firstStepCalled, "first step must be called")
		assert.True(t, pl.secondStepCalled, "second step must be called")
	})

	t.Run("test step succeeds after retries", func(t *testing.T) {
		type payload struct {
			attempts int32
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			Name:       "retryable",
			Retries:    2,
			RetryDelay: time.Millisecond,
			Run: func(ctx context.Context, pl *payload) error {
				attempt := atomic.AddInt32(&pl.attempts, 1)
				if attempt < 3 {
					return errors.New("temporary error")
				}

				return nil
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.NoError(t, err)
		assert.Equal(t, int32(3), pl.attempts)
	})

	t.Run("test step fails after retries exhausted", func(t *testing.T) {
		type payload struct {
			attempts int32
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			Name:       "retryable",
			Retries:    2,
			RetryDelay: time.Millisecond,
			Run: func(ctx context.Context, pl *payload) error {
				atomic.AddInt32(&pl.attempts, 1)
				return errors.New("temporary error")
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.EqualError(t, err, "retryable: temporary error")
		assert.Equal(t, int32(3), pl.attempts)
	})

	t.Run("test with failed step after retries and continue-on-error=true", func(t *testing.T) {
		type payload struct {
			firstStepAttempts int32
			secondStepCalled  bool
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			Name:            "first",
			Retries:         2,
			RetryDelay:      time.Millisecond,
			ContinueOnError: true,
			Run: func(ctx context.Context, pl *payload) error {
				atomic.AddInt32(&pl.firstStepAttempts, 1)
				return errors.New("test error")
			},
		})

		pipeline.Add(Step[*payload]{
			Name: "second",
			Run: func(ctx context.Context, pl *payload) error {
				pl.secondStepCalled = true
				return nil
			},
		})

		pl := &payload{}

		err := pipeline.Run(context.Background(), pl)
		require.NoError(t, err)
		assert.Equal(t, int32(3), pl.firstStepAttempts)
		assert.True(t, pl.secondStepCalled, "second step must be called")
	})

	t.Run("test retry delay interrupted by context cancel", func(t *testing.T) {
		type payload struct {
			attempts int32
		}

		pipeline := NewPipeline[*payload]()

		pipeline.Add(Step[*payload]{
			Name:       "retryable",
			Retries:    2,
			RetryDelay: time.Second,
			Run: func(ctx context.Context, pl *payload) error {
				atomic.AddInt32(&pl.attempts, 1)
				return errors.New("temporary error")
			},
		})

		pl := &payload{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := pipeline.Run(ctx, pl)
		require.EqualError(t, err, "retryable: context canceled")
		assert.Zero(t, pl.attempts)
	})

	t.Run("test pipeline stops before next step when context canceled", func(t *testing.T) {
		type payload struct {
			firstStepCalled  bool
			secondStepCalled bool
		}

		pipeline := NewPipeline[*payload]()
		ctx, cancel := context.WithCancel(context.Background())

		pipeline.Add(Step[*payload]{
			Name: "first",
			Run: func(ctx context.Context, pl *payload) error {
				pl.firstStepCalled = true
				cancel()
				return nil
			},
		})

		pipeline.Add(Step[*payload]{
			Name: "second",
			Run: func(ctx context.Context, pl *payload) error {
				pl.secondStepCalled = true
				return nil
			},
		})

		pl := &payload{}

		err := pipeline.Run(ctx, pl)
		require.EqualError(t, err, "second: context canceled after step \"first\": context canceled")
		assert.True(t, pl.firstStepCalled, "first step must be called")
		assert.False(t, pl.secondStepCalled, "second step must be not called")
	})

	t.Run("test metrics used", func(t *testing.T) {
		tests := []struct {
			name                    string
			steps                   []Step[*struct{}]
			expectedErr             string
			expectedPipelineStarted map[string]int
			expectedStepStarted     map[string]int
			expectedStepSucceed     map[string]int
			expectedStepFailed      map[string]int
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
				expectedPipelineStarted: map[string]int{
					"deploy": 1,
				},
				expectedStepStarted: map[string]int{
					"deploy/first":  1,
					"deploy/failed": 1,
				},
				expectedStepSucceed: map[string]int{
					"deploy/first": 1,
				},
				expectedStepFailed: map[string]int{
					"deploy/failed": 1,
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
				expectedPipelineStarted: map[string]int{
					"deploy": 1,
				},
				expectedStepStarted: map[string]int{
					"deploy/panic": 1,
				},
				expectedStepSucceed: map[string]int{},
				expectedStepFailed: map[string]int{
					"deploy/panic": 1,
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				metrics := newTestMetrics()
				pipeline := NewPipelineWithConfig[*struct{}](Config{
					PipelineName: "deploy",
					Metrics:      metrics,
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

				assert.Equal(t, tt.expectedPipelineStarted, metrics.pipelineStarted)
				assert.Equal(t, tt.expectedStepStarted, metrics.stepStarted)
				assert.Equal(t, tt.expectedStepSucceed, metrics.stepSucceed)
				assert.Equal(t, tt.expectedStepFailed, metrics.stepFailed)
			})
		}
	})
}
