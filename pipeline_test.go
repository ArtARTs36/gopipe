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
		assert.Equal(t, int32(1), pl.attempts)
	})
}
