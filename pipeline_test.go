package gopipe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"

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
			When: func(payload *payload) bool {
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
}
