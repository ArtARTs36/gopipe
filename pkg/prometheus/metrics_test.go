package prometheus

import (
	"testing"

	clientprometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetrics(t *testing.T) {
	t.Run("increments all counters through collector", func(t *testing.T) {
		registry := clientprometheus.NewRegistry()
		metrics := New()

		err := registry.Register(metrics)
		if err != nil {
			t.Fatalf("register metrics: %v", err)
		}

		metrics.IncPipelineStarted("deploy")
		metrics.IncStepStarted("deploy", "clone")
		metrics.IncStepSucceed("deploy", "clone")
		metrics.IncStepFailed("deploy", "test")

		assertFloat64Equal(t, 1, testutil.ToFloat64(metrics.pipelineStarted.WithLabelValues("deploy")))
		assertFloat64Equal(t, 1, testutil.ToFloat64(metrics.stepStarted.WithLabelValues("deploy", "clone")))
		assertFloat64Equal(t, 1, testutil.ToFloat64(metrics.stepSucceed.WithLabelValues("deploy", "clone")))
		assertFloat64Equal(t, 1, testutil.ToFloat64(metrics.stepFailed.WithLabelValues("deploy", "test")))
	})

	t.Run("duplicate collector registration fails", func(t *testing.T) {
		registry := clientprometheus.NewRegistry()
		firstMetrics := New()
		secondMetrics := New()

		err := registry.Register(firstMetrics)
		if err != nil {
			t.Fatalf("register first metrics: %v", err)
		}

		err = registry.Register(secondMetrics)
		if err == nil {
			t.Fatal("expected duplicate registration error")
		}
	})
}

func assertFloat64Equal(t *testing.T, expected, actual float64) {
	t.Helper()

	if expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
