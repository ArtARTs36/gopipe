package prometheus

import (
	"fmt"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/artarts36/gopipe"
)

func MustRegister() {
	if err := Register(); err != nil {
		panic(fmt.Sprintf("gopipe: failed to register metrics: %s", err.Error()))
	}
}

func Register() error {
	return RegisterIn(prom.DefaultRegisterer)
}

func RegisterIn(registry prom.Registerer) error {
	metrics := New()

	if err := registry.Register(metrics); err != nil {
		return err
	}

	gopipe.SetDefaultMetrics(metrics)

	return nil
}
