package prometheus

import (
	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/artarts36/gopipe"
)

const namespace = "gopipe"

var (
	_ gopipe.Metrics = (*Metrics)(nil)
	_ prom.Collector = (*Metrics)(nil)
)

type Metrics struct {
	pipelineStarted *prom.CounterVec
	stepStarted     *prom.CounterVec
	stepSucceed     *prom.CounterVec
	stepFailed      *prom.CounterVec
}

func New() *Metrics {
	return &Metrics{
		pipelineStarted: prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Name:      "pipeline_started_total",
				Help:      "Total number of started pipelines.",
			},
			[]string{"pipeline_name"},
		),
		stepStarted: prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Name:      "step_started_total",
				Help:      "Total number of started pipeline steps.",
			},
			[]string{"pipeline_name", "step_name"},
		),
		stepSucceed: prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Name:      "step_succeed_total",
				Help:      "Total number of successful pipeline steps.",
			},
			[]string{"pipeline_name", "step_name"},
		),
		stepFailed: prom.NewCounterVec(
			prom.CounterOpts{
				Namespace: namespace,
				Name:      "step_failed_total",
				Help:      "Total number of failed pipeline steps.",
			},
			[]string{"pipeline_name", "step_name"},
		),
	}
}

func (m *Metrics) IncPipelineStarted(pipelineName string) {
	m.pipelineStarted.WithLabelValues(pipelineName).Inc()
}

func (m *Metrics) IncStepStarted(pipelineName, stepName string) {
	m.stepStarted.WithLabelValues(pipelineName, stepName).Inc()
}

func (m *Metrics) IncStepSucceed(pipelineName, stepName string) {
	m.stepSucceed.WithLabelValues(pipelineName, stepName).Inc()
}

func (m *Metrics) IncStepFailed(pipelineName, stepName string) {
	m.stepFailed.WithLabelValues(pipelineName, stepName).Inc()
}

func (m *Metrics) Describe(ch chan<- *prom.Desc) {
	m.pipelineStarted.Describe(ch)
	m.stepStarted.Describe(ch)
	m.stepSucceed.Describe(ch)
	m.stepFailed.Describe(ch)
}

func (m *Metrics) Collect(ch chan<- prom.Metric) {
	m.pipelineStarted.Collect(ch)
	m.stepStarted.Collect(ch)
	m.stepSucceed.Collect(ch)
	m.stepFailed.Collect(ch)
}
