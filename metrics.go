package gopipe

type Metrics interface {
	IncPipelineStarted(pipelineName string)

	IncStepStarted(pipelineName, stepName string)
	IncStepSucceed(pipelineName, stepName string)
	IncStepFailed(pipelineName, stepName string)
}

var defaultMetrics Metrics = nopMetrics{}

func SetDefaultMetrics(metrics Metrics) {
	if metrics == nil {
		panic("SetDefaultMetrics: metrics is nil")
	}

	defaultMetrics = metrics
}

type nopMetrics struct{}

func (nopMetrics) IncPipelineStarted(string) {}

func (nopMetrics) IncStepStarted(string, string) {}

func (nopMetrics) IncStepSucceed(string, string) {}

func (nopMetrics) IncStepFailed(string, string) {}
