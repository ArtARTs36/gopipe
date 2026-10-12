package gopipe

import "go.opentelemetry.io/otel/trace"

func newTracer(tp trace.TracerProvider) trace.Tracer {
	return tp.Tracer(instrumentationName, trace.WithInstrumentationVersion(instrumentationVersion))
}
