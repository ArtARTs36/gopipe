package gopipe

import "go.opentelemetry.io/otel/trace"

func newTracer(tp trace.TracerProvider) trace.Tracer {
	return tp.Tracer("github.com/artarts36/gopipe", trace.WithInstrumentationVersion("v0.2.0"))
}
