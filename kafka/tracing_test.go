package kafka

import (
	"context"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// #29: without otel.SetTextMapPropagator, the global is a no-op and W3C
// propagation silently did nothing. The library must default to TraceContext.
func TestTracingServiceInjectsWithoutGlobalPropagator(t *testing.T) {
	orig := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(orig) })
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) // the no-op default

	sr := tracetest.NewSpanRecorder()
	origTP := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(origTP) })
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))

	svc := NewTracingService(&TracingConfig{Enabled: true})
	spanCtx, end := svc.StartProducerSpan(context.Background(), "t", &Message{})

	km := &ckafka.Message{}
	svc.InjectTraceContext(spanCtx, km)
	end(nil)

	var found bool
	for _, h := range km.Headers {
		if h.Key == "traceparent" && len(h.Value) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("no traceparent header injected — propagator defaulted to no-op")
	}
	if len(sr.Ended()) != 1 {
		t.Errorf("ended spans = %d, want 1", len(sr.Ended()))
	}
}
