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

// Without otel.SetTextMapPropagator, the global is a no-op and W3C
// propagation silently did nothing. The library must default to TraceContext.
func TestTracingServiceInjectsWithoutGlobalPropagator(t *testing.T) {
	orig := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(orig) })
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) // the no-op default

	sr := tracetest.NewSpanRecorder()
	origTP := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(origTP) })
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))

	svc := newTracingService(&TracingConfig{Enabled: true})
	spanCtx, end := svc.startProducerSpan(context.Background(), "t", &Message{})

	km := &ckafka.Message{}
	svc.injectTraceContext(spanCtx, km)
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

func TestTracing_MessageKeyOnlyWhenOptedIn(t *testing.T) {
	for _, include := range []bool{false, true} {
		sr := tracetest.NewSpanRecorder()
		origTP := otel.GetTracerProvider()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
		svc := newTracingService(&TracingConfig{Enabled: true, IncludeMessageKey: include})
		msg := &Message{Topic: "t", Key: []byte("user-42")}
		_, end := svc.startProducerSpan(context.Background(), "t", msg)
		end(nil)
		_, end = svc.startConsumerSpan(context.Background(), "g", msg)
		end(nil)
		otel.SetTracerProvider(origTP)
		for _, s := range sr.Ended() {
			var found bool
			for _, a := range s.Attributes() {
				if string(a.Key) == "messaging.kafka.message.key" {
					found = a.Value.AsString() == "user-42"
				}
			}
			if found != include {
				t.Errorf("IncludeMessageKey=%v: span %q has key attribute = %v", include, s.Name(), found)
			}
		}
	}
}
