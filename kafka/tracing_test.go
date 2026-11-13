package kafka

import (
	"context"
	"errors"
	"slices"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// newRecorder installs a recording tracer provider and the W3C propagator as
// the OpenTelemetry globals, restoring both when the test ends. Tests using
// it mutate globals and must not run in parallel.
func newRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	origTP := otel.GetTracerProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origProp)
	})
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return sr
}

// attrsOf indexes a span's attributes by key.
func attrsOf(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	m := make(map[string]attribute.Value)
	for _, a := range s.Attributes() {
		m[string(a.Key)] = a.Value
	}
	return m
}

// spanNamed returns the single ended span with the given name.
func spanNamed(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("spans named %q = %d, want 1", name, len(found))
	}
	return found[0]
}

// carriedMessage starts and ends a producer span on topic, injects its context
// into a ckafka message and returns the headers as a consumer-side Message.
func carriedMessage(t *testing.T, svc *tracingService, topic string, offset int64) *Message {
	t.Helper()
	ctx, end := svc.startProducerSpan(context.Background(), topic, &Message{})
	km := &ckafka.Message{}
	svc.injectTraceContext(ctx, km)
	end(nil)
	msg := &Message{Topic: topic, Offset: offset, Headers: make(Headers)}
	for _, h := range km.Headers {
		msg.Headers[h.Key] = h.Value
	}
	if len(msg.Headers) == 0 {
		t.Fatal("inject produced no headers")
	}
	return msg
}

// Without otel.SetTextMapPropagator, the global is a no-op and W3C
// propagation silently did nothing. The library must default to TraceContext.
func TestTracing_ServiceInjectsWithoutGlobalPropagator(t *testing.T) {
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
		sr := newRecorder(t)
		svc := newTracingService(&TracingConfig{Enabled: true, IncludeMessageKey: include})
		msg := &Message{Topic: "t", Key: []byte("user-42")}
		_, end := svc.startProducerSpan(context.Background(), "t", msg)
		end(nil)
		_, end = svc.startConsumerSpan(context.Background(), "g", msg)
		end(nil)
		if n := len(sr.Ended()); n != 2 {
			t.Fatalf("IncludeMessageKey=%v: ended spans = %d, want 2", include, n)
		}
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

func TestTracing_ProducerSpan(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})

	_, end := svc.startProducerSpan(context.Background(), "t", &Message{})
	end(nil)

	if n := len(sr.Ended()); n != 1 {
		t.Fatalf("ended spans = %d, want 1", n)
	}
	s := sr.Ended()[0]
	if s.Name() != "t publish" {
		t.Errorf("name = %q, want %q", s.Name(), "t publish")
	}
	if s.SpanKind() != trace.SpanKindProducer {
		t.Errorf("kind = %v, want Producer", s.SpanKind())
	}
	attrs := attrsOf(s)
	if got := attrs["messaging.system"].AsString(); got != "kafka" {
		t.Errorf("messaging.system = %q, want kafka", got)
	}
	if got := attrs["messaging.destination.name"].AsString(); got != "t" {
		t.Errorf("messaging.destination.name = %q, want t", got)
	}
	if got := attrs["messaging.operation.type"].AsString(); got != "publish" {
		t.Errorf("messaging.operation.type = %q, want publish", got)
	}
}

func TestTracing_ConsumerSpanContinuesTrace(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})

	msg := carriedMessage(t, svc, "t", 7)
	msg.Partition = 3
	_, end := svc.startConsumerSpan(context.Background(), "g", msg)
	end(nil)

	if n := len(sr.Ended()); n != 2 {
		t.Fatalf("ended spans = %d, want 2", n)
	}
	prod := spanNamed(t, sr, "t publish")
	cons := spanNamed(t, sr, "g t process")
	if cons.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("kind = %v, want Consumer", cons.SpanKind())
	}
	if cons.SpanContext().TraceID() != prod.SpanContext().TraceID() {
		t.Errorf("consumer trace id %s, want producer's %s", cons.SpanContext().TraceID(), prod.SpanContext().TraceID())
	}
	if cons.Parent().SpanID() != prod.SpanContext().SpanID() {
		t.Errorf("consumer parent span id %s, want producer span id %s", cons.Parent().SpanID(), prod.SpanContext().SpanID())
	}
	attrs := attrsOf(cons)
	off, ok := attrs["messaging.kafka.offset"]
	if !ok || off.AsInt64() != 7 {
		t.Errorf("messaging.kafka.offset = %v (present=%v), want 7", off, ok)
	}
	if got := attrs["messaging.destination.partition.id"].AsInt64(); got != 3 {
		t.Errorf("partition id = %d, want 3", got)
	}
	if got := attrs["messaging.kafka.consumer.group"].AsString(); got != "g" {
		t.Errorf("consumer group = %q, want g", got)
	}
}

func TestTracing_BatchSpanLinks(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})

	msgs := []*Message{
		carriedMessage(t, svc, "t", 0),
		carriedMessage(t, svc, "t", 1),
		carriedMessage(t, svc, "t", 2),
	}
	if n := len(sr.Ended()); n != 3 {
		t.Fatalf("ended producer spans = %d, want 3", n)
	}
	producers := slices.Clone(sr.Ended())

	_, end := svc.startBatchConsumerSpan(context.Background(), "g", msgs)
	end(nil)

	if n := len(sr.Ended()); n != 4 {
		t.Fatalf("ended spans = %d, want 4", n)
	}
	batch := spanNamed(t, sr, "g t process batch")
	if batch.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("kind = %v, want Consumer", batch.SpanKind())
	}
	if got := attrsOf(batch)["messaging.batch.message_count"].AsInt64(); got != 3 {
		t.Errorf("messaging.batch.message_count = %d, want 3", got)
	}
	// First message is the parent.
	if batch.Parent().SpanID() != producers[0].SpanContext().SpanID() {
		t.Errorf("batch parent %s, want first message's span %s", batch.Parent().SpanID(), producers[0].SpanContext().SpanID())
	}
	links := batch.Links()
	if len(links) != 2 {
		t.Fatalf("links = %d, want 2", len(links))
	}
	for i, l := range links {
		want := producers[i+1].SpanContext().TraceID()
		if l.SpanContext.TraceID() != want {
			t.Errorf("link %d trace id %s, want message %d's %s", i, l.SpanContext.TraceID(), i+1, want)
		}
	}
}

func TestTracing_BatchSpanEmptyBatch(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})

	ctx, end := svc.startBatchConsumerSpan(context.Background(), "g", nil)
	if ctx == nil || end == nil {
		t.Fatal("empty batch must return a usable context and end function")
	}
	end(errors.New("ignored"))
	if n := len(sr.Ended()) + len(sr.Started()); n != 0 {
		t.Errorf("spans recorded for empty batch = %d, want 0", n)
	}
}

func TestTracing_EndRecordsError(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})

	_, end := svc.startProducerSpan(context.Background(), "bad", &Message{})
	end(errors.New("x"))
	_, end = svc.startProducerSpan(context.Background(), "good", &Message{})
	end(nil)

	if n := len(sr.Ended()); n != 2 {
		t.Fatalf("ended spans = %d, want 2", n)
	}
	bad := spanNamed(t, sr, "bad publish")
	if bad.Status().Code != codes.Error || bad.Status().Description != "x" {
		t.Errorf("status = %+v, want Error/x", bad.Status())
	}
	var exceptions int
	for _, e := range bad.Events() {
		if e.Name == "exception" {
			exceptions++
		}
	}
	if exceptions != 1 {
		t.Errorf("exception events = %d, want 1", exceptions)
	}

	good := spanNamed(t, sr, "good publish")
	if good.Status().Code == codes.Error {
		t.Errorf("nil error produced status %+v", good.Status())
	}
	if len(good.Events()) != 0 {
		t.Errorf("nil error recorded %d events", len(good.Events()))
	}
}

func TestTracing_ConsumerAndBatchEndRecordError(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})
	msg := &Message{Topic: "t"}

	_, end := svc.startConsumerSpan(context.Background(), "g", msg)
	end(errors.New("c"))
	_, end = svc.startBatchConsumerSpan(context.Background(), "g", []*Message{msg})
	end(errors.New("b"))

	if n := len(sr.Ended()); n != 2 {
		t.Fatalf("ended spans = %d, want 2", n)
	}
	for name, desc := range map[string]string{"g t process": "c", "g t process batch": "b"} {
		s := spanNamed(t, sr, name)
		if s.Status().Code != codes.Error || s.Status().Description != desc {
			t.Errorf("%s status = %+v, want Error/%s", name, s.Status(), desc)
		}
	}
}

func TestTracing_DefaultTracerNameAndVersion(t *testing.T) {
	cases := []struct {
		name        string
		cfg         TracingConfig
		wantName    string
		wantVersion string
	}{
		{"defaults", TracingConfig{Enabled: true}, "github.com/loipv/kafka-go", Version},
		{"custom", TracingConfig{Enabled: true, TracerName: "my/tracer", TracerVersion: "9.9.9"}, "my/tracer", "9.9.9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sr := newRecorder(t)
			svc := newTracingService(&tc.cfg)
			_, end := svc.startProducerSpan(context.Background(), "t", &Message{})
			end(nil)

			if n := len(sr.Ended()); n != 1 {
				t.Fatalf("ended spans = %d, want 1", n)
			}
			scope := sr.Ended()[0].InstrumentationScope()
			if scope.Name != tc.wantName {
				t.Errorf("scope name = %q, want %q", scope.Name, tc.wantName)
			}
			if scope.Version != tc.wantVersion {
				t.Errorf("scope version = %q, want %q", scope.Version, tc.wantVersion)
			}
		})
	}
}

func TestTracing_ExtractWithoutHeaders(t *testing.T) {
	sr := newRecorder(t)
	svc := newTracingService(&TracingConfig{Enabled: true})
	msg := &Message{Topic: "t"}

	ctx := svc.extractTraceContext(context.Background(), msg)
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("extract from nil headers returned a valid span context")
	}

	_, end := svc.startConsumerSpan(context.Background(), "g", msg)
	end(nil)
	if n := len(sr.Ended()); n != 1 {
		t.Fatalf("ended spans = %d, want 1", n)
	}
	if sr.Ended()[0].Parent().IsValid() {
		t.Error("consumer span without headers must be a root span")
	}
}

func TestTracing_HeaderCarriers(t *testing.T) {
	km := &ckafka.Message{Headers: []ckafka.Header{{Key: "a", Value: []byte("1")}}}
	kc := &kafkaHeaderCarrier{msg: km}
	if got := kc.Get("a"); got != "1" {
		t.Errorf("Get(a) = %q, want 1", got)
	}
	if got := kc.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}
	kc.Set("a", "2") // updates in place
	kc.Set("b", "3") // appends
	if len(km.Headers) != 2 || string(km.Headers[0].Value) != "2" || km.Headers[1].Key != "b" {
		t.Errorf("headers after Set = %+v", km.Headers)
	}
	if keys := kc.Keys(); !slices.Equal(keys, []string{"a", "b"}) {
		t.Errorf("Keys() = %v, want [a b]", keys)
	}

	m := &Message{}
	mc := &messageHeaderCarrier{msg: m}
	if got := mc.Get("a"); got != "" {
		t.Errorf("Get on nil headers = %q, want empty", got)
	}
	if keys := mc.Keys(); len(keys) != 0 {
		t.Errorf("Keys on nil headers = %v, want none", keys)
	}
	mc.Set("a", "1") // allocates the map
	if got := mc.Get("a"); got != "1" {
		t.Errorf("Get(a) = %q, want 1", got)
	}
	if got := mc.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}
	if keys := mc.Keys(); !slices.Equal(keys, []string{"a"}) {
		t.Errorf("Keys() = %v, want [a]", keys)
	}
}
