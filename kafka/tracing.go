package kafka

import (
	"context"
	"fmt"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Semantic convention attributes for messaging
const (
	messagingSystemKey              = "messaging.system"
	messagingDestinationNameKey     = "messaging.destination.name"
	messagingDestinationPartitionID = "messaging.destination.partition.id"
	messagingOperationNameKey       = "messaging.operation.name"
	messagingOperationTypeKey       = "messaging.operation.type"
	messagingKafkaOffsetKey         = "messaging.kafka.offset"
	messagingConsumerGroupKey       = "messaging.kafka.consumer.group"
	messagingKafkaMessageKeyKey     = "messaging.kafka.message.key"
	messagingBatchMessageCountKey   = "messaging.batch.message_count"
)

// tracingService provides OpenTelemetry tracing for Kafka operations
type tracingService struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	config     *TracingConfig
}

// newTracingService creates a new tracing service
func newTracingService(config *TracingConfig) *tracingService {
	tracerName := config.TracerName
	if tracerName == "" {
		tracerName = "github.com/loipv/kafka-go"
	}

	tracerVersion := config.TracerVersion
	if tracerVersion == "" {
		tracerVersion = Version
	}

	return &tracingService{
		tracer:     otel.Tracer(tracerName, trace.WithInstrumentationVersion(tracerVersion)),
		propagator: defaultPropagator(),
		config:     config,
	}
}

// defaultPropagator falls back to W3C Trace Context when the otel global is
// the zero-propagator (its default) — otherwise the library's headline
// tracing feature silently injected nothing.
func defaultPropagator() propagation.TextMapPropagator {
	p := otel.GetTextMapPropagator()
	if len(p.Fields()) == 0 {
		return propagation.TraceContext{}
	}
	return p
}

// startProducerSpan starts a new span for producing a message
func (t *tracingService) startProducerSpan(ctx context.Context, topic string, msg *Message) (context.Context, func(error)) {
	spanName := fmt.Sprintf("%s publish", topic)

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String(messagingSystemKey, "kafka"),
			attribute.String(messagingDestinationNameKey, topic),
			attribute.String(messagingOperationNameKey, "publish"),
			attribute.String(messagingOperationTypeKey, "publish"),
		),
	)

	// Add message key if present
	if msg.Key != nil {
		span.SetAttributes(attribute.String(messagingKafkaMessageKeyKey, string(msg.Key)))
	}

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// startConsumerSpan starts a new span for consuming a message
func (t *tracingService) startConsumerSpan(ctx context.Context, groupID string, msg *Message) (context.Context, func(error)) {
	// Extract trace context from message headers
	ctx = t.extractTraceContext(ctx, msg)

	spanName := fmt.Sprintf("%s %s process", groupID, msg.Topic)

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(messagingSystemKey, "kafka"),
			attribute.String(messagingDestinationNameKey, msg.Topic),
			attribute.Int(messagingDestinationPartitionID, int(msg.Partition)),
			attribute.String(messagingOperationNameKey, "process"),
			attribute.String(messagingOperationTypeKey, "process"),
			attribute.Int64(messagingKafkaOffsetKey, msg.Offset),
			attribute.String(messagingConsumerGroupKey, groupID),
		),
	)

	// Add message key if present
	if msg.Key != nil {
		span.SetAttributes(attribute.String(messagingKafkaMessageKeyKey, string(msg.Key)))
	}

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// startBatchConsumerSpan starts a new span for consuming a batch of messages
func (t *tracingService) startBatchConsumerSpan(ctx context.Context, groupID string, msgs []*Message) (context.Context, func(error)) {
	if len(msgs) == 0 {
		return ctx, func(error) {}
	}

	// Use first message's topic for span name
	topic := msgs[0].Topic
	spanName := fmt.Sprintf("%s %s process batch", groupID, topic)

	// Extract trace context from first message for parent span
	ctx = t.extractTraceContext(ctx, msgs[0])

	// Collect links from other messages
	var links []trace.Link
	for i := 1; i < len(msgs); i++ {
		msgCtx := t.extractTraceContext(context.Background(), msgs[i])
		spanCtx := trace.SpanContextFromContext(msgCtx)
		if spanCtx.IsValid() {
			links = append(links, trace.Link{
				SpanContext: spanCtx,
				Attributes: []attribute.KeyValue{
					attribute.Int("message.index", i),
				},
			})
		}
	}

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links...),
		trace.WithAttributes(
			attribute.String(messagingSystemKey, "kafka"),
			attribute.String(messagingDestinationNameKey, topic),
			attribute.String(messagingOperationNameKey, "process"),
			attribute.String(messagingOperationTypeKey, "process"),
			attribute.String(messagingConsumerGroupKey, groupID),
			attribute.Int(messagingBatchMessageCountKey, len(msgs)),
		),
	)

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// injectTraceContext injects trace context into Kafka message headers
func (t *tracingService) injectTraceContext(ctx context.Context, msg *ckafka.Message) {
	carrier := &kafkaHeaderCarrier{msg: msg}
	t.propagator.Inject(ctx, carrier)
}

// extractTraceContext extracts trace context from message headers
func (t *tracingService) extractTraceContext(ctx context.Context, msg *Message) context.Context {
	carrier := &messageHeaderCarrier{msg: msg}
	return t.propagator.Extract(ctx, carrier)
}

// kafkaHeaderCarrier implements propagation.TextMapCarrier for ckafka.Message
type kafkaHeaderCarrier struct {
	msg *ckafka.Message
}

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	// Try to update existing header in-place (avoid allocation)
	for i := range c.msg.Headers {
		if c.msg.Headers[i].Key == key {
			c.msg.Headers[i].Value = []byte(val)
			return
		}
	}
	// Append new header if not found
	c.msg.Headers = append(c.msg.Headers, ckafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// messageHeaderCarrier implements propagation.TextMapCarrier for Message
type messageHeaderCarrier struct {
	msg *Message
}

func (c *messageHeaderCarrier) Get(key string) string {
	if c.msg.Headers == nil {
		return ""
	}
	if val, ok := c.msg.Headers[key]; ok {
		return string(val)
	}
	return ""
}

func (c *messageHeaderCarrier) Set(key, val string) {
	if c.msg.Headers == nil {
		c.msg.Headers = make(Headers)
	}
	c.msg.Headers[key] = []byte(val)
}

func (c *messageHeaderCarrier) Keys() []string {
	if c.msg.Headers == nil {
		return nil
	}
	keys := make([]string, 0, len(c.msg.Headers))
	for k := range c.msg.Headers {
		keys = append(keys, k)
	}
	return keys
}
