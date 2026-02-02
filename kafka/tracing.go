package kafka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Semantic convention attributes for messaging
const (
	MessagingSystemKey              = "messaging.system"
	MessagingDestinationNameKey     = "messaging.destination.name"
	MessagingDestinationPartitionID = "messaging.destination.partition.id"
	MessagingOperationNameKey       = "messaging.operation.name"
	MessagingOperationTypeKey       = "messaging.operation.type"
	MessagingKafkaOffsetKey         = "messaging.kafka.offset"
	MessagingKafkaConsumerGroupKey  = "messaging.kafka.consumer.group"
	MessagingKafkaMessageKeyKey     = "messaging.kafka.message.key"
	MessagingBatchMessageCountKey   = "messaging.batch.message_count"
)

// TracingService provides OpenTelemetry tracing for Kafka operations
type TracingService struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	config     *TracingConfig
}

// NewTracingService creates a new tracing service
func NewTracingService(config *TracingConfig) *TracingService {
	tracerName := config.TracerName
	if tracerName == "" {
		tracerName = "github.com/loipv/kafka-go"
	}

	tracerVersion := config.TracerVersion
	if tracerVersion == "" {
		tracerVersion = "1.0.0"
	}

	return &TracingService{
		tracer:     otel.Tracer(tracerName, trace.WithInstrumentationVersion(tracerVersion)),
		propagator: otel.GetTextMapPropagator(),
		config:     config,
	}
}

// StartProducerSpan starts a new span for producing a message
func (t *TracingService) StartProducerSpan(ctx context.Context, topic string, msg *Message) (context.Context, func(error)) {
	spanName := fmt.Sprintf("%s publish", topic)

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String(MessagingSystemKey, "kafka"),
			attribute.String(MessagingDestinationNameKey, topic),
			attribute.String(MessagingOperationNameKey, "publish"),
			attribute.String(MessagingOperationTypeKey, "publish"),
		),
	)

	// Add message key if present
	if msg.Key != nil {
		span.SetAttributes(attribute.String(MessagingKafkaMessageKeyKey, string(msg.Key)))
	}

	// Add partition if specified
	if msg.Partition != PartitionAny && msg.Partition >= 0 {
		span.SetAttributes(attribute.Int(MessagingDestinationPartitionID, int(msg.Partition)))
	}

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// StartConsumerSpan starts a new span for consuming a message
func (t *TracingService) StartConsumerSpan(ctx context.Context, groupID string, msg *Message) (context.Context, func(error)) {
	// Extract trace context from message headers
	ctx = t.ExtractTraceContext(ctx, msg)

	spanName := fmt.Sprintf("%s %s process", groupID, msg.Topic)

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(MessagingSystemKey, "kafka"),
			attribute.String(MessagingDestinationNameKey, msg.Topic),
			attribute.Int(MessagingDestinationPartitionID, int(msg.Partition)),
			attribute.String(MessagingOperationNameKey, "process"),
			attribute.String(MessagingOperationTypeKey, "process"),
			attribute.Int64(MessagingKafkaOffsetKey, msg.Offset),
			attribute.String(MessagingKafkaConsumerGroupKey, groupID),
		),
	)

	// Add message key if present
	if msg.Key != nil {
		span.SetAttributes(attribute.String(MessagingKafkaMessageKeyKey, string(msg.Key)))
	}

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// StartBatchConsumerSpan starts a new span for consuming a batch of messages
func (t *TracingService) StartBatchConsumerSpan(ctx context.Context, groupID string, msgs []*Message) (context.Context, func(error)) {
	if len(msgs) == 0 {
		return ctx, func(error) {}
	}

	// Use first message's topic for span name
	topic := msgs[0].Topic
	spanName := fmt.Sprintf("%s %s process batch", groupID, topic)

	// Extract trace context from first message for parent span
	ctx = t.ExtractTraceContext(ctx, msgs[0])

	// Collect links from other messages
	var links []trace.Link
	for i := 1; i < len(msgs); i++ {
		msgCtx := t.ExtractTraceContext(context.Background(), msgs[i])
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
			attribute.String(MessagingSystemKey, "kafka"),
			attribute.String(MessagingDestinationNameKey, topic),
			attribute.String(MessagingOperationNameKey, "process"),
			attribute.String(MessagingOperationTypeKey, "process"),
			attribute.String(MessagingKafkaConsumerGroupKey, groupID),
			attribute.Int(MessagingBatchMessageCountKey, len(msgs)),
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

// InjectTraceContext injects trace context into Kafka message headers
func (t *TracingService) InjectTraceContext(ctx context.Context, msg *kafka.Message) {
	carrier := &kafkaHeaderCarrier{msg: msg}
	t.propagator.Inject(ctx, carrier)
}

// ExtractTraceContext extracts trace context from message headers
func (t *TracingService) ExtractTraceContext(ctx context.Context, msg *Message) context.Context {
	carrier := &messageHeaderCarrier{msg: msg}
	return t.propagator.Extract(ctx, carrier)
}

// kafkaHeaderCarrier implements propagation.TextMapCarrier for kafka.Message
type kafkaHeaderCarrier struct {
	msg *kafka.Message
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
	c.msg.Headers = append(c.msg.Headers, kafka.Header{
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

