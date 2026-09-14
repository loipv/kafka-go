package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer is a high-performance Kafka producer.
type Producer struct {
	producer *ckafka.Producer
	config   *ProducerConfig
	tracer   *TracingService
	logger   *slog.Logger
	closed   int32 // atomic: 0=open, 1=closed

	done        chan struct{}
	wg          sync.WaitGroup
	deliveryErr func(*Message, error)
}

// NewProducer creates a new Kafka producer
func NewProducer(opts ...ProducerOption) (*Producer, error) {
	config := newDefaultProducerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, ErrBrokersRequired
	}

	// Build kafka config map (connection/auth via the connConfig seam)
	configMap := buildProducerConfig(config)

	producer, err := ckafka.NewProducer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = slog.Default() // silence with slog.New(slog.DiscardHandler)
	}

	client := &Producer{
		producer:    producer,
		config:      config,
		logger:      logger,
		done:        make(chan struct{}),
		deliveryErr: config.DeliveryErrorHandler,
	}

	// Initialize tracing if enabled
	if config.Tracing != nil && config.Tracing.Enabled {
		client.tracer = NewTracingService(config.Tracing)
	}

	// Start delivery report handler
	client.wg.Add(1)
	go client.handleDeliveryReports()

	return client, nil
}

// Produce sends a single message to a topic and waits for its delivery report.
func (p *Producer) Produce(ctx context.Context, topic string, msg *Message) error {
	return p.produceAndAwait(ctx, []*ckafka.Message{p.prepareMessage(ctx, topic, msg)})
}

// ProduceBatch sends multiple messages to one topic and waits for all reports.
func (p *Producer) ProduceBatch(ctx context.Context, topic string, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	km := make([]*ckafka.Message, 0, len(msgs))
	for _, m := range msgs {
		km = append(km, p.prepareMessage(ctx, topic, m))
	}
	return p.produceAndAwait(ctx, km)
}

// ProduceMultiTopicBatch sends messages to multiple topics and waits for all reports.
func (p *Producer) ProduceMultiTopicBatch(ctx context.Context, batches []TopicBatch) error {
	total := 0
	for _, b := range batches {
		total += len(b.Messages)
	}
	if total == 0 {
		return nil
	}
	km := make([]*ckafka.Message, 0, total)
	for _, b := range batches {
		for _, m := range b.Messages {
			km = append(km, p.prepareMessage(ctx, b.Topic, m))
		}
	}
	return p.produceAndAwait(ctx, km)
}

// prepareMessage builds the wire message and stashes the span ender in
// Opaque — the delivery report hands the same closure back, so correlation
// is by identity, never by index.
func (p *Producer) prepareMessage(ctx context.Context, topic string, msg *Message) *ckafka.Message {
	km := p.buildKafkaMessage(topic, msg)
	if p.tracer != nil {
		msgCtx, end := p.tracer.StartProducerSpan(ctx, topic, msg)
		p.tracer.InjectTraceContext(msgCtx, km)
		km.Opaque = end
	}
	return km
}

func (p *Producer) produceAndAwait(ctx context.Context, msgs []*ckafka.Message) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		err := ErrProducerClosed
		for _, m := range msgs {
			endSpan(m, err) // spans were started in prepareMessage before this check
		}
		return err
	}
	deliveryChan := make(chan ckafka.Event, len(msgs))
	var errs []error
	pending := 0
	for _, m := range msgs {
		if err := p.producer.Produce(m, deliveryChan); err != nil {
			endSpan(m, err) // no report will ever arrive for this message
			errs = append(errs, err)
			continue
		}
		pending++
	}
	for i := 0; i < pending; i++ {
		select {
		case e := <-deliveryChan:
			m, ok := e.(*ckafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				endSpan(m, m.TopicPartition.Error)
				errs = append(errs, fmt.Errorf("delivery failed for topic %s partition %d: %w",
					topicOf(m), m.TopicPartition.Partition, m.TopicPartition.Error))
			} else {
				endSpan(m, nil)
			}
		case <-ctx.Done():
			for _, m := range msgs {
				endSpan(m, ctx.Err()) // idempotent: already-reported messages skip
			}
			return errors.Join(append(errs, ctx.Err())...)
		}
	}
	return errors.Join(errs...)
}

func endSpan(m *ckafka.Message, err error) {
	if end, ok := m.Opaque.(func(error)); ok {
		end(err)
		m.Opaque = nil
	}
}

func topicOf(m *ckafka.Message) string {
	if m.TopicPartition.Topic == nil {
		return ""
	}
	return *m.TopicPartition.Topic
}

// ProduceAsync queues a message on librdkafka's internal queue and returns
// immediately; librdkafka batches (linger.ms / batch.num.messages) and
// back-pressures naturally when its queue is full. Delivery failures go to
// the DeliveryErrorHandler.
func (p *Producer) ProduceAsync(topic string, msg *Message) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return ErrProducerClosed
	}
	return p.producer.Produce(p.buildKafkaMessage(topic, msg), nil)
}

// Flush waits for all queued messages to be sent
func (p *Producer) Flush(timeout time.Duration) error {
	remaining := p.producer.Flush(int(timeout.Milliseconds()))
	if remaining > 0 {
		return fmt.Errorf("%d messages still in queue after flush", remaining)
	}
	return nil
}

// Close flushes outstanding messages and closes the producer. It waits for
// the delivery-report reader to drain before closing the producer under it.
func (p *Producer) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	close(p.done)
	p.wg.Wait() // let the report reader drain before closing the producer under it (#30)
	p.producer.Flush(10000)
	p.producer.Close()
	return nil
}

// buildKafkaMessage builds a ckafka.Message from Message
func (c *Producer) buildKafkaMessage(topic string, msg *Message) *ckafka.Message {
	kafkaMsg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Value: msg.Value,
	}

	if msg.Key != nil {
		kafkaMsg.Key = msg.Key
	}

	if !msg.Timestamp.IsZero() {
		kafkaMsg.Timestamp = msg.Timestamp
	}

	if msg.Headers != nil {
		for k, v := range msg.Headers {
			kafkaMsg.Headers = append(kafkaMsg.Headers, ckafka.Header{
				Key:   k,
				Value: v,
			})
		}
	}

	return kafkaMsg
}

// handleDeliveryReports handles delivery reports from the producer
func (p *Producer) handleDeliveryReports() {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		case e, ok := <-p.producer.Events():
			if !ok {
				return
			}
			switch ev := e.(type) {
			case *ckafka.Message:
				if ev.TopicPartition.Error != nil {
					endSpan(ev, ev.TopicPartition.Error)
					p.reportDeliveryError(ev, ev.TopicPartition.Error)
				} else {
					endSpan(ev, nil)
				}
			case ckafka.Error:
				p.logger.Error("kafka error", "error", ev)
			}
		}
	}
}

// reportDeliveryError routes an async delivery failure to the configured
// DeliveryErrorHandler, or logs it when none is set.
func (p *Producer) reportDeliveryError(ev *ckafka.Message, err error) {
	var headers Headers
	if len(ev.Headers) > 0 {
		headers = make(Headers, len(ev.Headers))
		for _, h := range ev.Headers {
			headers[h.Key] = h.Value
		}
	}
	msg := &Message{
		Key:       ev.Key,
		Value:     ev.Value,
		Headers:   headers,
		Partition: ev.TopicPartition.Partition,
		Offset:    int64(ev.TopicPartition.Offset),
		Topic:     topicOf(ev),
	}
	if p.deliveryErr != nil {
		p.deliveryErr(msg, err)
		return
	}
	p.logger.Error("async delivery failed", "topic", msg.Topic, "error", err)
}

// Helper functions

func getCompressionName(compression Compression) string {
	switch compression {
	case CompressionGZIP:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	default:
		return "none"
	}
}
