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
	tracer   *tracingService
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
		client.tracer = newTracingService(config.Tracing)
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

// prepareMessage builds the wire message and stashes the span ender in
// Opaque — the delivery report hands the same closure back, so correlation
// is by identity, never by index.
func (p *Producer) prepareMessage(ctx context.Context, topic string, msg *Message) *ckafka.Message {
	km := p.buildKafkaMessage(topic, msg)
	if p.tracer != nil {
		msgCtx, end := p.tracer.startProducerSpan(ctx, topic, msg)
		p.tracer.injectTraceContext(msgCtx, km)
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
	// await consumes one delivery report; it returns false when ctx is done.
	await := func() bool {
		select {
		case e := <-deliveryChan:
			pending--
			if err := finishDelivery(e); err != nil {
				errs = append(errs, err)
			}
			return true
		case <-ctx.Done():
			return false
		}
	}
	abort := func() error {
		for _, m := range msgs {
			endSpan(m, ctx.Err()) // idempotent: already-reported messages skip
		}
		return errors.Join(append(errs, ctx.Err())...)
	}
	for _, m := range msgs {
		for {
			err := p.producer.Produce(m, deliveryChan)
			if err == nil {
				pending++
				break
			}
			var kerr ckafka.Error
			// An empty queue cannot drain, so queue-full there is permanent
			// (e.g. the message alone exceeds queue.buffering.max.kbytes).
			if !errors.As(err, &kerr) || kerr.Code() != ckafka.ErrQueueFull ||
				(pending == 0 && p.producer.Len() == 0) {
				endSpan(m, err) // no report will ever arrive for this message
				errs = append(errs, err)
				break
			}
			// librdkafka's queue is full: back-pressure by draining one
			// delivery report (or flushing) and retrying the same message.
			if ctx.Err() != nil {
				return abort()
			}
			if pending > 0 {
				if !await() {
					return abort()
				}
			} else {
				p.producer.Flush(100)
			}
		}
	}
	for pending > 0 {
		if !await() {
			return abort()
		}
	}
	return errors.Join(errs...)
}

// finishDelivery ends the span for one delivery report and returns the
// delivery error, if any.
func finishDelivery(e ckafka.Event) error {
	m, ok := e.(*ckafka.Message)
	if !ok {
		return nil
	}
	if err := m.TopicPartition.Error; err != nil {
		endSpan(m, err)
		return fmt.Errorf("delivery failed for topic %s partition %d: %w",
			topicOf(m), m.TopicPartition.Partition, err)
	}
	endSpan(m, nil)
	return nil
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

	// Flush while the reader goroutine is still consuming Events(): Len()
	// counts unread delivery reports, so stopping the reader first would
	// stall Flush for the full timeout and drop async delivery failures.
	p.producer.Flush(10000)
	close(p.done)
	p.wg.Wait() // let the report reader exit before closing the producer under it (#30)
	p.producer.Close()
	return nil
}

// buildKafkaMessage builds a ckafka.Message from Message
func (p *Producer) buildKafkaMessage(topic string, msg *Message) *ckafka.Message {
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
