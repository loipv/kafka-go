package kafka

import (
	"context"
	"errors"
	"fmt"
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
	logger   Logger
	closed   int32 // atomic: 0=open, 1=closed

	// Queue for batched sending
	queueMu     sync.Mutex
	queue       map[string][]*Message
	queueTicker *time.Ticker
	queueDone   chan struct{}
}

// NewProducer creates a new Kafka producer
func NewProducer(opts ...ProducerOption) (*Producer, error) {
	config := newDefaultProducerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("brokers are required")
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
		logger = NewDefaultLogger(config.LogLevel)
	}

	client := &Producer{
		producer:  producer,
		config:    config,
		logger:    logger,
		queue:     make(map[string][]*Message),
		queueDone: make(chan struct{}),
	}

	// Initialize tracing if enabled
	if config.Tracing != nil && config.Tracing.Enabled {
		client.tracer = NewTracingService(config.Tracing)
	}

	// Start delivery report handler
	go client.handleDeliveryReports()

	// Start queue flusher
	client.queueTicker = time.NewTicker(100 * time.Millisecond)
	go client.flushQueue()

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
		return fmt.Errorf("producer is closed")
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

// ProduceAsync queues a message for automatic batching
func (c *Producer) ProduceAsync(ctx context.Context, topic string, msg *Message) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("client is closed")
	}

	c.queueMu.Lock()
	c.queue[topic] = append(c.queue[topic], msg)
	c.queueMu.Unlock()

	return nil
}

// Flush waits for all queued messages to be sent
func (c *Producer) Flush(timeout time.Duration) error {
	// First flush the queue
	c.flushQueueNow()

	// Then wait for producer to flush
	remaining := c.producer.Flush(int(timeout.Milliseconds()))
	if remaining > 0 {
		return fmt.Errorf("%d messages still in queue after flush", remaining)
	}
	return nil
}

// Close closes the client
func (c *Producer) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	// Stop queue flusher
	close(c.queueDone)
	c.queueTicker.Stop()

	// Flush remaining messages
	c.flushQueueNow()
	c.producer.Flush(10000)

	c.producer.Close()
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
func (c *Producer) handleDeliveryReports() {
	for {
		select {
		case <-c.queueDone:
			return
		case e, ok := <-c.producer.Events():
			if !ok {
				return
			}
			switch ev := e.(type) {
			case *ckafka.Message:
				if ev.TopicPartition.Error != nil {
					c.logger.Error("Delivery failed: %v", ev.TopicPartition.Error)
				}
			case ckafka.Error:
				c.logger.Error("Kafka error: %v", ev)
			}
		}
	}
}

// flushQueue periodically flushes the queue
func (c *Producer) flushQueue() {
	for {
		select {
		case <-c.queueTicker.C:
			c.flushQueueNow()
		case <-c.queueDone:
			return
		}
	}
}

// flushQueueNow immediately flushes all queued messages
// Optimized to avoid allocation when queue is empty and pre-size new map
func (c *Producer) flushQueueNow() {
	c.queueMu.Lock()
	// Fast path: nothing to flush
	if len(c.queue) == 0 {
		c.queueMu.Unlock()
		return
	}
	queue := c.queue
	// Pre-size new map with previous capacity to reduce allocations
	c.queue = make(map[string][]*Message, len(queue))
	c.queueMu.Unlock()

	for topic, msgs := range queue {
		if len(msgs) == 0 {
			continue
		}
		// Produce without waiting for delivery (fire and forget for queued messages)
		for _, msg := range msgs {
			kafkaMsg := c.buildKafkaMessage(topic, msg)
			if err := c.producer.Produce(kafkaMsg, nil); err != nil {
				c.logger.Error("Failed to produce queued message: %v", err)
			}
		}
	}
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
