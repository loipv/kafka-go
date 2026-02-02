package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Verify KafkaClient implements Client interface
var _ Client = (*KafkaClient)(nil)

// KafkaClient implements the Client interface
type KafkaClient struct {
	producer *kafka.Producer
	config   *ClientConfig
	tracer   *TracingService
	logger   Logger
	closed   int32 // atomic: 0=open, 1=closed

	// Queue for batched sending
	queueMu     sync.Mutex
	queue       map[string][]*Message
	queueTicker *time.Ticker
	queueDone   chan struct{}
}

// NewClient creates a new Kafka client
func NewClient(opts ...ClientOption) (*KafkaClient, error) {
	config := newDefaultClientConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("brokers are required")
	}

	// Build kafka config map
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Brokers, ","),
		"acks":              int(config.Acks),
	}

	if config.ClientID != "" {
		configMap.SetKey("client.id", config.ClientID)
	}

	if config.ConnectionTimeout > 0 {
		configMap.SetKey("socket.connection.setup.timeout.ms", int(config.ConnectionTimeout.Milliseconds()))
	}

	if config.RequestTimeout > 0 {
		configMap.SetKey("request.timeout.ms", int(config.RequestTimeout.Milliseconds()))
	}

	if config.Compression != CompressionNone {
		configMap.SetKey("compression.type", getCompressionName(config.Compression))
	}

	if config.Idempotent {
		configMap.SetKey("enable.idempotence", true)
	}

	if config.SSL {
		configMap.SetKey("security.protocol", "ssl")
	}

	if config.SASL != nil {
		if config.SSL {
			configMap.SetKey("security.protocol", "sasl_ssl")
		} else {
			configMap.SetKey("security.protocol", "sasl_plaintext")
		}
		configMap.SetKey("sasl.mechanism", config.SASL.Mechanism)
		configMap.SetKey("sasl.username", config.SASL.Username)
		configMap.SetKey("sasl.password", config.SASL.Password)
	}

	// Set log level
	configMap.SetKey("log_level", int(config.LogLevel))

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = NewDefaultLogger(config.LogLevel)
	}

	client := &KafkaClient{
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

// Send sends a single message to a topic
func (c *KafkaClient) Send(ctx context.Context, topic string, msg *Message) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("client is closed")
	}

	kafkaMsg := c.buildKafkaMessage(topic, msg)

	// Add tracing
	var endSpan func(error)
	if c.tracer != nil {
		ctx, endSpan = c.tracer.StartProducerSpan(ctx, topic, msg)
		c.tracer.InjectTraceContext(ctx, kafkaMsg)
	}

	deliveryChan := make(chan kafka.Event, 1)
	err := c.producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
		if endSpan != nil {
			endSpan(err)
		}
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report
	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			if endSpan != nil {
				endSpan(m.TopicPartition.Error)
			}
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		if endSpan != nil {
			endSpan(nil)
		}
		return nil
	case <-ctx.Done():
		if endSpan != nil {
			endSpan(ctx.Err())
		}
		return ctx.Err()
	}
}

// SendBatch sends multiple messages to a single topic
func (c *KafkaClient) SendBatch(ctx context.Context, topic string, msgs []*Message) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("client is closed")
	}

	if len(msgs) == 0 {
		return nil
	}

	type messageWithSpan struct {
		kafkaMsg *kafka.Message
		endSpan  func(error)
	}

	msgsWithSpans := make([]messageWithSpan, 0, len(msgs))
	var produceErrors []error

	for _, msg := range msgs {
		kafkaMsg := c.buildKafkaMessage(topic, msg)

		var endSpan func(error)
		if c.tracer != nil {
			msgCtx, es := c.tracer.StartProducerSpan(ctx, topic, msg)
			c.tracer.InjectTraceContext(msgCtx, kafkaMsg)
			endSpan = es
		}

		msgsWithSpans = append(msgsWithSpans, messageWithSpan{
			kafkaMsg: kafkaMsg,
			endSpan:  endSpan,
		})
	}

	// Create delivery channel for all messages
	deliveryChan := make(chan kafka.Event, len(msgs))
	producedCount := 0

	for _, mws := range msgsWithSpans {
		err := c.producer.Produce(mws.kafkaMsg, deliveryChan)
		if err != nil {
			produceErrors = append(produceErrors, err)
			if mws.endSpan != nil {
				mws.endSpan(err)
			}
		} else {
			producedCount++
		}
	}

	// Wait for all delivery reports
	deliveryErrors := make(map[int]error)
	for i := 0; i < producedCount; i++ {
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				deliveryErrors[i] = m.TopicPartition.Error
			}
		case <-ctx.Done():
			// End all remaining spans with context error
			for j := i; j < len(msgsWithSpans); j++ {
				if msgsWithSpans[j].endSpan != nil {
					msgsWithSpans[j].endSpan(ctx.Err())
				}
			}
			return ctx.Err()
		}
	}

	// End all spans with appropriate errors
	deliveryIdx := 0
	for i, mws := range msgsWithSpans {
		if mws.endSpan != nil {
			// Check if this message had a produce error
			if i < len(produceErrors) && produceErrors[i] != nil {
				// Already ended in produce loop
				continue
			}
			// Check delivery error
			if err, ok := deliveryErrors[deliveryIdx]; ok {
				mws.endSpan(err)
			} else {
				mws.endSpan(nil)
			}
			deliveryIdx++
		}
	}

	// Collect all errors
	allErrors := append(produceErrors, mapValuesToSlice(deliveryErrors)...)
	if len(allErrors) > 0 {
		return fmt.Errorf("failed to send %d messages: %v", len(allErrors), allErrors)
	}

	return nil
}

// SendMultiTopicBatch sends messages to multiple topics
func (c *KafkaClient) SendMultiTopicBatch(ctx context.Context, batches []TopicMessages) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("client is closed")
	}

	totalMsgs := 0
	for _, batch := range batches {
		totalMsgs += len(batch.Messages)
	}

	if totalMsgs == 0 {
		return nil
	}

	type messageWithSpan struct {
		kafkaMsg *kafka.Message
		endSpan  func(error)
	}

	msgsWithSpans := make([]messageWithSpan, 0, totalMsgs)
	var produceErrors []error

	for _, batch := range batches {
		for _, msg := range batch.Messages {
			kafkaMsg := c.buildKafkaMessage(batch.Topic, msg)

			var endSpan func(error)
			if c.tracer != nil {
				msgCtx, es := c.tracer.StartProducerSpan(ctx, batch.Topic, msg)
				c.tracer.InjectTraceContext(msgCtx, kafkaMsg)
				endSpan = es
			}

			msgsWithSpans = append(msgsWithSpans, messageWithSpan{
				kafkaMsg: kafkaMsg,
				endSpan:  endSpan,
			})
		}
	}

	// Create delivery channel
	deliveryChan := make(chan kafka.Event, totalMsgs)
	producedCount := 0

	for _, mws := range msgsWithSpans {
		err := c.producer.Produce(mws.kafkaMsg, deliveryChan)
		if err != nil {
			produceErrors = append(produceErrors, err)
			if mws.endSpan != nil {
				mws.endSpan(err)
			}
		} else {
			producedCount++
		}
	}

	// Wait for all delivery reports
	deliveryErrors := make(map[int]error)
	for i := 0; i < producedCount; i++ {
		select {
		case e := <-deliveryChan:
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				deliveryErrors[i] = m.TopicPartition.Error
			}
		case <-ctx.Done():
			for j := i; j < len(msgsWithSpans); j++ {
				if msgsWithSpans[j].endSpan != nil {
					msgsWithSpans[j].endSpan(ctx.Err())
				}
			}
			return ctx.Err()
		}
	}

	// End all spans
	deliveryIdx := 0
	for i, mws := range msgsWithSpans {
		if mws.endSpan != nil {
			if i < len(produceErrors) && produceErrors[i] != nil {
				continue
			}
			if err, ok := deliveryErrors[deliveryIdx]; ok {
				mws.endSpan(err)
			} else {
				mws.endSpan(nil)
			}
			deliveryIdx++
		}
	}

	allErrors := append(produceErrors, mapValuesToSlice(deliveryErrors)...)
	if len(allErrors) > 0 {
		return fmt.Errorf("failed to send %d messages: %v", len(allErrors), allErrors)
	}

	return nil
}

// SendQueued queues a message for automatic batching
func (c *KafkaClient) SendQueued(ctx context.Context, topic string, msg *Message) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return fmt.Errorf("client is closed")
	}

	c.queueMu.Lock()
	c.queue[topic] = append(c.queue[topic], msg)
	c.queueMu.Unlock()

	return nil
}

// Flush waits for all queued messages to be sent
func (c *KafkaClient) Flush(timeout time.Duration) error {
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
func (c *KafkaClient) Close() error {
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

// buildKafkaMessage builds a kafka.Message from Message
func (c *KafkaClient) buildKafkaMessage(topic string, msg *Message) *kafka.Message {
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: msg.Value,
	}

	if msg.Key != nil {
		kafkaMsg.Key = msg.Key
	}

	if msg.Partition != 0 {
		kafkaMsg.TopicPartition.Partition = msg.Partition
	}

	if !msg.Timestamp.IsZero() {
		kafkaMsg.Timestamp = msg.Timestamp
	}

	if msg.Headers != nil {
		for k, v := range msg.Headers {
			kafkaMsg.Headers = append(kafkaMsg.Headers, kafka.Header{
				Key:   k,
				Value: v,
			})
		}
	}

	return kafkaMsg
}

// handleDeliveryReports handles delivery reports from the producer
func (c *KafkaClient) handleDeliveryReports() {
	for {
		select {
		case <-c.queueDone:
			return
		case e, ok := <-c.producer.Events():
			if !ok {
				return
			}
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					c.logger.Error("Delivery failed: %v", ev.TopicPartition.Error)
				}
			case kafka.Error:
				c.logger.Error("Kafka error: %v", ev)
			}
		}
	}
}

// flushQueue periodically flushes the queue
func (c *KafkaClient) flushQueue() {
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
func (c *KafkaClient) flushQueueNow() {
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
		// Send without waiting for delivery (fire and forget for queued messages)
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

func mapValuesToSlice(m map[int]error) []error {
	result := make([]error, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}
	return result
}

