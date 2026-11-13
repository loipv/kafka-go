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

// Producer is a Kafka producer built on a librdkafka producer handle. Create
// one with NewProducer and release it with Close.
//
// Produce, ProduceBatch, ProduceAsync and Flush are safe for concurrent use by
// multiple goroutines. Close must not run concurrently with them: stop the
// producing goroutines first, otherwise a produce call can race the teardown
// of the underlying handle.
type Producer struct {
	producer *ckafka.Producer
	config   *ProducerConfig
	tracer   *tracingService
	logger   *slog.Logger
	closed   atomic.Bool // true once closed

	done        chan struct{}
	wg          sync.WaitGroup
	deliveryErr func(*Message, error)
}

// NewProducer creates a Producer from the given options on top of the
// defaults documented on each ProducerWith* option. It returns
// ErrBrokersRequired when no broker is configured, and an error wrapping
// librdkafka's when the resulting configuration is rejected (for example an
// unsupported compression codec or a conflicting raw key). It does not wait for
// a broker connection: unreachable brokers surface as delivery errors on the
// first produce.
func NewProducer(opts ...ProducerOption) (*Producer, error) {
	config := newDefaultProducerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, ErrBrokersRequired
	}

	warnPlaintextSASL(config.Logger, config.conn())

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

	p := &Producer{
		producer:    producer,
		config:      config,
		logger:      logger,
		done:        make(chan struct{}),
		deliveryErr: config.DeliveryErrorHandler,
	}

	// Initialize tracing if enabled
	if config.Tracing != nil && config.Tracing.Enabled {
		p.tracer = newTracingService(config.Tracing)
	}

	// Start delivery report handler
	p.wg.Add(1)
	go p.handleDeliveryReports()

	return p, nil
}

// Produce sends a single message to topic and blocks until its delivery report
// arrives, returning the delivery error if the broker did not acknowledge it.
// The message's Partition, Offset and Topic fields are ignored; the partition
// is chosen by librdkafka from the key.
//
// When librdkafka's local queue is full, Produce applies back-pressure: it
// waits for in-flight delivery reports to drain the queue and retries. A
// message that can never fit (for example one larger than
// queue.buffering.max.kbytes) fails with a queue-full error instead of waiting
// forever. If ctx is done before the report arrives, Produce returns the
// context's error; the message may still be delivered afterwards. Calling it
// after Close returns ErrProducerClosed. It must not be called concurrently
// with Close.
func (p *Producer) Produce(ctx context.Context, topic string, msg *Message) error {
	return p.produceAndAwait(ctx, []*ckafka.Message{p.prepareMessage(ctx, topic, msg)})
}

// ProduceBatch sends msgs to topic and blocks until every delivery report has
// arrived. It returns nil for an empty batch. Failures are collected rather than
// stopping at the first one: the result is the errors.Join of every message
// that failed (use errors.Is / errors.As on it), or nil when all were
// acknowledged. The joined errors carry only the topic and partition, so a
// returned error does not identify which messages failed: treat the delivery
// of each message of the batch as unknown (some may have been delivered). When
// ctx ends first, the messages that have no delivery report yet may still be
// delivered later.
// Back-pressure, ctx handling and the ErrProducerClosed rule are the same as
// for Produce, and it must not be called concurrently with Close.
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
// is by identity, never by index. The report is a different *ckafka.Message,
// so clearing its Opaque leaves the original's set: the closure ends the span
// at most once, however many times it is called.
func (p *Producer) prepareMessage(ctx context.Context, topic string, msg *Message) *ckafka.Message {
	km := p.buildKafkaMessage(topic, msg)
	if p.tracer != nil {
		msgCtx, end := p.tracer.startProducerSpan(ctx, topic, msg)
		p.tracer.injectTraceContext(msgCtx, km)
		var once sync.Once
		km.Opaque = func(err error) { once.Do(func() { end(err) }) }
	}
	return km
}

// produceAndAwait queues msgs on librdkafka, retrying a full queue by draining
// delivery reports, and returns once every queued message has reported (or
// ctx is done). The errors.Join of all failures is the result.
func (p *Producer) produceAndAwait(ctx context.Context, msgs []*ckafka.Message) error {
	if p.closed.Load() {
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
			endSpan(m, ctx.Err()) // no-op for a message whose report already ended its span
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
// delivery error, if any. An event that is not a message report confirms
// nothing and is an error.
func finishDelivery(e ckafka.Event) error {
	m, ok := e.(*ckafka.Message)
	if !ok {
		return fmt.Errorf("delivery: unexpected event %T: %v", e, e)
	}
	if err := m.TopicPartition.Error; err != nil {
		endSpan(m, err)
		return fmt.Errorf("delivery failed for topic %s partition %d: %w",
			topicOf(m), m.TopicPartition.Partition, err)
	}
	endSpan(m, nil)
	return nil
}

// endSpan ends the producer span stashed in m.Opaque, recording err on it (nil
// for success). The closure itself ends its span at most once (see
// prepareMessage); clearing m.Opaque only spares this message a second call.
func endSpan(m *ckafka.Message, err error) {
	if end, ok := m.Opaque.(func(error)); ok {
		end(err)
		m.Opaque = nil
	}
}

// topicOf returns the message's topic, or "" when it has none.
func topicOf(m *ckafka.Message) string {
	if m.TopicPartition.Topic == nil {
		return ""
	}
	return *m.TopicPartition.Topic
}

// ProduceAsync queues msg on librdkafka's internal queue and returns
// immediately, without waiting for a delivery report; librdkafka batches the
// queued messages (linger.ms / batch.num.messages). A successful return only
// means the message was queued. Delivery failures reported later go to the
// handler set with ProducerWithDeliveryErrorHandler, or are logged when there
// is none.
//
// There is no back-pressure: when the local queue is full, ProduceAsync returns
// the queue-full error immediately and the caller decides whether to retry.
// Unlike Produce it does not start a trace span or inject trace context.
// Calling it after Close returns ErrProducerClosed, and it must not be called
// concurrently with Close.
func (p *Producer) ProduceAsync(topic string, msg *Message) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}
	return p.producer.Produce(p.buildKafkaMessage(topic, msg), nil)
}

// Flush waits up to timeout for every queued message (including ProduceAsync
// ones) to be delivered. It returns an error stating how many messages are
// still queued when the timeout expires, and nil when the queue drained.
// Calling it after Close returns ErrProducerClosed (Close already flushed), and
// it must not be called concurrently with Close.
func (p *Producer) Flush(timeout time.Duration) error {
	if p.closed.Load() {
		return ErrProducerClosed // the librdkafka handle is gone; touching it faults
	}
	remaining := p.producer.Flush(int(timeout.Milliseconds()))
	if remaining > 0 {
		return fmt.Errorf("%d messages still in queue after flush", remaining)
	}
	return nil
}

// closeFlushTimeout bounds the final flush in Close. A variable so tests can
// shorten it.
var closeFlushTimeout = 10 * time.Second

// Close flushes outstanding messages and closes the producer. The flush waits
// up to 10 seconds; if messages are still undelivered after that, Close logs a
// warning with the number left behind and closes anyway, so those messages are
// lost. It waits for the delivery-report goroutine to exit before releasing the
// librdkafka handle, and always returns nil. Close is safe to call more than
// once (later calls do nothing), but it must not be called concurrently with
// Produce, ProduceBatch or ProduceAsync.
func (p *Producer) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Flush while the reader goroutine is still consuming Events(): Len()
	// counts unread delivery reports, so stopping the reader first would
	// stall Flush for the full timeout and drop async delivery failures.
	if remaining := p.producer.Flush(int(closeFlushTimeout.Milliseconds())); remaining > 0 {
		p.logger.Warn("producer closed with undelivered messages", "remaining", remaining)
	}
	close(p.done)
	p.wg.Wait() // let the report reader exit before closing the producer under it
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
