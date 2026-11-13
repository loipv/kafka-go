package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// maxDLQErrorBytes caps the x-dlq-error-message header value.
const maxDLQErrorBytes = 1024

// truncateUTF8 returns the longest prefix of s that is at most limit bytes and
// ends on a rune boundary. It returns s unchanged when it already fits.
func truncateUTF8(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// dlqService handles Dead Letter Queue operations
type dlqService struct {
	producer *ckafka.Producer
	config   *DLQConfig
	metrics  *metricsCollector
	closed   atomic.Bool // true once closed
}

// dlqDeliveryTimeout bounds how long the poll loop waits for a DLQ delivery
// report unless the DLQ connection's Raw config sets message.timeout.ms
// (or its alias delivery.timeout.ms).
const dlqDeliveryTimeout = 30 * time.Second

// dlqProducerConfig builds the DLQ producer's librdkafka config. The DLQ send
// runs inside the consumer's poll loop, so message.timeout.ms is capped at
// dlqDeliveryTimeout (rather than librdkafka's 5 minutes) unless Raw sets
// message.timeout.ms or its alias delivery.timeout.ms.
func dlqProducerConfig(cc connConfig) ckafka.ConfigMap {
	cm := cc.configMap()
	cm["acks"] = -1 // All replicas
	_, hasMessage := cm["message.timeout.ms"]
	_, hasAlias := cm["delivery.timeout.ms"] // librdkafka alias of message.timeout.ms
	if !hasMessage && !hasAlias {
		cm["message.timeout.ms"] = int(dlqDeliveryTimeout.Milliseconds())
	}
	return cm
}

// newDLQService creates a new DLQ service. It builds its producer from the
// same connConfig as the parent consumer, so SSL/SASL is never dropped.
func newDLQService(cc connConfig, config *DLQConfig, metrics *metricsCollector) (*dlqService, error) {
	cm := dlqProducerConfig(cc)

	producer, err := ckafka.NewProducer(&cm)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLQ producer: %w", err)
	}

	return &dlqService{
		producer: producer,
		config:   config,
		metrics:  metrics,
	}, nil
}

// produceToDLQ sends one failed message to the DLQ topic; see produceAll.
func (s *dlqService) produceToDLQ(ctx context.Context, msg *Message, err error, attempts int) error {
	return s.produceAll(ctx, []*Message{msg}, err, attempts)
}

// buildDLQMessage copies msg into a DLQ record. The caller's Message is never
// mutated — the x-dlq-* headers go on a clone. The delivery report echoes the
// source topic back through Opaque so metrics can be keyed by it.
func (s *dlqService) buildDLQMessage(msg *Message, cause error, attempts int) *ckafka.Message {
	out := *msg
	out.Headers = maps.Clone(msg.Headers) // nil-safe

	out.SetHeader("x-dlq-original-topic", []byte(msg.Topic))
	out.SetHeader("x-dlq-timestamp", appendTime(nil, time.Now()))
	out.SetHeader("x-dlq-handler-retry-count", []byte(strconv.Itoa(attempts))) // the real count
	if s.config.IncludeErrorInfo && cause != nil {
		out.SetHeader("x-dlq-error-message", []byte(truncateUTF8(cause.Error(), maxDLQErrorBytes)))
	}

	dlqTopic := s.config.Topic
	kafkaMsg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &dlqTopic,
			Partition: ckafka.PartitionAny,
		},
		Key:    out.Key,
		Value:  out.Value,
		Opaque: msg.Topic,
	}
	for k, v := range out.Headers {
		kafkaMsg.Headers = append(kafkaMsg.Headers, ckafka.Header{Key: k, Value: v})
	}
	return kafkaMsg
}

// produceAll sends msgs to the DLQ topic: it queues every message first, then
// awaits one delivery report per queued message, so a batch costs one round
// trip instead of len(msgs). Each message is a single produce attempt confirmed
// by its delivery report; the sent-to-DLQ metric counts confirmed messages only,
// keyed by source topic. It returns the errors.Join of every failure and nil
// only when all messages were confirmed; the caller decides what a failure
// means (the partition blocks).
func (s *dlqService) produceAll(ctx context.Context, msgs []*Message, cause error, attempts int) error {
	if s.closed.Load() {
		return ErrDLQClosed
	}
	deliveryChan := make(chan ckafka.Event, len(msgs))
	var errs []error
	queued := 0
	for _, msg := range msgs {
		if perr := s.producer.Produce(s.buildDLQMessage(msg, cause, attempts), deliveryChan); perr != nil {
			errs = append(errs, fmt.Errorf("failed to produce to DLQ: %w", perr))
			continue
		}
		queued++
	}

	for range queued {
		select {
		case e := <-deliveryChan:
			source, err := confirmDLQEvent(e)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			s.metrics.incSentToDLQ(source)
		case <-ctx.Done():
			// Unconfirmed messages are not counted; their reports land in the
			// buffered channel and are dropped with it.
			errs = append(errs, fmt.Errorf("DLQ delivery interrupted: %w", ctx.Err()))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// confirmDLQEvent turns one delivery event into the confirmed message's source
// topic (metrics are keyed by it, not by the DLQ topic) or an error.
func confirmDLQEvent(e ckafka.Event) (string, error) {
	m, ok := e.(*ckafka.Message)
	if !ok {
		// Fail safe: an unconfirmed message must block its partition and retry.
		return "", fmt.Errorf("DLQ delivery: unexpected event %T: %v", e, e)
	}
	if m.TopicPartition.Error != nil {
		return "", fmt.Errorf("DLQ delivery failed: %w", m.TopicPartition.Error)
	}
	source, _ := m.Opaque.(string)
	return source, nil
}

// Close closes the DLQ service
func (s *dlqService) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.producer.Flush(10000)
	s.producer.Close()
	return nil
}

// metricsCollector collects handler-failure and DLQ metrics
type metricsCollector struct {
	global  TopicMetrics
	byTopic map[string]*TopicMetrics
	blocked []TopicPartition
	mu      sync.RWMutex
}

// newMetricsCollector creates a new metrics collector
func newMetricsCollector() *metricsCollector {
	return &metricsCollector{
		byTopic: make(map[string]*TopicMetrics),
	}
}

// getOrCreateTopicMetrics returns existing topic metrics or creates new one
// Uses double-checked locking for optimal performance
func (m *metricsCollector) getOrCreateTopicMetrics(topic string) *TopicMetrics {
	// Fast path: check with RLock (most common case - topic already exists)
	m.mu.RLock()
	metrics, ok := m.byTopic[topic]
	m.mu.RUnlock()

	if ok {
		return metrics
	}

	// Slow path: create with Lock (rare case - first message for this topic)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if metrics, ok = m.byTopic[topic]; ok {
		return metrics
	}

	metrics = &TopicMetrics{}
	m.byTopic[topic] = metrics
	return metrics
}

// incHandlerFailures counts one failed handler invocation for topic.
func (m *metricsCollector) incHandlerFailures(topic string) {
	atomic.AddInt64(&m.global.HandlerFailures, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.HandlerFailures, 1)
}

// incSentToDLQ counts one confirmed DLQ produce for the source topic.
func (m *metricsCollector) incSentToDLQ(topic string) {
	atomic.AddInt64(&m.global.SentToDLQ, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.SentToDLQ, 1)
}

// setBlocked stores an atomic snapshot of the currently blocked partitions.
func (m *metricsCollector) setBlocked(tps []TopicPartition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocked = tps
}

// snapshot returns a copy of all metrics.
func (m *metricsCollector) snapshot() *Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	byTopic := make(map[string]TopicMetrics)
	for k, v := range m.byTopic {
		byTopic[k] = TopicMetrics{
			HandlerFailures: atomic.LoadInt64(&v.HandlerFailures),
			SentToDLQ:       atomic.LoadInt64(&v.SentToDLQ),
		}
	}

	return &Metrics{
		HandlerFailures:   atomic.LoadInt64(&m.global.HandlerFailures),
		SentToDLQ:         atomic.LoadInt64(&m.global.SentToDLQ),
		ByTopic:           byTopic,
		BlockedPartitions: append([]TopicPartition(nil), m.blocked...),
	}
}

// idempotencyStore remembers processed idempotency keys for one TTL. It is
// per-process memory only: it is lost on restart or when a partition is
// rebalanced to another instance, a failed batch is redelivered whole and
// unfiltered, and memory grows with the number of distinct keys seen within
// one TTL.
//
// ponytail: unbounded map, one write lock per sweep — cap with an LRU or move to an external store if key volume per TTL gets large
type idempotencyStore struct {
	store     map[string]time.Time
	ttl       time.Duration
	mu        sync.RWMutex
	done      chan struct{}
	ticker    *time.Ticker
	closeOnce sync.Once
}

// newIdempotencyStore creates a new idempotency store
func newIdempotencyStore(ttl time.Duration) *idempotencyStore {
	if ttl <= 0 { // time.NewTicker(0) panics — clamp instead
		ttl = defaultIdempotencyTTL
	}
	s := &idempotencyStore{
		store:  make(map[string]time.Time),
		ttl:    ttl,
		done:   make(chan struct{}),
		ticker: time.NewTicker(ttl / 10), // Cleanup every 1/10 of TTL
	}

	go s.cleanup()

	return s
}

// add adds a key to the store
func (s *idempotencyStore) add(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = time.Now()
}

// isDuplicate checks if a key is a duplicate
func (s *idempotencyStore) isDuplicate(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if ts, ok := s.store[key]; ok {
		if time.Since(ts) < s.ttl {
			return true
		}
	}
	return false
}

// close closes the idempotency store. Safe to call more than once.
func (s *idempotencyStore) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.ticker.Stop()
	})
}

// cleanup periodically removes expired entries
func (s *idempotencyStore) cleanup() {
	for {
		select {
		case <-s.done:
			return
		case <-s.ticker.C:
			s.mu.Lock()
			now := time.Now()
			for key, ts := range s.store {
				if now.Sub(ts) >= s.ttl {
					delete(s.store, key)
				}
			}
			s.mu.Unlock()
		}
	}
}
