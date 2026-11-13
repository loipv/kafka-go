package kafka

import (
	"context"
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
	closed   int32 // atomic: 0=open, 1=closed
}

// dlqDeliveryTimeout bounds how long the poll loop waits for a DLQ delivery
// report unless the DLQ connection's Raw config sets message.timeout.ms.
const dlqDeliveryTimeout = 30 * time.Second

// dlqProducerConfig builds the DLQ producer's librdkafka config. The DLQ send
// runs inside the consumer's poll loop, so message.timeout.ms is capped at
// dlqDeliveryTimeout (rather than librdkafka's 5 minutes) unless Raw sets it.
func dlqProducerConfig(cc connConfig) ckafka.ConfigMap {
	cm := cc.configMap()
	cm["acks"] = -1 // All replicas
	if _, ok := cm["message.timeout.ms"]; !ok {
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

// produceToDLQ sends a failed message to the DLQ topic. The caller's Message
// is never mutated — DLQ headers go on a shallow copy. The produce is a
// single attempt confirmed by a delivery report; the caller decides what a
// failure means (the partition blocks).
func (s *dlqService) produceToDLQ(ctx context.Context, msg *Message, err error, attempts int) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return ErrDLQClosed
	}
	out := *msg
	out.Headers = maps.Clone(msg.Headers) // nil-safe

	out.SetHeader("x-dlq-original-topic", []byte(msg.Topic))
	out.SetHeader("x-dlq-timestamp", appendTime(nil, time.Now()))
	out.SetHeader("x-dlq-handler-retry-count", []byte(strconv.Itoa(attempts))) // #9: the real count
	if s.config.IncludeErrorInfo && err != nil {
		out.SetHeader("x-dlq-error-message", []byte(truncateUTF8(err.Error(), maxDLQErrorBytes)))
	}

	dlqTopic := s.config.Topic
	kafkaMsg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &dlqTopic,
			Partition: ckafka.PartitionAny,
		},
		Key:   out.Key,
		Value: out.Value,
	}

	// Add headers
	for k, v := range out.Headers {
		kafkaMsg.Headers = append(kafkaMsg.Headers, ckafka.Header{
			Key:   k,
			Value: v,
		})
	}

	deliveryChan := make(chan ckafka.Event, 1)
	if perr := s.producer.Produce(kafkaMsg, deliveryChan); perr != nil {
		return fmt.Errorf("failed to produce to DLQ: %w", perr)
	}

	// Wait for delivery
	select {
	case e := <-deliveryChan:
		if m, ok := e.(*ckafka.Message); ok && m.TopicPartition.Error != nil {
			return fmt.Errorf("DLQ delivery failed: %w", m.TopicPartition.Error)
		}
		// Metrics are keyed by the source topic, not the DLQ topic.
		s.metrics.incrementSentToDLQ(msg.Topic)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the DLQ service
func (s *dlqService) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.producer.Flush(10000)
	s.producer.Close()
	return nil
}

// metricsCollector collects DLQ metrics
type metricsCollector struct {
	global  DLQGlobalMetrics
	byTopic map[string]*DLQTopicMetrics
	blocked []TopicPartition
	mu      sync.RWMutex
}

// newMetricsCollector creates a new metrics collector
func newMetricsCollector() *metricsCollector {
	return &metricsCollector{
		byTopic: make(map[string]*DLQTopicMetrics),
	}
}

// getOrCreateTopicMetrics returns existing topic metrics or creates new one
// Uses double-checked locking for optimal performance
func (m *metricsCollector) getOrCreateTopicMetrics(topic string) *DLQTopicMetrics {
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

	metrics = &DLQTopicMetrics{}
	m.byTopic[topic] = metrics
	return metrics
}

// incrementHandlerRetries increments handler retry count
func (m *metricsCollector) incrementHandlerRetries(topic string) {
	atomic.AddInt64(&m.global.HandlerRetries, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.HandlerRetries, 1)
}

// incrementSentToDLQ increments sent to DLQ count
func (m *metricsCollector) incrementSentToDLQ(topic string) {
	atomic.AddInt64(&m.global.MessagesSentToDLQ, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.SentToDLQ, 1)
}

// setBlocked stores an atomic snapshot of the currently blocked partitions.
func (m *metricsCollector) setBlocked(tps []TopicPartition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocked = tps
}

// getMetrics returns all metrics
func (m *metricsCollector) getMetrics() *DLQMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	byTopic := make(map[string]DLQTopicMetrics)
	for k, v := range m.byTopic {
		byTopic[k] = DLQTopicMetrics{
			HandlerRetries: atomic.LoadInt64(&v.HandlerRetries),
			SentToDLQ:      atomic.LoadInt64(&v.SentToDLQ),
		}
	}

	return &DLQMetrics{
		Global: DLQGlobalMetrics{
			HandlerRetries:    atomic.LoadInt64(&m.global.HandlerRetries),
			MessagesSentToDLQ: atomic.LoadInt64(&m.global.MessagesSentToDLQ),
		},
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
