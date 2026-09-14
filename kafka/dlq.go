package kafka

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// DLQService handles Dead Letter Queue operations
type DLQService struct {
	producer *ckafka.Producer
	config   *DLQConfig
	metrics  *DLQMetricsCollector
	closed   int32 // atomic: 0=open, 1=closed
	after    func(time.Duration) <-chan time.Time
}

// newDLQService creates a new DLQ service. It builds its producer from the
// same connConfig as the parent consumer, so SSL/SASL is never dropped.
func newDLQService(cc connConfig, config *DLQConfig, metrics *DLQMetricsCollector) (*DLQService, error) {
	cm := cc.configMap()
	cm["acks"] = -1 // All replicas

	producer, err := ckafka.NewProducer(&cm)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLQ producer: %w", err)
	}

	return &DLQService{
		producer: producer,
		config:   config,
		metrics:  metrics,
		after:    time.After,
	}, nil
}

// produceToDLQ sends a failed message to the DLQ topic. The caller's Message
// is never mutated — DLQ headers go on a shallow copy. The produce is
// confirmed by a delivery report and retried per DLQConfig before giving up.
func (s *DLQService) produceToDLQ(ctx context.Context, msg *Message, err error, attempts int) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return ErrDLQClosed
	}
	out := *msg
	out.Headers = maps.Clone(msg.Headers) // nil-safe

	out.SetHeader("x-dlq-original-topic", []byte(msg.Topic))
	out.SetHeader("x-dlq-timestamp", appendTime(nil, time.Now()))
	out.SetHeader("x-dlq-handler-retry-count", []byte(strconv.Itoa(attempts))) // #9: the real count
	if s.config.IncludeErrorInfo && err != nil {
		out.SetHeader("x-dlq-error-message", []byte(err.Error()))
	}

	return s.produceWithRetry(ctx, s.config.Topic, &out)
}

// produceWithRetry retries the DLQ produce per DLQConfig.MaxRetries /
// RetryDelay / RetryBackoffMultiplier — a failed produce used to drop the
// message after one attempt (#10-adjacent gap).
func (s *DLQService) produceWithRetry(ctx context.Context, topic string, msg *Message) error {
	maxRetries, delay := s.config.MaxRetries, s.config.RetryDelay
	var lastErr error
	for attempt := 0; ; attempt++ {
		lastErr = s.produceToTopic(ctx, topic, msg)
		if lastErr == nil {
			return nil
		}
		if attempt >= maxRetries {
			return fmt.Errorf("dlq produce failed after %d attempts: %w", attempt+1, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.after(delay):
			delay = time.Duration(float64(delay) * s.config.RetryBackoffMultiplier)
		}
	}
}

// produceToTopic sends a message to a specific topic
func (s *DLQService) produceToTopic(ctx context.Context, topic string, msg *Message) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return ErrDLQClosed
	}

	kafkaMsg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Key:   msg.Key,
		Value: msg.Value,
	}

	// Add headers
	for k, v := range msg.Headers {
		kafkaMsg.Headers = append(kafkaMsg.Headers, ckafka.Header{
			Key:   k,
			Value: v,
		})
	}

	deliveryChan := make(chan ckafka.Event, 1)
	err := s.producer.Produce(kafkaMsg, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce to DLQ: %w", err)
	}

	// Wait for delivery
	select {
	case e := <-deliveryChan:
		if m, ok := e.(*ckafka.Message); ok && m.TopicPartition.Error != nil {
			return fmt.Errorf("DLQ delivery failed: %w", m.TopicPartition.Error)
		}
		s.metrics.IncrementSentToDLQ(topic)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the DLQ service
func (s *DLQService) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.producer.Flush(10000)
	s.producer.Close()
	return nil
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config          *CircuitBreakerConfig
	state           CircuitState
	failures        int
	successes       int
	lastFailureTime time.Time
	now             func() time.Time // seam for tests; defaults to time.Now
	mu              sync.Mutex       // Use Mutex instead of RWMutex to avoid lock upgrade issues
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
		now:    time.Now,
	}
}

// State returns the current circuit state
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Check if we should transition from open to half-open
	if cb.state == CircuitOpen {
		if cb.now().Sub(cb.lastFailureTime) >= cb.config.Timeout {
			cb.state = CircuitHalfOpen
			cb.successes = 0
		}
	}

	return cb.state
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.successes++

	switch cb.state {
	case CircuitHalfOpen:
		if cb.successes >= cb.config.SuccessThreshold {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.successes = 0
		}
	case CircuitClosed:
		// Reset failures on success
		cb.failures = 0
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = cb.now()

	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
		}
	case CircuitHalfOpen:
		// Any failure in half-open goes back to open
		cb.state = CircuitOpen
		cb.successes = 0
	}
}

// Reset resets the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = CircuitClosed
	cb.failures = 0
	cb.successes = 0
}

// IsOpen returns true if circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.State() == CircuitOpen
}

// IsClosed returns true if circuit is closed
func (cb *CircuitBreaker) IsClosed() bool {
	return cb.State() == CircuitClosed
}

// DLQMetricsCollector collects DLQ metrics
type DLQMetricsCollector struct {
	global  DLQGlobalMetrics
	byTopic map[string]*DLQTopicMetrics
	blocked []TopicPartition
	mu      sync.RWMutex
}

// NewDLQMetricsCollector creates a new metrics collector
func NewDLQMetricsCollector() *DLQMetricsCollector {
	return &DLQMetricsCollector{
		byTopic: make(map[string]*DLQTopicMetrics),
	}
}

// getOrCreateTopicMetrics returns existing topic metrics or creates new one
// Uses double-checked locking for optimal performance
func (m *DLQMetricsCollector) getOrCreateTopicMetrics(topic string) *DLQTopicMetrics {
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

// IncrementHandlerRetries increments handler retry count
func (m *DLQMetricsCollector) IncrementHandlerRetries(topic string) {
	atomic.AddInt64(&m.global.HandlerRetries, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.HandlerRetries, 1)
}

// IncrementSentToDLQ increments sent to DLQ count
func (m *DLQMetricsCollector) IncrementSentToDLQ(topic string) {
	atomic.AddInt64(&m.global.MessagesSentToDLQ, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.SentToDLQ, 1)
}

// IncrementReprocessAttempts increments reprocess attempts
func (m *DLQMetricsCollector) IncrementReprocessAttempts(topic string) {
	atomic.AddInt64(&m.global.ReprocessAttempts, 1)
	metrics := m.getOrCreateTopicMetrics(topic)
	atomic.AddInt64(&metrics.ReprocessAttempts, 1)
}

// IncrementReprocessSuccesses increments reprocess successes
func (m *DLQMetricsCollector) IncrementReprocessSuccesses() {
	atomic.AddInt64(&m.global.ReprocessSuccesses, 1)
}

// IncrementReprocessFailures increments reprocess failures
func (m *DLQMetricsCollector) IncrementReprocessFailures() {
	atomic.AddInt64(&m.global.ReprocessFailures, 1)
}

// SetBlocked stores an atomic snapshot of the currently blocked partitions.
func (m *DLQMetricsCollector) SetBlocked(tps []TopicPartition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocked = tps
}

// GetMetrics returns all metrics
func (m *DLQMetricsCollector) GetMetrics() *DLQMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	byTopic := make(map[string]DLQTopicMetrics)
	for k, v := range m.byTopic {
		byTopic[k] = DLQTopicMetrics{
			HandlerRetries:    atomic.LoadInt64(&v.HandlerRetries),
			SentToDLQ:         atomic.LoadInt64(&v.SentToDLQ),
			ReprocessAttempts: atomic.LoadInt64(&v.ReprocessAttempts),
		}
	}

	return &DLQMetrics{
		Global: DLQGlobalMetrics{
			HandlerRetries:     atomic.LoadInt64(&m.global.HandlerRetries),
			MessagesSentToDLQ:  atomic.LoadInt64(&m.global.MessagesSentToDLQ),
			ReprocessAttempts:  atomic.LoadInt64(&m.global.ReprocessAttempts),
			ReprocessSuccesses: atomic.LoadInt64(&m.global.ReprocessSuccesses),
			ReprocessFailures:  atomic.LoadInt64(&m.global.ReprocessFailures),
		},
		ByTopic:           byTopic,
		BlockedPartitions: append([]TopicPartition(nil), m.blocked...),
	}
}

// ResetMetrics resets all metrics
func (m *DLQMetricsCollector) ResetMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	atomic.StoreInt64(&m.global.HandlerRetries, 0)
	atomic.StoreInt64(&m.global.MessagesSentToDLQ, 0)
	atomic.StoreInt64(&m.global.ReprocessAttempts, 0)
	atomic.StoreInt64(&m.global.ReprocessSuccesses, 0)
	atomic.StoreInt64(&m.global.ReprocessFailures, 0)

	m.blocked = nil
	m.byTopic = make(map[string]*DLQTopicMetrics)
}

// IdempotencyStore stores processed message keys for idempotency
type IdempotencyStore struct {
	store     map[string]time.Time
	ttl       time.Duration
	mu        sync.RWMutex
	done      chan struct{}
	ticker    *time.Ticker
	closeOnce sync.Once
}

// NewIdempotencyStore creates a new idempotency store
func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	if ttl <= 0 { // time.NewTicker(0) panics — clamp instead
		ttl = DefaultIdempotencyTTL
	}
	s := &IdempotencyStore{
		store:  make(map[string]time.Time),
		ttl:    ttl,
		done:   make(chan struct{}),
		ticker: time.NewTicker(ttl / 10), // Cleanup every 1/10 of TTL
	}

	go s.cleanup()

	return s
}

// Add adds a key to the store
func (s *IdempotencyStore) Add(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = time.Now()
}

// IsDuplicate checks if a key is a duplicate
func (s *IdempotencyStore) IsDuplicate(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if ts, ok := s.store[key]; ok {
		if time.Since(ts) < s.ttl {
			return true
		}
	}
	return false
}

// Remove removes a key from the store
func (s *IdempotencyStore) Remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, key)
}

// Close closes the idempotency store. Safe to call more than once.
func (s *IdempotencyStore) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.ticker.Stop()
	})
}

// Size returns the number of keys in the store
func (s *IdempotencyStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.store)
}

// cleanup periodically removes expired entries
func (s *IdempotencyStore) cleanup() {
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
