package kafka

import (
	"sync"
	"sync/atomic"
)

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
