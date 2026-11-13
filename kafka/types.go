package kafka

import (
	"context"
	"time"
)

// Headers is a map of header key-value pairs
type Headers map[string][]byte

// Message represents a Kafka message
type Message struct {
	Key     []byte
	Value   []byte
	Headers Headers
	// Partition and Offset are only populated on the consume side; they are
	// ignored when producing.
	Partition int32
	Offset    int64
	Timestamp time.Time
	Topic     string
}

// SetHeader sets a header, lazily allocating the map. Safe on zero Messages.
func (m *Message) SetHeader(key string, value []byte) {
	if m.Headers == nil {
		m.Headers = make(Headers, 1)
	}
	m.Headers[key] = value
}

// GroupedBatch represents messages grouped by key
type GroupedBatch struct {
	Key      string
	Messages []*Message
}

// Acks configuration for producer acknowledgment
type Acks int

const (
	// AcksNone - No acknowledgment
	AcksNone Acks = 0
	// AcksLeader - Leader acknowledgment only
	AcksLeader Acks = 1
	// AcksAll - All replicas acknowledgment
	AcksAll Acks = -1
)

// Compression is a message compression codec. Its values are the
// librdkafka compression.type names; the empty value means no compression.
type Compression string

const (
	// CompressionNone - No compression
	CompressionNone Compression = "none"
	// CompressionGZIP - GZIP compression
	CompressionGZIP Compression = "gzip"
	// CompressionSnappy - Snappy compression
	CompressionSnappy Compression = "snappy"
	// CompressionLZ4 - LZ4 compression
	CompressionLZ4 Compression = "lz4"
	// CompressionZSTD - ZSTD compression
	CompressionZSTD Compression = "zstd"
)

// PartitionAssignor represents partition assignment strategy
type PartitionAssignor string

const (
	// AssignorRange assigns partitions based on ranges
	AssignorRange PartitionAssignor = "range"
	// AssignorRoundRobin assigns partitions in round-robin fashion
	AssignorRoundRobin PartitionAssignor = "roundrobin"
	// AssignorCooperativeSticky uses cooperative rebalancing with sticky assignment
	AssignorCooperativeSticky PartitionAssignor = "cooperative-sticky"
)

// HealthStatus represents health check status
type HealthStatus string

const (
	// HealthStatusUp indicates the service is healthy
	HealthStatusUp HealthStatus = "UP"
	// HealthStatusDown indicates the service is unhealthy
	HealthStatusDown HealthStatus = "DOWN"
)

// HealthResult represents health check result
type HealthResult struct {
	Status  HealthStatus   `json:"status"`
	Details map[string]any `json:"details,omitempty"`
	Error   string         `json:"error,omitzero"`
}

// Handler types

// MessageHandler handles a single message
type MessageHandler func(ctx context.Context, msg *Message) error

// BatchHandler handles a batch of messages
type BatchHandler func(ctx context.Context, msgs []*Message) error

// GroupedBatchHandler handles key-grouped batches
type GroupedBatchHandler func(ctx context.Context, groups []GroupedBatch) error

// ErrorHandler handles errors during message processing. Returning nil means
// the handler took ownership of the message: it is parked (its offset may
// advance). Returning an error defers to the DLQ / block machinery.
type ErrorHandler func(ctx context.Context, msg *Message, err error) error

// IdempotencyKeyFunc extracts idempotency key from message
type IdempotencyKeyFunc func(msg *Message) string

// RebalanceType says whether a rebalance event assigned or revoked partitions.
type RebalanceType string

const (
	// RebalanceAssigned means partitions were assigned to this consumer.
	RebalanceAssigned RebalanceType = "assigned"
	// RebalanceRevoked means partitions were revoked from this consumer.
	RebalanceRevoked RebalanceType = "revoked"
)

// RebalanceEvent represents a partition rebalance event
type RebalanceEvent struct {
	// Type is RebalanceAssigned or RebalanceRevoked
	Type RebalanceType
	// Partitions contains the affected topic-partitions
	Partitions []TopicPartition
}

// TopicPartition represents a topic and partition pair
type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

// RebalanceCallback is called when partitions are assigned or revoked
// Return an error to abort the rebalance (use with caution)
type RebalanceCallback func(event RebalanceEvent) error

// Metrics is a snapshot of the consumer's handler-failure and DLQ counters.
type Metrics struct {
	// HandlerFailures counts every failed handler invocation, including the
	// first attempt, not only the retries after it.
	HandlerFailures int64 `json:"handlerFailures"`
	// SentToDLQ counts messages whose DLQ produce was confirmed.
	SentToDLQ int64 `json:"sentToDlq"`
	// ByTopic breaks both counters down by source topic.
	ByTopic map[string]TopicMetrics `json:"byTopic"`
	// BlockedPartitions lists the partitions currently blocked on an
	// unparkable message.
	BlockedPartitions []TopicPartition `json:"blockedPartitions,omitempty"`
}

// TopicMetrics holds the counters for one source topic.
type TopicMetrics struct {
	// HandlerFailures counts every failed handler invocation, including the
	// first attempt, for messages from this topic.
	HandlerFailures int64 `json:"handlerFailures"`
	// SentToDLQ counts messages from this topic whose DLQ produce was confirmed.
	SentToDLQ int64 `json:"sentToDlq"`
}
