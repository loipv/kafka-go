package kafka

import (
	"context"
	"time"
)

// Headers is a map of header key-value pairs
type Headers map[string][]byte

// Message represents a Kafka message
type Message struct {
	Key       []byte
	Value     []byte
	Headers   Headers
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

// TopicBatch represents messages for a specific topic
type TopicBatch struct {
	Topic    string
	Messages []*Message
}

// GroupedBatch represents messages grouped by key
type GroupedBatch struct {
	Key      string
	Messages []*Message
}

// PartitionAny represents any partition
const PartitionAny int32 = -1

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

// Compression types for message compression
type Compression int

const (
	// CompressionNone - No compression
	CompressionNone Compression = 0
	// CompressionGZIP - GZIP compression
	CompressionGZIP Compression = 1
	// CompressionSnappy - Snappy compression
	CompressionSnappy Compression = 2
	// CompressionLZ4 - LZ4 compression
	CompressionLZ4 Compression = 3
	// CompressionZSTD - ZSTD compression
	CompressionZSTD Compression = 4
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

// CircuitState represents circuit breaker states
type CircuitState string

const (
	// CircuitClosed - Normal operation
	CircuitClosed CircuitState = "CLOSED"
	// CircuitOpen - DLQ blocked (failure threshold exceeded)
	CircuitOpen CircuitState = "OPEN"
	// CircuitHalfOpen - Testing recovery
	CircuitHalfOpen CircuitState = "HALF_OPEN"
)

// LogLevel represents logging level
type LogLevel int

const (
	// LogLevelNone - No logging
	LogLevelNone LogLevel = 0
	// LogLevelError - Error level
	LogLevelError LogLevel = 1
	// LogLevelWarn - Warning level
	LogLevelWarn LogLevel = 2
	// LogLevelInfo - Info level
	LogLevelInfo LogLevel = 3
	// LogLevelDebug - Debug level
	LogLevelDebug LogLevel = 4
)

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

// RebalanceEvent represents a partition rebalance event
type RebalanceEvent struct {
	// Type is either "assigned" or "revoked"
	Type string
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

// DLQMetrics represents DLQ metrics
type DLQMetrics struct {
	Global            DLQGlobalMetrics           `json:"global"`
	ByTopic           map[string]DLQTopicMetrics `json:"byTopic"`
	BlockedPartitions []TopicPartition           `json:"blockedPartitions,omitempty"`
}

// DLQGlobalMetrics represents global DLQ metrics
type DLQGlobalMetrics struct {
	HandlerRetries     int64 `json:"handlerRetries"`
	MessagesSentToDLQ  int64 `json:"messagesSentToDlq"`
	ReprocessAttempts  int64 `json:"reprocessAttempts"`
	ReprocessSuccesses int64 `json:"reprocessSuccesses"`
	ReprocessFailures  int64 `json:"reprocessFailures"`
}

// DLQTopicMetrics represents per-topic DLQ metrics
type DLQTopicMetrics struct {
	HandlerRetries    int64 `json:"handlerRetries"`
	SentToDLQ         int64 `json:"sentToDlq"`
	ReprocessAttempts int64 `json:"reprocessAttempts"`
}
