package kafka

import (
	"context"
	"sync"
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

// MessagePool provides object pooling for Message structs to reduce GC pressure.
// Use this for high-throughput scenarios where message allocation is a bottleneck.
//
// Usage:
//
//	pool := kafka.NewMessagePool()
//	msg := pool.Get()
//	// ... use msg ...
//	pool.Put(msg)  // Return to pool when done
//
// IMPORTANT: Only put messages back that were obtained from the same pool.
// Never put a message back if it's still referenced elsewhere.
type MessagePool struct {
	pool sync.Pool
}

// NewMessagePool creates a new message pool
func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Message{}
			},
		},
	}
}

// Get retrieves a Message from the pool or creates a new one
func (p *MessagePool) Get() *Message {
	return p.pool.Get().(*Message)
}

// Put returns a Message to the pool after resetting it
// The message will be reset to zero values before being pooled
func (p *MessagePool) Put(msg *Message) {
	if msg == nil {
		return
	}
	// Reset message to avoid data leaks
	msg.Key = nil
	msg.Value = nil
	msg.Headers = nil
	msg.Partition = 0
	msg.Offset = 0
	msg.Timestamp = time.Time{}
	msg.Topic = ""
	p.pool.Put(msg)
}

// GetWithHeaders retrieves a Message from the pool with pre-allocated headers map
func (p *MessagePool) GetWithHeaders(headerCount int) *Message {
	msg := p.pool.Get().(*Message)
	if headerCount > 0 {
		msg.Headers = make(Headers, headerCount)
	}
	return msg
}

// defaultMessagePool is a global pool for internal use
var defaultMessagePool = NewMessagePool()

// AcquireMessage gets a Message from the default pool
// For high-throughput scenarios, prefer creating your own MessagePool
func AcquireMessage() *Message {
	return defaultMessagePool.Get()
}

// ReleaseMessage returns a Message to the default pool
// IMPORTANT: Only call this if the message was obtained via AcquireMessage
// and is no longer referenced anywhere
func ReleaseMessage(msg *Message) {
	defaultMessagePool.Put(msg)
}

// TopicMessages represents messages for a specific topic
type TopicMessages struct {
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
	Status  HealthStatus           `json:"status"`
	Details map[string]interface{} `json:"details,omitempty"`
	Error   error                  `json:"error,omitempty"`
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

// ErrorHandler handles errors during message processing
type ErrorHandler func(err error, msg *Message)

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

// Client interface defines the producer API
type Client interface {
	// Send sends a single message to a topic
	Send(ctx context.Context, topic string, msg *Message) error

	// SendBatch sends multiple messages to a single topic
	SendBatch(ctx context.Context, topic string, msgs []*Message) error

	// SendMultiTopicBatch sends messages to multiple topics
	SendMultiTopicBatch(ctx context.Context, batches []TopicMessages) error

	// SendQueued queues a message for automatic batching
	SendQueued(ctx context.Context, topic string, msg *Message) error

	// Flush waits for all queued messages to be sent
	Flush(timeout time.Duration) error

	// Close closes the client
	Close() error
}

// Consumer interface defines the consumer API
type Consumer interface {
	// Handle registers a handler for single messages
	Handle(handler MessageHandler)

	// HandleBatch registers a handler for batch messages
	HandleBatch(handler BatchHandler)

	// HandleGroupedBatch registers a handler for key-grouped batches
	HandleGroupedBatch(handler GroupedBatchHandler)

	// Start starts consuming messages (blocking)
	Start(ctx context.Context) error

	// Close closes the consumer
	Close(ctx context.Context) error

	// Pause pauses consumption
	Pause()

	// Resume resumes consumption
	Resume()

	// GetDLQMetrics returns DLQ metrics
	GetDLQMetrics() *DLQMetrics

	// GetCircuitState returns circuit breaker state
	GetCircuitState(dlqTopic string) CircuitState

	// ResetCircuit resets the circuit breaker
	ResetCircuit(dlqTopic string)
}

// DLQMetrics represents DLQ metrics
type DLQMetrics struct {
	Global  DLQGlobalMetrics           `json:"global"`
	ByTopic map[string]DLQTopicMetrics `json:"byTopic"`
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

