package kafka

import (
	"context"
	"time"
)

// Headers maps header keys to values. A Kafka message may carry the same header
// key more than once; on consume the last occurrence wins, and on produce each
// key is sent once, in no particular order.
type Headers map[string][]byte

// Message is a Kafka message, used both for producing and for consuming.
type Message struct {
	// Key is the message key. It selects the partition when producing. May be
	// nil.
	Key []byte
	// Value is the message payload. May be nil.
	Value []byte
	// Headers are the message headers. Nil when a consumed message has none.
	Headers Headers
	// Partition and Offset are only populated on the consume side; they are
	// ignored when producing.
	Partition int32
	Offset    int64
	// Timestamp is the message timestamp. When producing, the zero value lets
	// librdkafka use the current time. When consuming, it is the timestamp the
	// broker or producer recorded.
	Timestamp time.Time
	// Topic is the source topic on the consume side. It is ignored when
	// producing: the topic argument of Produce, ProduceBatch and ProduceAsync
	// decides.
	Topic string
}

// SetHeader sets the header key to value, replacing any existing value for the
// key and allocating the Headers map when it is nil, so it is safe on a zero
// Message.
func (m *Message) SetHeader(key string, value []byte) {
	if m.Headers == nil {
		m.Headers = make(Headers, 1)
	}
	m.Headers[key] = value
}

// GroupedBatch is the messages of one batch that share a key, as passed to an
// OnGroupedBatch handler.
type GroupedBatch struct {
	// Key is the message key shared by the group, converted to a string. Messages
	// without a key form one group with the empty Key.
	Key string
	// Messages are the group's messages in the order they were received.
	Messages []*Message
}

// Acks is the number of broker acknowledgments a produced message needs before
// it counts as delivered (librdkafka's acks). The values match librdkafka's.
type Acks int

const (
	// AcksNone does not wait for any acknowledgment: a message counts as
	// delivered once it is sent, and may be lost without an error.
	AcksNone Acks = 0
	// AcksLeader waits for the partition leader only; a message can still be
	// lost if the leader fails before replicating it.
	AcksLeader Acks = 1
	// AcksAll waits for all in-sync replicas. It is the producer default and is
	// required for an idempotent producer.
	AcksAll Acks = -1
)

// Compression is a message compression codec. It is a string holding one of
// librdkafka's compression.type values. The empty value and CompressionNone both
// mean no compression; any other value that librdkafka does not support is
// rejected by NewProducer.
type Compression string

const (
	// CompressionNone disables compression.
	CompressionNone Compression = "none"
	// CompressionGZIP compresses with gzip.
	CompressionGZIP Compression = "gzip"
	// CompressionSnappy compresses with Snappy.
	CompressionSnappy Compression = "snappy"
	// CompressionLZ4 compresses with LZ4.
	CompressionLZ4 Compression = "lz4"
	// CompressionZSTD compresses with Zstandard.
	CompressionZSTD Compression = "zstd"
)

// PartitionAssignor is a consumer group partition assignment strategy. Its
// values are librdkafka's partition.assignment.strategy names.
type PartitionAssignor string

const (
	// AssignorRange assigns partitions topic by topic in contiguous ranges. It is
	// the consumer default.
	AssignorRange PartitionAssignor = "range"
	// AssignorRoundRobin spreads partitions across the group members one by one.
	AssignorRoundRobin PartitionAssignor = "roundrobin"
	// AssignorCooperativeSticky keeps existing assignments stable and rebalances
	// incrementally, so only the moved partitions are revoked.
	AssignorCooperativeSticky PartitionAssignor = "cooperative-sticky"
)

// HealthStatus is the outcome of a health check.
type HealthStatus string

const (
	// HealthStatusUp means the check passed.
	HealthStatusUp HealthStatus = "UP"
	// HealthStatusDown means the check failed; see HealthResult.Error and Details.
	HealthStatusDown HealthStatus = "DOWN"
)

// HealthResult is the result of a HealthChecker check. It marshals to JSON with
// the lower-case keys shown on its fields.
type HealthResult struct {
	// Status is HealthStatusUp or HealthStatusDown.
	Status HealthStatus `json:"status"`
	// Details holds check-specific data; the HealthChecker method documents its
	// keys. It may be empty or nil, notably when the check failed early.
	Details map[string]any `json:"details,omitempty"`
	// Error describes why the check is down. It is empty when Status is up, and
	// also when a lag check is down only because the lag exceeds the limit.
	Error string `json:"error,omitzero"`
}

// Handler types

// MessageHandler handles a single message; register it with Consumer.OnMessage.
// Returning nil parks the message (its offset may be stored); returning an
// error starts the retry, error handler and DLQ flow. The context is derived
// from the one passed to Consumer.Start.
type MessageHandler func(ctx context.Context, msg *Message) error

// BatchHandler handles a batch of messages; register it with Consumer.OnBatch.
// The messages are in consumption order and may come from several partitions.
// Returning nil parks the whole batch; returning an error fails the whole
// batch. If an idempotency key is configured, duplicates are removed before the
// call, so the slice can be shorter than the number of messages consumed.
type BatchHandler func(ctx context.Context, msgs []*Message) error

// GroupedBatchHandler handles key-grouped batches; register it with
// Consumer.OnGroupedBatch. Each GroupedBatch holds the messages of one key in
// consumption order. Success and failure apply to the whole batch, as for
// BatchHandler, and duplicates are removed before grouping.
type GroupedBatchHandler func(ctx context.Context, groups []GroupedBatch) error

// ErrorHandler is called with a message the consumer could not process and the
// error that failed it; register it with ConsumerWithErrorHandler. Returning
// nil means the handler took ownership of the message: it is parked (its offset
// may advance) and the DLQ is not used. Returning an error defers to the DLQ
// or, without one, to blocking the partition. In batch mode it is called once
// for each message of the failed batch. With SkipOnMaxRetries the message is
// parked whatever it returns. It is not called for a message or batch whose
// handling was interrupted by shutdown (the Start context ended, or Close was
// called during a retry wait); that message is redelivered instead.
type ErrorHandler func(ctx context.Context, msg *Message, err error) error

// IdempotencyKeyFunc returns the idempotency key of a message; set it with
// ConsumerWithIdempotencyKey. Return an empty string for a message that must
// never be treated as a duplicate. It must be cheap and must not modify the
// message.
type IdempotencyKeyFunc func(msg *Message) string

// RebalanceType says whether a rebalance event assigned or revoked partitions.
type RebalanceType string

const (
	// RebalanceAssigned means partitions were assigned to this consumer.
	RebalanceAssigned RebalanceType = "assigned"
	// RebalanceRevoked means partitions were revoked from this consumer.
	RebalanceRevoked RebalanceType = "revoked"
)

// RebalanceEvent is delivered to the RebalanceCallback when partitions are
// assigned to or revoked from this consumer.
type RebalanceEvent struct {
	// Type is RebalanceAssigned or RebalanceRevoked
	Type RebalanceType
	// Partitions contains the affected topic-partitions
	Partitions []TopicPartition
}

// TopicPartition identifies a partition of a topic.
type TopicPartition struct {
	// Topic is the topic name.
	Topic string
	// Partition is the partition number.
	Partition int32
	// Offset is the offset librdkafka reports for the partition in a
	// RebalanceEvent (often a logical value such as "unset", not a message
	// position). It is not populated in Metrics.BlockedPartitions.
	Offset int64
}

// RebalanceCallback is called when partitions are assigned or revoked; set it
// with ConsumerWithRebalanceCallback. It runs on whichever goroutine is polling
// the client: normally the one running Consumer.Start, and the one calling
// Consumer.Close while the client shuts down. A non-nil return is logged and makes the library skip its own
// handling of that event (use with caution).
type RebalanceCallback func(event RebalanceEvent) error

// Metrics is a snapshot of the consumer's handler-failure and DLQ counters,
// returned by Consumer.Metrics. The counters only grow, over the life of the
// Consumer; BlockedPartitions is the state at the time of the snapshot.
type Metrics struct {
	// HandlerFailures counts failed handler invocations per message. With a
	// single-message handler every failed attempt counts, including the first,
	// not only the retries after it. With a batch handler a failed batch counts
	// once for each message handed to the handler. A handler error returned
	// after the Start context ended is shutdown, not a failure, and is not
	// counted. It equals the sum of the per-topic values in ByTopic.
	HandlerFailures int64 `json:"handlerFailures"`
	// SentToDLQ counts messages whose DLQ produce was confirmed.
	SentToDLQ int64 `json:"sentToDlq"`
	// ByTopic breaks both counters down by source topic, keyed by the topic the
	// message was consumed from (not the DLQ topic). Topics with no failures
	// and no DLQ sends may be absent.
	ByTopic map[string]TopicMetrics `json:"byTopic"`
	// BlockedPartitions is a snapshot, not a counter: the partitions blocked on
	// an unparkable message at the time of the call. It shrinks as partitions
	// recover or are revoked.
	BlockedPartitions []TopicPartition `json:"blockedPartitions,omitempty"`
}

// TopicMetrics holds the counters for one source topic; Metrics.ByTopic maps a
// topic name to it. The counters are keyed by the topic the message was
// consumed from.
type TopicMetrics struct {
	// HandlerFailures counts failed handler invocations for messages from this
	// topic: with a single-message handler every failed attempt, including the
	// first; with a batch handler one per message of a failed batch.
	HandlerFailures int64 `json:"handlerFailures"`
	// SentToDLQ counts messages from this topic whose DLQ produce was confirmed.
	SentToDLQ int64 `json:"sentToDlq"`
}
