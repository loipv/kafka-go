package kafka

import (
	"log/slog"
	"time"
)

// ConsumerConfig holds all consumer configuration
type ConsumerConfig struct {
	// Connection
	Brokers []string
	GroupID string
	Topics  []string

	// SSL/SASL authentication
	SSL  bool
	SASL *SASLConfig

	// Session
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration

	// Batch processing
	BatchProcessing bool
	BatchSize       int
	BatchTimeout    time.Duration
	GroupByKey      bool

	// Idempotency
	IdempotencyKey IdempotencyKeyFunc
	IdempotencyTTL time.Duration

	// DLQ
	DLQ      *DLQConfig
	DLQRetry *DLQRetryConfig

	// Commit settings
	AutoCommit         bool
	AutoCommitInterval time.Duration
	FromBeginning      bool

	// Partition assignment
	PartitionAssignor PartitionAssignor

	// Retry
	Retry *RetryConfig

	// Error handling
	ErrorHandler ErrorHandler

	// Rebalance callback
	RebalanceCallback RebalanceCallback

	// Tracing
	Tracing *TracingConfig

	// Logging
	Logger *slog.Logger

	// Escape hatch
	Raw map[string]any
}

// DLQConfig holds Dead Letter Queue configuration
type DLQConfig struct {
	Topic                  string
	MaxRetries             int
	RetryDelay             time.Duration
	RetryBackoffMultiplier float64
	IncludeErrorInfo       bool
	CircuitBreaker         *CircuitBreakerConfig
}

// DLQRetryConfig holds DLQ auto-retry configuration
type DLQRetryConfig struct {
	Enabled           bool
	MaxRetries        int
	Delay             time.Duration
	BackoffMultiplier float64
	FinalDLQTopic     string
	FromBeginning     bool
	GroupID           string
}

// CircuitBreakerConfig holds circuit breaker configuration
type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	Timeout          time.Duration
}

// ConsumerOption is a function that configures the consumer
type ConsumerOption func(*ConsumerConfig)

// ==================== Consumer Options ====================

// ConsumerWithBrokers sets the Kafka broker addresses for consumer
func ConsumerWithBrokers(brokers ...string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Brokers = brokers
	}
}

// ConsumerWithSSL enables SSL for consumer
func ConsumerWithSSL(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SSL = enabled
	}
}

// ConsumerWithSASL sets SASL authentication for consumer
func ConsumerWithSASL(sasl *SASLConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SASL = sasl
	}
}

// ConsumerWithGroupID sets the consumer group ID
func ConsumerWithGroupID(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

// ConsumerWithTopics sets the topics to consume
func ConsumerWithTopics(topics ...string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topics = topics
	}
}

// ConsumerWithSessionTimeout sets the session timeout
func ConsumerWithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SessionTimeout = timeout
	}
}

// ConsumerWithHeartbeatInterval sets the heartbeat interval
func ConsumerWithHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.HeartbeatInterval = interval
	}
}

// ConsumerWithRebalanceTimeout sets the rebalance timeout
func ConsumerWithRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.RebalanceTimeout = timeout
	}
}

// ConsumerWithBatchProcessing enables batch processing
func ConsumerWithBatchProcessing(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchProcessing = enabled
	}
}

// ConsumerWithBatchSize sets the batch size
func ConsumerWithBatchSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchSize = size
	}
}

// ConsumerWithBatchTimeout sets the batch timeout
func ConsumerWithBatchTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchTimeout = timeout
	}
}

// ConsumerWithGroupByKey enables key-based grouping
func ConsumerWithGroupByKey(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupByKey = enabled
	}
}

// ConsumerWithIdempotencyKey sets the idempotency key extractor
func ConsumerWithIdempotencyKey(fn IdempotencyKeyFunc) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IdempotencyKey = fn
	}
}

// ConsumerWithIdempotencyTTL sets the idempotency TTL
func ConsumerWithIdempotencyTTL(ttl time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IdempotencyTTL = ttl
	}
}

// ConsumerWithDLQ sets DLQ configuration
func ConsumerWithDLQ(dlq *DLQConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.DLQ = dlq
	}
}

// ConsumerWithDLQRetry sets DLQ retry configuration
func ConsumerWithDLQRetry(retry *DLQRetryConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.DLQRetry = retry
	}
}

// ConsumerWithAutoCommit sets auto commit
func ConsumerWithAutoCommit(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommit = enabled
	}
}

// ConsumerWithAutoCommitInterval sets auto commit interval
func ConsumerWithAutoCommitInterval(interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommitInterval = interval
	}
}

// ConsumerWithFromBeginning sets whether to start from the beginning
func ConsumerWithFromBeginning(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.FromBeginning = enabled
	}
}

// ConsumerWithPartitionAssignor sets the partition assignment strategy
func ConsumerWithPartitionAssignor(assignor PartitionAssignor) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.PartitionAssignor = assignor
	}
}

// ConsumerWithRetry sets consumer retry configuration
func ConsumerWithRetry(retry *RetryConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Retry = retry
	}
}

// ConsumerWithErrorHandler sets the error handler
func ConsumerWithErrorHandler(handler ErrorHandler) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.ErrorHandler = handler
	}
}

// ConsumerWithRebalanceCallback sets the rebalance callback
// The callback is invoked when partitions are assigned or revoked during a rebalance
// Use this for:
// - Manual offset commits before partition revocation
// - Resource cleanup when losing partitions
// - Initializing resources when gaining new partitions
// - Logging/monitoring rebalance events
func ConsumerWithRebalanceCallback(callback RebalanceCallback) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.RebalanceCallback = callback
	}
}

// ConsumerWithTracing sets tracing configuration for consumer
func ConsumerWithTracing(tracing *TracingConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Tracing = tracing
	}
}

// ConsumerWithLogger sets a custom logger for consumer. The default is
// slog.Default(); silence it with ConsumerWithLogger(slog.New(slog.DiscardHandler)).
func ConsumerWithLogger(logger *slog.Logger) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Logger = logger
	}
}

// ConsumerWithRawConfig merges raw librdkafka consumer configuration keys.
// Raw overrides the connection/auth keys only (bootstrap.servers, SSL/SASL);
// builder-level keys (group.id, enable.auto.commit) are set after the merge
// and win. Use it for keys this library does not model (e.g.
// "fetch.min.bytes", "max.partition.fetch.bytes").
func ConsumerWithRawConfig(raw map[string]any) ConsumerOption {
	return func(c *ConsumerConfig) { c.Raw = raw }
}

// newDefaultConsumerConfig creates a new consumer config with default values
func newDefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		SessionTimeout:     DefaultSessionTimeout,
		HeartbeatInterval:  DefaultHeartbeatInterval,
		BatchSize:          DefaultBatchSize,
		BatchTimeout:       DefaultBatchTimeout,
		IdempotencyTTL:     DefaultIdempotencyTTL,
		AutoCommit:         true,
		AutoCommitInterval: DefaultAutoCommitInterval,
		PartitionAssignor:  AssignorRange,
	}
}
