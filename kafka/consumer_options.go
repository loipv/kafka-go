package kafka

import (
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

	// Pressure management
	BackPressureThreshold int
	MaxQueueSize          int

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
	LogLevel LogLevel
	Logger   Logger
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

// WithConsumerBrokers sets the Kafka broker addresses for consumer
// Deprecated: Use ConsumerWithBrokers instead for consistency
func WithConsumerBrokers(brokers ...string) ConsumerOption {
	return ConsumerWithBrokers(brokers...)
}

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

// WithGroupID sets the consumer group ID
func WithGroupID(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

// WithTopics sets the topics to consume
func WithTopics(topics ...string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topics = topics
	}
}

// WithSessionTimeout sets the session timeout
func WithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SessionTimeout = timeout
	}
}

// WithHeartbeatInterval sets the heartbeat interval
func WithHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.HeartbeatInterval = interval
	}
}

// WithRebalanceTimeout sets the rebalance timeout
func WithRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.RebalanceTimeout = timeout
	}
}

// WithBatchProcessing enables batch processing
func WithBatchProcessing(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchProcessing = enabled
	}
}

// WithBatchSize sets the batch size
func WithBatchSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchSize = size
	}
}

// WithBatchTimeout sets the batch timeout
func WithBatchTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchTimeout = timeout
	}
}

// WithGroupByKey enables key-based grouping
func WithGroupByKey(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupByKey = enabled
	}
}

// WithBackPressureThreshold sets the back pressure threshold
func WithBackPressureThreshold(threshold int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BackPressureThreshold = threshold
	}
}

// WithMaxQueueSize sets the max queue size
func WithMaxQueueSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.MaxQueueSize = size
	}
}

// WithIdempotencyKey sets the idempotency key extractor
func WithIdempotencyKey(fn IdempotencyKeyFunc) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IdempotencyKey = fn
	}
}

// WithIdempotencyTTL sets the idempotency TTL
func WithIdempotencyTTL(ttl time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IdempotencyTTL = ttl
	}
}

// WithDLQ sets DLQ configuration
func WithDLQ(dlq *DLQConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.DLQ = dlq
	}
}

// WithDLQRetry sets DLQ retry configuration
func WithDLQRetry(retry *DLQRetryConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.DLQRetry = retry
	}
}

// WithAutoCommit sets auto commit
func WithAutoCommit(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommit = enabled
	}
}

// WithAutoCommitInterval sets auto commit interval
func WithAutoCommitInterval(interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommitInterval = interval
	}
}

// WithFromBeginning sets whether to start from the beginning
func WithFromBeginning(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.FromBeginning = enabled
	}
}

// WithPartitionAssignor sets the partition assignment strategy
func WithPartitionAssignor(assignor PartitionAssignor) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.PartitionAssignor = assignor
	}
}

// WithConsumerRetry sets consumer retry configuration
func WithConsumerRetry(retry *RetryConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Retry = retry
	}
}

// WithErrorHandler sets the error handler
func WithErrorHandler(handler ErrorHandler) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.ErrorHandler = handler
	}
}

// WithRebalanceCallback sets the rebalance callback
// The callback is invoked when partitions are assigned or revoked during a rebalance
// Use this for:
// - Manual offset commits before partition revocation
// - Resource cleanup when losing partitions
// - Initializing resources when gaining new partitions
// - Logging/monitoring rebalance events
func WithRebalanceCallback(callback RebalanceCallback) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.RebalanceCallback = callback
	}
}

// WithConsumerTracing sets tracing configuration for consumer
// Deprecated: Use ConsumerWithTracing instead for consistency
func WithConsumerTracing(tracing *TracingConfig) ConsumerOption {
	return ConsumerWithTracing(tracing)
}

// ConsumerWithTracing sets tracing configuration for consumer
func ConsumerWithTracing(tracing *TracingConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Tracing = tracing
	}
}

// WithConsumerLogLevel sets the log level for consumer
// Deprecated: Use ConsumerWithLogLevel instead for consistency
func WithConsumerLogLevel(level LogLevel) ConsumerOption {
	return ConsumerWithLogLevel(level)
}

// ConsumerWithLogLevel sets the log level for consumer
func ConsumerWithLogLevel(level LogLevel) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.LogLevel = level
	}
}

// ConsumerWithLogger sets a custom logger for consumer
func ConsumerWithLogger(logger Logger) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Logger = logger
	}
}

// newDefaultConsumerConfig creates a new consumer config with default values
func newDefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		SessionTimeout:        DefaultSessionTimeout,
		HeartbeatInterval:     DefaultHeartbeatInterval,
		RebalanceTimeout:      DefaultRebalanceTimeout,
		BatchSize:             DefaultBatchSize,
		BatchTimeout:          DefaultBatchTimeout,
		BackPressureThreshold: DefaultBackPressureThreshold,
		MaxQueueSize:          DefaultMaxQueueSize,
		IdempotencyTTL:        DefaultIdempotencyTTL,
		AutoCommit:            true,
		AutoCommitInterval:    DefaultAutoCommitInterval,
		PartitionAssignor:     AssignorRange,
		LogLevel:              LogLevelInfo,
	}
}

