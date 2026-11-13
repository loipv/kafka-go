package kafka

import (
	"log/slog"
	"time"
)

// ConsumerConfig holds all consumer configuration. Every field has a
// ConsumerWith* option of the same name that documents its default; NewConsumer
// applies the defaults first and the options on top. Build a ConsumerConfig
// through those options (or a custom ConsumerOption) rather than by hand, so
// the defaults are not lost.
type ConsumerConfig struct {
	// Connection: ConsumerWithBrokers, ConsumerWithGroupID and
	// ConsumerWithTopics. All three are required.
	Brokers []string
	GroupID string
	Topics  []string

	// SSL and SASL: ConsumerWithSSL and ConsumerWithSASL. Both are off by
	// default.
	SSL  bool
	SASL *SASLConfig

	// Session: ConsumerWithSessionTimeout, ConsumerWithHeartbeatInterval and
	// ConsumerWithRebalanceTimeout.
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration

	// Batch processing: ConsumerWithBatchSize and ConsumerWithBatchTimeout.
	// Only used when a batch handler is registered.
	BatchSize    int
	BatchTimeout time.Duration

	// Idempotency: ConsumerWithIdempotencyKey and ConsumerWithIdempotencyTTL.
	IdempotencyKey IdempotencyKeyFunc
	IdempotencyTTL time.Duration

	// DLQ is set by ConsumerWithDLQ; nil means no dead letter queue.
	DLQ *DLQConfig

	// Commit settings: ConsumerWithAutoCommit, ConsumerWithAutoCommitInterval
	// and ConsumerWithFromBeginning.
	AutoCommit         bool
	AutoCommitInterval time.Duration
	FromBeginning      bool

	// PartitionAssignor is set by ConsumerWithPartitionAssignor.
	PartitionAssignor PartitionAssignor

	// Retry is set by ConsumerWithRetry; nil means the RetryConfig defaults.
	Retry *RetryConfig

	// ErrorHandler is set by ConsumerWithErrorHandler; nil means none.
	ErrorHandler ErrorHandler

	// RebalanceCallback is set by ConsumerWithRebalanceCallback; nil means
	// none.
	RebalanceCallback RebalanceCallback

	// Tracing is set by ConsumerWithTracing; nil means tracing is off.
	Tracing *TracingConfig

	// Logger is set by ConsumerWithLogger; nil means slog.Default().
	Logger *slog.Logger

	// Raw holds raw librdkafka keys set by ConsumerWithRawConfig. Nil means
	// none.
	Raw map[string]any
}

// DLQConfig holds Dead Letter Queue configuration. Pass it to ConsumerWithDLQ.
// A message that cannot be processed is produced to Topic with its original
// key, value and headers plus x-dlq-original-topic, x-dlq-timestamp
// (RFC 3339), x-dlq-handler-retry-count and, with IncludeErrorInfo,
// x-dlq-error-message.
//
// The DLQ produce is a single attempt, confirmed by its delivery report and
// bounded by the DLQ producer's message.timeout.ms: 30s unless Raw sets
// message.timeout.ms (delivery.timeout.ms is accepted as an alias). The
// producer always uses acks=all. When the produce fails or times out the
// message is not parked: its partition blocks and the message is retried later
// (see the consumer's at-least-once behaviour), so a broken DLQ stalls only the
// affected partitions and nothing is dropped.
type DLQConfig struct {
	// Topic is the dead letter topic. It is required: NewConsumer returns
	// ErrDLQTopicRequired when a DLQConfig has an empty Topic.
	Topic string
	// IncludeErrorInfo adds the handler error text as the x-dlq-error-message
	// header. The default (false) omits it. The text may contain sensitive data
	// from the failed message; it is truncated to 1024 bytes.
	IncludeErrorInfo bool

	// Brokers, SSL, SASL and Raw select a separate DLQ cluster. With empty
	// Brokers (the default) the DLQ producer inherits the consumer's connection
	// (brokers, SSL, SASL, Raw) and SSL, SASL and Raw here are ignored. Set
	// Brokers and the DLQ producer uses only the SSL, SASL and Raw below —
	// nothing is inherited, so the consumer's credentials never reach another
	// cluster.
	Brokers []string
	SSL     bool
	SASL    *SASLConfig
	Raw     map[string]any
}

// RetryConfig holds retry configuration. Pass it to ConsumerWithRetry. A zero
// field (or a negative one) uses its default value: MaxRetries 3,
// InitialInterval 1s, MaxInterval 30s, Multiplier 2.0. Retries cannot be
// disabled by setting a field to 0: that selects the default. Without
// ConsumerWithRetry the same defaults apply.
//
// A single-message handler (OnMessage) that returns an error is called again
// after a wait that starts at InitialInterval and is multiplied by Multiplier
// after every failure, capped at MaxInterval. A batch handler (OnBatch,
// OnGroupedBatch) is not retried in place: its failure goes straight to the
// error handler, the DLQ or the partition block, and only InitialInterval and
// MaxInterval are used, as the wait before a blocked partition is retried
// (doubling from InitialInterval up to MaxInterval).
type RetryConfig struct {
	// MaxRetries is the number of retries after the first attempt, so a message
	// handler is called at most MaxRetries+1 times. The default is 3.
	MaxRetries int
	// InitialInterval is the wait before the first retry and the first
	// partition-block wait. The default is 1s.
	InitialInterval time.Duration
	// MaxInterval caps the wait between retries and the partition-block wait.
	// The default is 30s.
	MaxInterval time.Duration
	// Multiplier is the factor the retry wait grows by after each failure. The
	// default is 2.0. It does not affect the partition-block wait, which always
	// doubles.
	Multiplier float64
	// SkipOnMaxRetries is the explicit opt-in to dropping a message: when the
	// retries of a message handler are exhausted, the message is offered to the
	// error handler and, if that does not claim it, to the DLQ on a best-effort
	// basis, and then parked even if neither took it, so it is lost. The default
	// (false) keeps the message and blocks its partition instead. It only applies
	// to OnMessage handlers; the error passed on wraps ErrSkippedOnMaxRetries.
	SkipOnMaxRetries bool
}

// ConsumerOption configures a Consumer. Pass options to NewConsumer; they are
// applied in order, so a later option overrides an earlier one for the same
// setting.
type ConsumerOption func(*ConsumerConfig)

// Consumer defaults, applied by newDefaultConsumerConfig and normalizeRetry.
const (
	defaultSessionTimeout       = 30 * time.Second
	defaultHeartbeatInterval    = 3 * time.Second
	defaultBatchSize            = 100
	defaultBatchTimeout         = 5 * time.Second
	defaultIdempotencyTTL       = 1 * time.Hour
	defaultAutoCommitInterval   = 5 * time.Second
	defaultRetryMaxRetries      = 3
	defaultRetryInitialInterval = 1 * time.Second
	defaultRetryMultiplier      = 2.0
	defaultRetryMaxInterval     = 30 * time.Second
)

// ConsumerWithBrokers sets the Kafka broker addresses (bootstrap.servers). There
// is no default: NewConsumer returns ErrBrokersRequired when no broker is
// given.
func ConsumerWithBrokers(brokers ...string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Brokers = brokers
	}
}

// ConsumerWithSSL enables TLS to the brokers (security.protocol ssl, or
// sasl_ssl together with ConsumerWithSASL). The default is false (plaintext).
func ConsumerWithSSL(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SSL = enabled
	}
}

// ConsumerWithSASL sets SASL authentication. The default is nil (no SASL).
// Without ConsumerWithSSL the credentials travel unencrypted (sasl_plaintext)
// and NewConsumer logs a warning.
func ConsumerWithSASL(sasl *SASLConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SASL = sasl
	}
}

// ConsumerWithGroupID sets the consumer group ID (group.id). There is no
// default: NewConsumer returns ErrGroupIDRequired when it is empty.
func ConsumerWithGroupID(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

// ConsumerWithTopics sets the topics to subscribe to. There is no default:
// NewConsumer returns ErrTopicsRequired when none is given.
func ConsumerWithTopics(topics ...string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topics = topics
	}
}

// ConsumerWithSessionTimeout sets the group session timeout
// (session.timeout.ms): the time the broker waits for a heartbeat before it
// considers the consumer dead and rebalances. The default is 30s; a value of
// zero or less leaves the key unset, so librdkafka's own default applies.
// NewConsumer returns an error when it is not smaller than the computed
// max.poll.interval.ms (see ConsumerWithRebalanceTimeout).
func ConsumerWithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.SessionTimeout = timeout
	}
}

// ConsumerWithHeartbeatInterval sets how often the consumer heartbeats to the
// group coordinator (heartbeat.interval.ms). The default is 3s; a value of zero
// or less leaves the key unset, so librdkafka's own default applies.
func ConsumerWithHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.HeartbeatInterval = interval
	}
}

// ConsumerWithRebalanceTimeout sets max.poll.interval.ms: the longest the
// consumer may go without polling before the group considers it failed. The
// default is zero, meaning the library computes the value itself. Whatever is
// set, the effective value is the largest of the option, 300s, and 1.5 times
// the handler retry budget (the sum of the retry waits of ConsumerWithRetry)
// plus the DLQ delivery timeout, so a low value can never make the consumer
// less tolerant than the default.
func ConsumerWithRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.RebalanceTimeout = timeout
	}
}

// ConsumerWithBatchSize sets the number of messages that triggers a batch
// flush. The default is 100. It must be greater than zero; NewConsumer returns
// ErrInvalidBatchSize otherwise, whether or not a batch handler is registered.
// It only affects consumers with a batch handler (OnBatch or OnGroupedBatch).
func ConsumerWithBatchSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchSize = size
	}
}

// ConsumerWithBatchTimeout sets how often a partial batch is flushed to the
// batch handler. The default is 5s. It must be greater than zero; NewConsumer
// returns ErrInvalidBatchTimeout otherwise, whether or not a batch handler is
// registered. It only affects consumers with a batch handler (OnBatch or
// OnGroupedBatch).
func ConsumerWithBatchTimeout(timeout time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchTimeout = timeout
	}
}

// ConsumerWithIdempotencyKey sets the idempotency key extractor. The default is
// nil: no de-duplication. Messages whose key was already processed
// successfully are skipped and their offset advances; an empty key is never
// filtered. In batch mode duplicates are removed before the handler is called,
// including repeats inside the same batch, and therefore also inside the groups
// passed to OnGroupedBatch.
//
// The memory is in-process only: it is lost on restart or when a partition
// moves to another instance, and it grows with the number of distinct keys seen
// within one TTL (see ConsumerWithIdempotencyTTL). A key is recorded only after
// the handler succeeded; the keys of a failed batch are not recorded, and the
// whole batch is redelivered unfiltered, so real duplicate protection for
// failed batches belongs in the handler. Treat it as best-effort
// de-duplication, not exactly-once.
func ConsumerWithIdempotencyKey(fn IdempotencyKeyFunc) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IdempotencyKey = fn
	}
}

// ConsumerWithIdempotencyTTL sets how long a processed idempotency key is
// remembered. The default is 1 hour; zero or less also selects 1 hour. It only
// matters when ConsumerWithIdempotencyKey is set. The store is in-process
// memory only, and holds one entry per distinct key seen within the TTL, so
// memory grows with key volume times TTL; see ConsumerWithIdempotencyKey for
// its limits.
func ConsumerWithIdempotencyTTL(ttl time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.IdempotencyTTL = ttl
	}
}

// ConsumerWithDLQ enables a dead letter queue for messages that cannot be
// processed. The default is nil: no DLQ, so a message that fails (after
// retries) and is not claimed by the error handler blocks its partition
// instead. See DLQConfig for the delivery guarantees.
func ConsumerWithDLQ(dlq *DLQConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.DLQ = dlq
	}
}

// ConsumerWithAutoCommit sets whether librdkafka commits offsets in the
// background (enable.auto.commit). The default is true, committing every
// ConsumerWithAutoCommitInterval. Only offsets of parked messages (processed,
// confirmed to the DLQ, or claimed by the error handler) are ever committed.
//
// With false nothing is committed in the background: the handler (or the
// application) must call Consumer.Commit itself. Offsets are still stored when
// messages park, so Commit without arguments commits them; the library itself
// only commits stored offsets when partitions are revoked in a rebalance.
func ConsumerWithAutoCommit(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommit = enabled
	}
}

// ConsumerWithAutoCommitInterval sets how often offsets are committed when
// auto commit is on (auto.commit.interval.ms). The default is 5s; a value of
// zero or less leaves the key unset, so librdkafka's own default applies. It has
// no effect with ConsumerWithAutoCommit(false).
func ConsumerWithAutoCommitInterval(interval time.Duration) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.AutoCommitInterval = interval
	}
}

// ConsumerWithFromBeginning sets where a consumer group with no committed
// offset starts (auto.offset.reset): true reads from the earliest offset,
// false (the default) only reads messages produced after it joins (latest).
// A group that already has a committed offset resumes from it either way.
func ConsumerWithFromBeginning(enabled bool) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.FromBeginning = enabled
	}
}

// ConsumerWithPartitionAssignor sets the partition assignment strategy
// (partition.assignment.strategy). The default is AssignorRange. All members
// of a group should use the same strategy.
func ConsumerWithPartitionAssignor(assignor PartitionAssignor) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.PartitionAssignor = assignor
	}
}

// ConsumerWithRetry sets the handler retry policy. The default is nil, which
// behaves like an empty RetryConfig: 3 retries, 1s initial wait doubling up to
// 30s. See RetryConfig for what applies to single-message and batch handlers.
func ConsumerWithRetry(retry *RetryConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Retry = retry
	}
}

// ConsumerWithErrorHandler sets the handler called for a message the consumer
// could not process (a single-message handler after its retries, or each
// message of a failed batch). The default is nil. See ErrorHandler: returning
// nil means the handler took ownership of the message, so it is parked and its
// offset may advance; returning an error defers to the DLQ or, without one,
// blocks the partition.
func ConsumerWithErrorHandler(handler ErrorHandler) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.ErrorHandler = handler
	}
}

// ConsumerWithRebalanceCallback sets a callback invoked when partitions are
// assigned or revoked during a rebalance. The default is nil: the library still
// handles assignment itself. The callback runs on the goroutine running Start,
// before the library applies the assignment, so keep it short. Use it for:
//   - manual offset commits before partition revocation
//   - resource cleanup when losing partitions
//   - initializing resources when gaining new partitions
//   - logging or monitoring rebalance events
//
// A non-nil error is logged and returned from the library's rebalance
// callback, and the library skips its own assign or revoke handling for that
// event.
func ConsumerWithRebalanceCallback(callback RebalanceCallback) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.RebalanceCallback = callback
	}
}

// ConsumerWithTracing sets the OpenTelemetry tracing configuration. The default
// is nil (tracing off); a non-nil config also needs Enabled set. Each handler
// invocation runs inside a consumer span, parented to the trace context found
// in the message headers.
func ConsumerWithTracing(tracing *TracingConfig) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Tracing = tracing
	}
}

// ConsumerWithLogger sets the logger for the consumer's own diagnostics
// (rebalances, retries, blocked partitions, DLQ failures). The default is
// slog.Default(); silence it with
// ConsumerWithLogger(slog.New(slog.DiscardHandler)). The library never logs
// message keys, values or headers.
func ConsumerWithLogger(logger *slog.Logger) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Logger = logger
	}
}

// ConsumerWithRawConfig merges raw librdkafka consumer configuration keys. The
// default is nil (no extra keys). Use it for keys this library does not model
// (e.g. "fetch.min.bytes", "max.partition.fetch.bytes", "client.id").
//
// Raw is merged into the connection and authentication keys (bootstrap.servers,
// security.protocol, sasl.*), which it may therefore override. The library then
// sets these keys after the merge, so Raw cannot override them: group.id,
// auto.offset.reset, enable.auto.commit, enable.auto.offset.store (always false:
// offsets are stored only when a message is parked) and max.poll.interval.ms
// (use ConsumerWithRebalanceTimeout). Also set after the merge, while their
// option is above zero or non-empty (which is the default): session.timeout.ms,
// heartbeat.interval.ms, auto.commit.interval.ms and
// partition.assignment.strategy. Setting the option itself to zero (or empty)
// leaves the key to Raw.
func ConsumerWithRawConfig(raw map[string]any) ConsumerOption {
	return func(c *ConsumerConfig) { c.Raw = raw }
}

// newDefaultConsumerConfig creates a new consumer config with default values
func newDefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		SessionTimeout:     defaultSessionTimeout,
		HeartbeatInterval:  defaultHeartbeatInterval,
		BatchSize:          defaultBatchSize,
		BatchTimeout:       defaultBatchTimeout,
		IdempotencyTTL:     defaultIdempotencyTTL,
		AutoCommit:         true,
		AutoCommitInterval: defaultAutoCommitInterval,
		PartitionAssignor:  AssignorRange,
	}
}

// normalizeRetry resolves r into a RetryConfig with every field set: a nil r
// or a field that is not > 0 takes its default. SkipOnMaxRetries is carried
// through unchanged.
func normalizeRetry(r *RetryConfig) RetryConfig {
	out := RetryConfig{
		MaxRetries:      defaultRetryMaxRetries,
		InitialInterval: defaultRetryInitialInterval,
		MaxInterval:     defaultRetryMaxInterval,
		Multiplier:      defaultRetryMultiplier,
	}
	if r == nil {
		return out
	}
	if r.MaxRetries > 0 {
		out.MaxRetries = r.MaxRetries
	}
	if r.InitialInterval > 0 {
		out.InitialInterval = r.InitialInterval
	}
	if r.MaxInterval > 0 {
		out.MaxInterval = r.MaxInterval
	}
	if r.Multiplier > 0 {
		out.Multiplier = r.Multiplier
	}
	out.SkipOnMaxRetries = r.SkipOnMaxRetries
	return out
}
