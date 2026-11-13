package kafka

import (
	"log/slog"
	"time"
)

// ProducerConfig holds all producer configuration
type ProducerConfig struct {
	// Connection
	Brokers           []string
	ClientID          string
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration

	// SSL/SASL
	SSL  bool
	SASL *SASLConfig

	// Producer settings
	Acks        Acks
	Compression Compression
	Idempotent  bool

	// Logging
	Logger *slog.Logger

	// Tracing
	Tracing *TracingConfig

	// DeliveryErrorHandler receives async delivery failures from
	// ProduceAsync. Without one, failures are only logged.
	DeliveryErrorHandler func(msg *Message, err error)

	// Escape hatch
	Raw map[string]any
}

// SASLConfig holds SASL authentication configuration
type SASLConfig struct {
	Mechanism string
	Username  string
	Password  string
}

// RetryConfig holds retry configuration. A zero field uses its default value:
// MaxRetries 3, InitialInterval 1s, MaxInterval 30s, Multiplier 2.0.
type RetryConfig struct {
	MaxRetries       int
	InitialInterval  time.Duration
	MaxInterval      time.Duration
	Multiplier       float64
	SkipOnMaxRetries bool
}

// TracingConfig holds OpenTelemetry tracing configuration
type TracingConfig struct {
	Enabled       bool
	TracerName    string
	TracerVersion string

	// IncludeMessageKey records the message key as the
	// messaging.kafka.message.key span attribute. Off by default: message
	// keys may contain personal data (user ids, emails).
	IncludeMessageKey bool
}

// ProducerOption is a function that configures the client
type ProducerOption func(*ProducerConfig)

// Default values
const (
	defaultConnectionTimeout    = 10 * time.Second
	defaultRequestTimeout       = 30 * time.Second
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

// ==================== Producer Options ====================

// ProducerWithBrokers sets the Kafka broker addresses
func ProducerWithBrokers(brokers ...string) ProducerOption {
	return func(c *ProducerConfig) {
		c.Brokers = brokers
	}
}

// ProducerWithClientID sets the client ID
func ProducerWithClientID(clientID string) ProducerOption {
	return func(c *ProducerConfig) {
		c.ClientID = clientID
	}
}

// ProducerWithConnectionTimeout sets the connection timeout
func ProducerWithConnectionTimeout(timeout time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.ConnectionTimeout = timeout
	}
}

// ProducerWithRequestTimeout sets the request timeout
func ProducerWithRequestTimeout(timeout time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.RequestTimeout = timeout
	}
}

// ProducerWithSSL enables SSL
func ProducerWithSSL(enabled bool) ProducerOption {
	return func(c *ProducerConfig) {
		c.SSL = enabled
	}
}

// ProducerWithSASL sets SASL authentication
func ProducerWithSASL(sasl *SASLConfig) ProducerOption {
	return func(c *ProducerConfig) {
		c.SASL = sasl
	}
}

// ProducerWithAcks sets the acknowledgment level
func ProducerWithAcks(acks Acks) ProducerOption {
	return func(c *ProducerConfig) {
		c.Acks = acks
	}
}

// ProducerWithCompression sets the compression type
func ProducerWithCompression(compression Compression) ProducerOption {
	return func(c *ProducerConfig) {
		c.Compression = compression
	}
}

// ProducerWithIdempotent enables idempotent producer
func ProducerWithIdempotent(enabled bool) ProducerOption {
	return func(c *ProducerConfig) {
		c.Idempotent = enabled
	}
}

// ProducerWithLogger sets a custom logger. The default is slog.Default();
// silence it with ProducerWithLogger(slog.New(slog.DiscardHandler)).
func ProducerWithLogger(logger *slog.Logger) ProducerOption {
	return func(c *ProducerConfig) {
		c.Logger = logger
	}
}

// ProducerWithTracing sets tracing configuration
func ProducerWithTracing(tracing *TracingConfig) ProducerOption {
	return func(c *ProducerConfig) {
		c.Tracing = tracing
	}
}

// ProducerWithRawConfig merges raw librdkafka producer configuration keys.
// Raw overrides the connection/auth keys only (bootstrap.servers, SSL/SASL);
// builder-level keys (acks, compression.type, enable.idempotence, retries)
// are set after the merge and win. Use it for keys this library does not
// model (e.g. "linger.ms", "queue.buffering.max.messages").
func ProducerWithRawConfig(raw map[string]any) ProducerOption {
	return func(c *ProducerConfig) { c.Raw = raw }
}

// ProducerWithDeliveryErrorHandler sets the sink for asynchronous delivery
// failures (ProduceAsync / fire-and-forget produce).
func ProducerWithDeliveryErrorHandler(h func(msg *Message, err error)) ProducerOption {
	return func(c *ProducerConfig) { c.DeliveryErrorHandler = h }
}

// ==================== Default Configs ====================

// newDefaultProducerConfig creates a new producer config with default values
func newDefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		ConnectionTimeout: defaultConnectionTimeout,
		RequestTimeout:    defaultRequestTimeout,
		Acks:              AcksAll,
		Compression:       CompressionNone,
	}
}
