package kafka

import (
	"time"
)

// ProducerConfig holds all client configuration
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

	// Retry
	Retry *RetryConfig

	// Logging
	LogLevel LogLevel
	Logger   Logger

	// Tracing
	Tracing *TracingConfig

	// Escape hatch
	Raw map[string]any
}

// SASLConfig holds SASL authentication configuration
type SASLConfig struct {
	Mechanism string
	Username  string
	Password  string
}

// RetryConfig holds retry configuration
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
}

// ProducerOption is a function that configures the client
type ProducerOption func(*ProducerConfig)

// Default values
var (
	DefaultConnectionTimeout    = 10 * time.Second
	DefaultRequestTimeout       = 30 * time.Second
	DefaultSessionTimeout       = 30 * time.Second
	DefaultHeartbeatInterval    = 3 * time.Second
	DefaultRebalanceTimeout     = 60 * time.Second
	DefaultBatchSize            = 100
	DefaultBatchTimeout         = 5 * time.Second
	DefaultIdempotencyTTL       = 1 * time.Hour
	DefaultAutoCommitInterval   = 5 * time.Second
	DefaultDLQMaxRetries        = 3
	DefaultDLQRetryDelay        = 1 * time.Second
	DefaultDLQBackoffMultiplier = 2.0
	DefaultRetryMaxRetries      = 3
	DefaultRetryInitialInterval = 1 * time.Second
	DefaultRetryMultiplier      = 2.0
)

// ==================== Client Options ====================

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

// ProducerWithRetry sets retry configuration
func ProducerWithRetry(retry *RetryConfig) ProducerOption {
	return func(c *ProducerConfig) {
		c.Retry = retry
	}
}

// WithLogLevel sets the log level
func WithLogLevel(level LogLevel) ProducerOption {
	return func(c *ProducerConfig) {
		c.LogLevel = level
	}
}

// ProducerWithLogger sets a custom logger
func ProducerWithLogger(logger Logger) ProducerOption {
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

// ProducerWithRawConfig merges raw librdkafka producer configuration keys,
// overriding anything the library itself set. Use it for keys this library
// does not model (e.g. "linger.ms", "queue.buffering.max.messages").
func ProducerWithRawConfig(raw map[string]any) ProducerOption {
	return func(c *ProducerConfig) { c.Raw = raw }
}

// ==================== Default Configs ====================

// newDefaultProducerConfig creates a new producer config with default values
func newDefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		ConnectionTimeout: DefaultConnectionTimeout,
		RequestTimeout:    DefaultRequestTimeout,
		Acks:              AcksAll,
		Compression:       CompressionNone,
		LogLevel:          LogLevelInfo,
	}
}
