package kafka

import (
	"time"
)

// ClientConfig holds all client configuration
type ClientConfig struct {
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

// ClientOption is a function that configures the client
type ClientOption func(*ClientConfig)

// Default values
var (
	DefaultConnectionTimeout     = 10 * time.Second
	DefaultRequestTimeout        = 30 * time.Second
	DefaultSessionTimeout        = 30 * time.Second
	DefaultHeartbeatInterval     = 3 * time.Second
	DefaultRebalanceTimeout      = 60 * time.Second
	DefaultBatchSize             = 100
	DefaultBatchTimeout          = 5 * time.Second
	DefaultBackPressureThreshold = 80
	DefaultMaxQueueSize          = 1000
	DefaultIdempotencyTTL        = 1 * time.Hour
	DefaultAutoCommitInterval    = 5 * time.Second
	DefaultDLQMaxRetries         = 3
	DefaultDLQRetryDelay         = 1 * time.Second
	DefaultDLQBackoffMultiplier  = 2.0
	DefaultRetryMaxRetries       = 3
	DefaultRetryInitialInterval  = 1 * time.Second
	DefaultRetryMultiplier       = 2.0
)

// ==================== Client Options ====================

// WithBrokers sets the Kafka broker addresses
func WithBrokers(brokers ...string) ClientOption {
	return func(c *ClientConfig) {
		c.Brokers = brokers
	}
}

// WithClientID sets the client ID
func WithClientID(clientID string) ClientOption {
	return func(c *ClientConfig) {
		c.ClientID = clientID
	}
}

// WithConnectionTimeout sets the connection timeout
func WithConnectionTimeout(timeout time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.ConnectionTimeout = timeout
	}
}

// WithRequestTimeout sets the request timeout
func WithRequestTimeout(timeout time.Duration) ClientOption {
	return func(c *ClientConfig) {
		c.RequestTimeout = timeout
	}
}

// WithSSL enables SSL
func WithSSL(enabled bool) ClientOption {
	return func(c *ClientConfig) {
		c.SSL = enabled
	}
}

// WithSASL sets SASL authentication
func WithSASL(sasl *SASLConfig) ClientOption {
	return func(c *ClientConfig) {
		c.SASL = sasl
	}
}

// WithAcks sets the acknowledgment level
func WithAcks(acks Acks) ClientOption {
	return func(c *ClientConfig) {
		c.Acks = acks
	}
}

// WithCompression sets the compression type
func WithCompression(compression Compression) ClientOption {
	return func(c *ClientConfig) {
		c.Compression = compression
	}
}

// WithIdempotent enables idempotent producer
func WithIdempotent(enabled bool) ClientOption {
	return func(c *ClientConfig) {
		c.Idempotent = enabled
	}
}

// WithRetry sets retry configuration
func WithRetry(retry *RetryConfig) ClientOption {
	return func(c *ClientConfig) {
		c.Retry = retry
	}
}

// WithLogLevel sets the log level
func WithLogLevel(level LogLevel) ClientOption {
	return func(c *ClientConfig) {
		c.LogLevel = level
	}
}

// WithLogger sets a custom logger
func WithLogger(logger Logger) ClientOption {
	return func(c *ClientConfig) {
		c.Logger = logger
	}
}

// WithTracing sets tracing configuration
func WithTracing(tracing *TracingConfig) ClientOption {
	return func(c *ClientConfig) {
		c.Tracing = tracing
	}
}

// ==================== Default Configs ====================

// newDefaultClientConfig creates a new client config with default values
func newDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ConnectionTimeout: DefaultConnectionTimeout,
		RequestTimeout:    DefaultRequestTimeout,
		Acks:              AcksAll,
		Compression:       CompressionNone,
		LogLevel:          LogLevelInfo,
	}
}
