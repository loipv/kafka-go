package kafka

import (
	"log/slog"
	"time"
)

// ProducerConfig holds all producer configuration. Every field has a
// ProducerWith* option of the same name that documents its default; NewProducer
// applies the defaults first and the options on top. Build a ProducerConfig
// through those options (or a custom ProducerOption) rather than by hand, so
// the defaults are not lost.
type ProducerConfig struct {
	// Connection: ProducerWithBrokers (required), ProducerWithClientID,
	// ProducerWithConnectionTimeout and ProducerWithRequestTimeout.
	Brokers           []string
	ClientID          string
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration

	// SSL and SASL: ProducerWithSSL and ProducerWithSASL. Both are off by
	// default.
	SSL  bool
	SASL *SASLConfig

	// Producer settings: ProducerWithAcks, ProducerWithCompression and
	// ProducerWithIdempotent.
	Acks        Acks
	Compression Compression
	Idempotent  bool

	// Logger is set by ProducerWithLogger; nil means slog.Default().
	Logger *slog.Logger

	// Tracing is set by ProducerWithTracing; nil means tracing is off.
	Tracing *TracingConfig

	// DeliveryErrorHandler receives async delivery failures from
	// ProduceAsync. Without one, failures are only logged.
	DeliveryErrorHandler func(msg *Message, err error)

	// Raw holds raw librdkafka keys set by ProducerWithRawConfig. Nil means
	// none.
	Raw map[string]any
}

// ProducerOption configures a Producer. Pass options to NewProducer (or
// NewHealthChecker); they are applied in order, so a later option overrides an
// earlier one for the same setting.
type ProducerOption func(*ProducerConfig)

// Producer defaults, applied by newDefaultProducerConfig.
const (
	defaultConnectionTimeout = 10 * time.Second
	defaultRequestTimeout    = 30 * time.Second
)

// ProducerWithBrokers sets the Kafka broker addresses (bootstrap.servers).
// There is no default: NewProducer returns ErrBrokersRequired when no broker
// is given.
func ProducerWithBrokers(brokers ...string) ProducerOption {
	return func(c *ProducerConfig) {
		c.Brokers = brokers
	}
}

// ProducerWithClientID sets the client ID (client.id) the producer reports to
// the brokers. The default is empty, in which case librdkafka uses its own
// default client id.
func ProducerWithClientID(clientID string) ProducerOption {
	return func(c *ProducerConfig) {
		c.ClientID = clientID
	}
}

// ProducerWithConnectionTimeout sets how long librdkafka may take to set up a
// broker connection (socket.connection.setup.timeout.ms). The default is 10s;
// a value of zero or less leaves the key unset, so librdkafka's own default
// applies.
func ProducerWithConnectionTimeout(timeout time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.ConnectionTimeout = timeout
	}
}

// ProducerWithRequestTimeout sets the timeout of a broker request
// (request.timeout.ms); for produce requests it is how long the broker waits
// for the acknowledgments chosen by ProducerWithAcks. The default is 30s; a value of zero or less leaves the
// key unset, so librdkafka's own default applies. It is not the message
// delivery timeout: set message.timeout.ms through ProducerWithRawConfig to
// bound how long a message may stay undelivered.
func ProducerWithRequestTimeout(timeout time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.RequestTimeout = timeout
	}
}

// ProducerWithSSL enables TLS to the brokers (security.protocol ssl, or
// sasl_ssl together with ProducerWithSASL). The default is false (plaintext).
func ProducerWithSSL(enabled bool) ProducerOption {
	return func(c *ProducerConfig) {
		c.SSL = enabled
	}
}

// ProducerWithSASL sets SASL authentication. The default is nil (no SASL).
// Without ProducerWithSSL the credentials travel unencrypted
// (sasl_plaintext) and NewProducer logs a warning.
func ProducerWithSASL(sasl *SASLConfig) ProducerOption {
	return func(c *ProducerConfig) {
		c.SASL = sasl
	}
}

// ProducerWithAcks sets how many broker acknowledgments a message needs
// before it counts as delivered. The default is AcksAll. Idempotent
// production (ProducerWithIdempotent) requires AcksAll.
func ProducerWithAcks(acks Acks) ProducerOption {
	return func(c *ProducerConfig) {
		c.Acks = acks
	}
}

// ProducerWithCompression sets the compression codec for produced batches
// (compression.type). The default is CompressionNone. NewProducer rejects a
// value librdkafka does not support.
func ProducerWithCompression(compression Compression) ProducerOption {
	return func(c *ProducerConfig) {
		c.Compression = compression
	}
}

// ProducerWithIdempotent enables the librdkafka idempotent producer
// (enable.idempotence), which prevents duplicates and reordering caused by
// producer-side retries. The default is false, in which case the key is left
// unset and librdkafka's default applies.
func ProducerWithIdempotent(enabled bool) ProducerOption {
	return func(c *ProducerConfig) {
		c.Idempotent = enabled
	}
}

// ProducerWithLogger sets the logger for the producer's own diagnostics
// (async delivery failures without a handler, librdkafka errors, a close with
// undelivered messages). The default is slog.Default(); silence it with
// ProducerWithLogger(slog.New(slog.DiscardHandler)). The library never logs
// message keys, values or headers.
func ProducerWithLogger(logger *slog.Logger) ProducerOption {
	return func(c *ProducerConfig) {
		c.Logger = logger
	}
}

// ProducerWithTracing sets the OpenTelemetry tracing configuration. The
// default is nil (tracing off); a non-nil config also needs Enabled set.
// Produce and ProduceBatch create a span and inject the trace context into the
// message headers; ProduceAsync does not.
func ProducerWithTracing(tracing *TracingConfig) ProducerOption {
	return func(c *ProducerConfig) {
		c.Tracing = tracing
	}
}

// ProducerWithRawConfig merges raw librdkafka producer configuration keys. The
// default is nil (no extra keys). Use it for keys this library does not model
// (e.g. "linger.ms", "queue.buffering.max.messages", "message.timeout.ms").
//
// Raw is merged into the connection and authentication keys (bootstrap.servers,
// client.id, security.protocol, sasl.*), which it may therefore override. The
// library then sets these keys after the merge, so Raw cannot override them:
// acks (always), request.timeout.ms and socket.connection.setup.timeout.ms
// (while the timeout option is above zero, which is the default),
// compression.type (unless the compression is CompressionNone or empty) and
// enable.idempotence (only when ProducerWithIdempotent(true) is set).
func ProducerWithRawConfig(raw map[string]any) ProducerOption {
	return func(c *ProducerConfig) { c.Raw = raw }
}

// ProducerWithDeliveryErrorHandler sets the sink for asynchronous delivery
// failures reported after ProduceAsync returned. The default is nil: failures
// are logged at error level through the producer's logger. The handler runs on
// the producer's single delivery-report goroutine, so a slow handler delays
// later reports and must not call Close. The message passed to it carries the
// key, value, headers and topic; Partition and Offset are those of the failed
// delivery report.
func ProducerWithDeliveryErrorHandler(h func(msg *Message, err error)) ProducerOption {
	return func(c *ProducerConfig) { c.DeliveryErrorHandler = h }
}

// newDefaultProducerConfig creates a new producer config with default values
func newDefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		ConnectionTimeout: defaultConnectionTimeout,
		RequestTimeout:    defaultRequestTimeout,
		Acks:              AcksAll,
		Compression:       CompressionNone,
	}
}
