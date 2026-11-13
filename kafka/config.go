package kafka

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// connConfig is the connection/auth configuration shared by every
// librdkafka handle in this package. It exists so the DLQ producer and the
// health checker can never again forget SSL/SASL.
type connConfig struct {
	Brokers  []string
	ClientID string
	SSL      bool
	SASL     *SASLConfig
	// Raw is merged last within the connection/auth map — the escape hatch
	// for librdkafka keys this library does not model (e.g.
	// queued.max.messages.kbytes). Builder-level keys (acks, group.id, ...)
	// are applied after the merge and are not overridable via Raw.
	Raw map[string]any
}

// configMap returns the shared connection/auth keys. ckafka.ConfigMap is a
// plain map — direct assignment cannot fail, so there are no SetKey errors
// to ignore.
func (cc connConfig) configMap() ckafka.ConfigMap {
	cm := ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(cc.Brokers, ","),
	}
	if cc.ClientID != "" {
		cm["client.id"] = cc.ClientID
	}
	switch {
	case cc.SASL != nil && cc.SSL:
		cm["security.protocol"] = "sasl_ssl"
	case cc.SASL != nil:
		cm["security.protocol"] = "sasl_plaintext"
	case cc.SSL:
		cm["security.protocol"] = "ssl"
	}
	if cc.SASL != nil {
		cm["sasl.mechanism"] = cc.SASL.Mechanism
		cm["sasl.username"] = cc.SASL.Username
		cm["sasl.password"] = cc.SASL.Password
	}
	for k, v := range cc.Raw {
		cm[k] = v
	}
	return cm
}

// warnPlaintextSASL logs once when the resolved security.protocol sends SASL
// credentials without TLS. It decides from the resolved map so a Raw override
// is honoured (librdkafka accepts the protocol name in any case), and never
// logs the username or password.
func warnPlaintextSASL(l *slog.Logger, cc connConfig) {
	if !strings.EqualFold(fmt.Sprint(cc.configMap()["security.protocol"]), "sasl_plaintext") {
		return
	}
	if l == nil {
		l = slog.Default()
	}
	msg := "SASL credentials are sent without TLS (security.protocol=sasl_plaintext); enable SSL to protect them"
	if cc.SASL == nil { // protocol forced to sasl_plaintext via Raw
		l.Warn(msg)
		return
	}
	l.Warn(msg, "mechanism", cc.SASL.Mechanism)
}

// conn is the producer's connection/auth configuration.
func (c *ProducerConfig) conn() connConfig {
	return connConfig{Brokers: c.Brokers, ClientID: c.ClientID, SSL: c.SSL, SASL: c.SASL, Raw: c.Raw}
}

// conn is the consumer's connection/auth configuration. It has no ClientID:
// the consumer does not set client.id.
func (c *ConsumerConfig) conn() connConfig {
	return connConfig{Brokers: c.Brokers, SSL: c.SSL, SASL: c.SASL, Raw: c.Raw}
}

// dlqConn is the connection for every DLQ handle (the DLQ producer):
// the consumer's own unless DLQ.Brokers points at a separate cluster.
func (c *ConsumerConfig) dlqConn() connConfig {
	if c.DLQ == nil || len(c.DLQ.Brokers) == 0 {
		return c.conn()
	}
	return connConfig{Brokers: c.DLQ.Brokers, SSL: c.DLQ.SSL, SASL: c.DLQ.SASL, Raw: c.DLQ.Raw}
}

// timeoutRaw returns the user's message timeout from raw librdkafka config:
// message.timeout.ms, or its alias delivery.timeout.ms when that is absent. nil
// means the user set neither.
func timeoutRaw(raw map[string]any) any {
	if v, ok := raw["message.timeout.ms"]; ok {
		return v
	}
	return raw["delivery.timeout.ms"]
}

// dlqTimeout is how long one DLQ delivery can block the poll loop: 0 without a
// DLQ, otherwise the DLQ connection's message.timeout.ms or its alias
// delivery.timeout.ms (int or numeric string, as librdkafka accepts both) or
// dlqDeliveryTimeout.
func (c *ConsumerConfig) dlqTimeout() time.Duration {
	if c.DLQ == nil {
		return 0
	}
	var ms int
	switch v := timeoutRaw(c.dlqConn().Raw).(type) {
	case int:
		ms = v
	case string:
		ms, _ = strconv.Atoi(v)
	}
	if ms <= 0 {
		return dlqDeliveryTimeout
	}
	return time.Duration(ms) * time.Millisecond
}

// buildProducerConfig returns the librdkafka config for a producer: the shared
// connection/auth keys (Raw merged in), then the keys the library owns, which
// Raw cannot override (see ProducerWithRawConfig).
func buildProducerConfig(c *ProducerConfig) ckafka.ConfigMap {
	cm := c.conn().configMap()
	cm["acks"] = int(c.Acks)
	if c.Compression != "" && c.Compression != CompressionNone {
		cm["compression.type"] = string(c.Compression)
	}
	if c.Idempotent {
		cm["enable.idempotence"] = true
	}
	if c.ConnectionTimeout > 0 {
		cm["socket.connection.setup.timeout.ms"] = int(c.ConnectionTimeout.Milliseconds())
	}
	if c.RequestTimeout > 0 {
		cm["request.timeout.ms"] = int(c.RequestTimeout.Milliseconds())
	}
	return cm
}

// buildConsumerConfig returns the librdkafka config for a consumer: the shared
// connection/auth keys (Raw merged in), then the keys the library owns, which
// Raw cannot override (see ConsumerWithRawConfig). It fails when
// session.timeout.ms is not below the computed max.poll.interval.ms.
func buildConsumerConfig(c *ConsumerConfig) (ckafka.ConfigMap, error) {
	cm := c.conn().configMap()
	cm["group.id"] = c.GroupID
	cm["auto.offset.reset"] = offsetReset(c.FromBeginning)
	cm["enable.auto.commit"] = c.AutoCommit
	// At-least-once: offsets are stored only when a message is parked (see
	// Consumer.deliver). Auto-store would store on read, before the handler runs.
	cm["enable.auto.offset.store"] = false

	if c.SessionTimeout > 0 {
		cm["session.timeout.ms"] = int(c.SessionTimeout.Milliseconds())
	}
	if c.HeartbeatInterval > 0 {
		cm["heartbeat.interval.ms"] = int(c.HeartbeatInterval.Milliseconds())
	}
	if c.AutoCommitInterval > 0 {
		cm["auto.commit.interval.ms"] = int(c.AutoCommitInterval.Milliseconds())
	}
	if c.PartitionAssignor != "" {
		cm["partition.assignment.strategy"] = string(c.PartitionAssignor)
	}

	// max.poll.interval.ms is contested by three features (explicit
	// RebalanceTimeout, the in-loop retry budget, the in-loop DLQ delivery
	// wait). Take the max of them, floored at librdkafka's 300s default, so
	// wiring a low explicit value can never make batch consumers MORE
	// fragile: max(explicit, (retryBudget+dlqTimeout)*1.5, 300s). dlqTimeout
	// is counted once, however many messages a failed batch sends to the DLQ.
	mpiMs := 300000
	if c.RebalanceTimeout > 0 {
		if explicit := int(c.RebalanceTimeout.Milliseconds()); explicit > mpiMs {
			mpiMs = explicit
		}
	}
	if b := retryBudget(normalizeRetry(c.Retry)) + c.dlqTimeout(); b > 0 {
		if need := int(float64(b.Milliseconds()) * 1.5); need > mpiMs {
			mpiMs = need
		}
	}
	cm["max.poll.interval.ms"] = mpiMs

	if v, ok := cm["session.timeout.ms"]; ok {
		st, _ := v.(int)
		if st >= mpiMs {
			return nil, fmt.Errorf("session.timeout.ms (%d) must be < max.poll.interval.ms (%d)", st, mpiMs)
		}
	}
	return cm, nil
}

// SASLConfig holds SASL authentication credentials. Pass it to
// ProducerWithSASL or ConsumerWithSASL (or the DLQConfig.SASL field). The
// password is replaced by "[REDACTED]" in every output the type controls: the
// %v, %+v and %#v verbs (String, GoString), slog's text and JSON handlers
// (LogValue, MarshalJSON) and encoding/json (MarshalJSON), also when the
// SASLConfig or a pointer to it is nested in another struct such as DLQConfig.
// (fmt cannot call methods on unexported fields, so a SASLConfig value held in
// an unexported field of your own struct is printed field by field; hold a
// pointer there instead.) Because JSON output is redacted, a config cannot be
// round-tripped through JSON with its password; decoding JSON into a
// SASLConfig still reads the password.
//
// SASL without SSL sends the credentials unencrypted (security.protocol
// sasl_plaintext); NewProducer, NewConsumer and NewHealthChecker log a warning
// in that case, never including the username or password.
type SASLConfig struct {
	// Mechanism is the librdkafka sasl.mechanism, for example "PLAIN",
	// "SCRAM-SHA-256" or "SCRAM-SHA-512".
	Mechanism string
	// Username is the SASL user name (sasl.username).
	Username string
	// Password is the SASL password (sasl.password). It is redacted when the
	// SASLConfig is printed, logged or encoded as JSON.
	Password string
}

// String returns the config with the password redacted, so printing a
// SASLConfig (or a config that holds one) never leaks it. Value receiver:
// covers SASLConfig and *SASLConfig.
func (s SASLConfig) String() string {
	return fmt.Sprintf("SASLConfig{Mechanism:%s Username:%s Password:[REDACTED]}", s.Mechanism, s.Username)
}

// GoString redacts the password for the %#v verb, which bypasses String.
func (s SASLConfig) GoString() string { return s.String() }

// MarshalJSON encodes the config with the password redacted, so JSON logging
// (slog.NewJSONHandler marshals struct attributes with encoding/json, bypassing
// LogValue) and any other JSON output never leak it. The keys are the field
// names, as with the default encoding.
func (s SASLConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct{ Mechanism, Username, Password string }{s.Mechanism, s.Username, "[REDACTED]"})
}

// LogValue implements slog.LogValuer with the password redacted.
func (s SASLConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("mechanism", s.Mechanism),
		slog.String("username", s.Username),
		slog.String("password", "[REDACTED]"),
	)
}

// TracingConfig holds OpenTelemetry tracing configuration. Pass it to
// ProducerWithTracing or ConsumerWithTracing. Spans are created from the
// global OpenTelemetry tracer provider and the trace context travels in the
// message headers (the global propagator, W3C Trace Context when none is set).
type TracingConfig struct {
	// Enabled turns tracing on. The default (false) disables it even when a
	// TracingConfig is passed.
	Enabled bool
	// TracerName is the instrumentation name of the tracer. When empty it is
	// "github.com/loipv/kafka-go".
	TracerName string
	// TracerVersion is the instrumentation version of the tracer. When empty it
	// is Version.
	TracerVersion string

	// IncludeMessageKey records the message key as the
	// messaging.kafka.message.key span attribute. Off by default: message
	// keys may contain personal data (user ids, emails).
	IncludeMessageKey bool
}

// offsetReset maps ConsumerWithFromBeginning to librdkafka's auto.offset.reset
// value: "earliest" when fromBeginning is true, otherwise "latest".
func offsetReset(fromBeginning bool) string {
	if fromBeginning {
		return "earliest"
	}
	return "latest"
}

// ignoredDLQConnFields lists the DLQConfig connection fields that are set but
// ignored because Brokers is empty (the DLQ then inherits the consumer's
// connection; see dlqConn).
func ignoredDLQConnFields(d *DLQConfig) []string {
	if d == nil || len(d.Brokers) > 0 {
		return nil
	}
	var fields []string
	if d.SSL {
		fields = append(fields, "SSL")
	}
	if d.SASL != nil {
		fields = append(fields, "SASL")
	}
	if len(d.Raw) > 0 {
		fields = append(fields, "Raw")
	}
	return fields
}

// warnIgnoredDLQConn logs one warning naming the DLQ connection fields that
// are ignored without DLQConfig.Brokers. It names the fields, never their
// values.
func warnIgnoredDLQConn(l *slog.Logger, d *DLQConfig) {
	fields := ignoredDLQConnFields(d)
	if len(fields) == 0 {
		return
	}
	if l == nil {
		l = slog.Default()
	}
	l.Warn("DLQConfig connection fields are ignored without DLQConfig.Brokers; the DLQ uses the consumer's connection",
		"fields", strings.Join(fields, ","))
}
