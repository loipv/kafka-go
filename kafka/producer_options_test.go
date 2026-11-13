package kafka

import (
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

// sameValue compares a config field to its expectation: pointers by identity,
// everything else (slices, maps, scalars) by content.
func sameValue(got, want any) bool {
	if reflect.TypeOf(want).Kind() == reflect.Pointer {
		return got == want
	}
	return reflect.DeepEqual(got, want)
}

type producerOptionCase struct {
	name string
	opt  ProducerOption
	got  func(*ProducerConfig) any
	want any
}

func runProducerOptionCases(t *testing.T, cases []producerOptionCase) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := newDefaultProducerConfig()
			tt.opt(c)
			if got := tt.got(c); !sameValue(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestProducerOptions_Connection(t *testing.T) {
	runProducerOptionCases(t, []producerOptionCase{
		{"brokers single", ProducerWithBrokers("a:9092"), func(c *ProducerConfig) any { return c.Brokers }, []string{"a:9092"}},
		{"brokers several", ProducerWithBrokers("a:9092", "b:9092"), func(c *ProducerConfig) any { return c.Brokers }, []string{"a:9092", "b:9092"}},
		{"client id", ProducerWithClientID("my-app"), func(c *ProducerConfig) any { return c.ClientID }, "my-app"},
		{"connection timeout", ProducerWithConnectionTimeout(7 * time.Second), func(c *ProducerConfig) any { return c.ConnectionTimeout }, 7 * time.Second},
		{"request timeout", ProducerWithRequestTimeout(45 * time.Second), func(c *ProducerConfig) any { return c.RequestTimeout }, 45 * time.Second},
	})
}

func TestProducerOptions_Security(t *testing.T) {
	sasl := &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"}
	runProducerOptionCases(t, []producerOptionCase{
		{"ssl on", ProducerWithSSL(true), func(c *ProducerConfig) any { return c.SSL }, true},
		{"sasl", ProducerWithSASL(sasl), func(c *ProducerConfig) any { return c.SASL }, sasl},
	})
}

func TestProducerOptions_Delivery(t *testing.T) {
	runProducerOptionCases(t, []producerOptionCase{
		{"acks none", ProducerWithAcks(AcksNone), func(c *ProducerConfig) any { return c.Acks }, AcksNone},
		{"acks leader", ProducerWithAcks(AcksLeader), func(c *ProducerConfig) any { return c.Acks }, AcksLeader},
		{"acks all", ProducerWithAcks(AcksAll), func(c *ProducerConfig) any { return c.Acks }, AcksAll},
		{"compression gzip", ProducerWithCompression(CompressionGZIP), func(c *ProducerConfig) any { return c.Compression }, CompressionGZIP},
		{"compression snappy", ProducerWithCompression(CompressionSnappy), func(c *ProducerConfig) any { return c.Compression }, CompressionSnappy},
		{"compression lz4", ProducerWithCompression(CompressionLZ4), func(c *ProducerConfig) any { return c.Compression }, CompressionLZ4},
		{"compression zstd", ProducerWithCompression(CompressionZSTD), func(c *ProducerConfig) any { return c.Compression }, CompressionZSTD},
		{"idempotent on", ProducerWithIdempotent(true), func(c *ProducerConfig) any { return c.Idempotent }, true},
	})
}

func TestProducerOptions_LoggingAndTracing(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	tracing := &TracingConfig{Enabled: true, TracerName: "tracer", TracerVersion: "v1"}
	runProducerOptionCases(t, []producerOptionCase{
		{"logger", ProducerWithLogger(logger), func(c *ProducerConfig) any { return c.Logger }, logger},
		{"tracing", ProducerWithTracing(tracing), func(c *ProducerConfig) any { return c.Tracing }, tracing},
	})
}

func TestProducerOptions_RawConfig(t *testing.T) {
	raw := map[string]any{"linger.ms": 5, "queue.buffering.max.messages": 1000}
	runProducerOptionCases(t, []producerOptionCase{
		{"raw", ProducerWithRawConfig(raw), func(c *ProducerConfig) any { return c.Raw }, map[string]any{"linger.ms": 5, "queue.buffering.max.messages": 1000}},
	})
}

func TestProducerOptions_DeliveryErrorHandler(t *testing.T) {
	var gotMsg *Message
	var gotErr error
	h := func(msg *Message, err error) { gotMsg, gotErr = msg, err }
	c := newDefaultProducerConfig()
	ProducerWithDeliveryErrorHandler(h)(c)
	if c.DeliveryErrorHandler == nil {
		t.Fatal("DeliveryErrorHandler not stored")
	}
	msg, boom := &Message{Topic: "t"}, errors.New("boom")
	c.DeliveryErrorHandler(msg, boom)
	if gotMsg != msg || !errors.Is(gotErr, boom) {
		t.Errorf("stored handler is not the one passed: got (%v, %v)", gotMsg, gotErr)
	}
}

// Options apply in order and each touches only its own field.
func TestProducerOptions_LastWinsAndIndependent(t *testing.T) {
	c := newDefaultProducerConfig()
	for _, opt := range []ProducerOption{
		ProducerWithBrokers("a:1"), ProducerWithBrokers("b:2"),
		ProducerWithAcks(AcksNone), ProducerWithAcks(AcksLeader),
	} {
		opt(c)
	}
	if !reflect.DeepEqual(c.Brokers, []string{"b:2"}) || c.Acks != AcksLeader {
		t.Errorf("last option must win: Brokers=%v Acks=%v", c.Brokers, c.Acks)
	}
	if c.ClientID != "" || c.Compression != CompressionNone || c.ConnectionTimeout != 10*time.Second {
		t.Errorf("unrelated fields changed: %+v", c)
	}
}

func TestProducerConfig_Defaults(t *testing.T) {
	c := newDefaultProducerConfig()
	if c.ConnectionTimeout != 10*time.Second {
		t.Errorf("ConnectionTimeout = %v, want 10s", c.ConnectionTimeout)
	}
	if c.RequestTimeout != 30*time.Second {
		t.Errorf("RequestTimeout = %v, want 30s", c.RequestTimeout)
	}
	if c.Acks != Acks(-1) {
		t.Errorf("Acks = %v, want all (-1)", c.Acks)
	}
	if c.Compression != Compression("none") {
		t.Errorf("Compression = %q, want \"none\"", c.Compression)
	}
	if c.Brokers != nil || c.ClientID != "" || c.SSL || c.SASL != nil || c.Idempotent {
		t.Errorf("connection/security/idempotence defaults must be zero: %+v", c)
	}
	if c.Logger != nil || c.Tracing != nil || c.DeliveryErrorHandler != nil || c.Raw != nil {
		t.Errorf("Logger/Tracing/DeliveryErrorHandler/Raw defaults must be nil: %+v", c)
	}
}
