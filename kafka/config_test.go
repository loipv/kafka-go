package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConnConfigMap(t *testing.T) {
	tests := []struct {
		name string
		cfg  connConfig
		want map[string]any
	}{
		{
			name: "sasl over ssl yields sasl_ssl",
			cfg: connConfig{Brokers: []string{"b1:9092", "b2:9092"}, SSL: true,
				SASL: &SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "u", Password: "p"}},
			want: map[string]any{
				"bootstrap.servers": "b1:9092,b2:9092",
				"security.protocol": "sasl_ssl",
				"sasl.mechanism":    "SCRAM-SHA-256",
				"sasl.username":     "u",
				"sasl.password":     "p",
			},
		},
		{
			name: "sasl without ssl yields sasl_plaintext",
			cfg: connConfig{Brokers: []string{"b:9092"},
				SASL: &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"}},
			want: map[string]any{
				"security.protocol": "sasl_plaintext",
				"sasl.mechanism":    "PLAIN",
			},
		},
		{
			name: "ssl alone yields ssl",
			cfg:  connConfig{Brokers: []string{"b:9092"}, SSL: true},
			want: map[string]any{"security.protocol": "ssl"},
		},
		{
			name: "client id set when present",
			cfg:  connConfig{Brokers: []string{"b:9092"}, ClientID: "app-1"},
			want: map[string]any{"client.id": "app-1"},
		},
		{
			name: "raw merges and overrides built-ins",
			cfg: connConfig{Brokers: []string{"b:9092"},
				Raw: map[string]any{"bootstrap.servers": "override:9092", "queued.max.messages.kbytes": 64}},
			want: map[string]any{"bootstrap.servers": "override:9092", "queued.max.messages.kbytes": 64},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.configMap()
			for k, w := range tt.want {
				v, err := got.Get(k, nil)
				if err != nil {
					t.Fatalf("connConfig%+v: Get(%q) error = %v", tt.cfg, k, err)
				}
				if v != w {
					t.Errorf("connConfig%+v: key %q = %v, want %v", tt.cfg, k, v, w)
				}
			}
		})
	}
}

func TestConnConfigMap_NoAuthOmitsSecurityProtocol(t *testing.T) {
	cm := connConfig{Brokers: []string{"b:9092"}}.configMap()
	// ckafka.ConfigMap.Get returns (nil, nil) for a missing key — absence is a nil value.
	if v, _ := cm.Get("security.protocol", nil); v != nil {
		t.Errorf("security.protocol must be absent when no auth is configured, got %v", v)
	}
}

// The auth-dropping regression: every builder in this package must carry auth.
// A future client that forgets fails the moment it is added to this list.
func TestAllBuildersPropagateAuth(t *testing.T) {
	sasl := &SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "u", Password: "p"}
	builders := []struct {
		name  string
		build func() (ckafka.ConfigMap, error)
	}{
		{"producer", func() (ckafka.ConfigMap, error) {
			return buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"}, SSL: true, SASL: sasl}), nil
		}},
		{"consumer", func() (ckafka.ConfigMap, error) {
			return buildConsumerConfig(&ConsumerConfig{Brokers: []string{"b:9092"}, GroupID: "g", Topics: []string{"t"}, SSL: true, SASL: sasl})
		}},
		{"dlq-and-health", func() (ckafka.ConfigMap, error) {
			return connConfig{Brokers: []string{"b:9092"}, SSL: true, SASL: sasl}.configMap(), nil
		}},
	}
	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			cm, err := b.build()
			if err != nil {
				t.Fatalf("builder %q: %v", b.name, err)
			}
			// ConfigMap.Get returns (nil, nil) for a missing key — a dropped
			// auth key surfaces as a nil value, not an error.
			if v, _ := cm.Get("security.protocol", nil); v != "sasl_ssl" {
				t.Errorf("builder %q security.protocol = %v, want sasl_ssl", b.name, v)
			}
		})
	}
}

// DLQ on a separate cluster: empty DLQ.Brokers inherits the consumer's
// connection; a set DLQ.Brokers uses only the DLQ's own SSL/SASL/Raw so the
// consumer's credentials never reach a foreign cluster.
func TestDLQConn(t *testing.T) {
	consumerSASL := &SASLConfig{Mechanism: "PLAIN", Username: "cu", Password: "cp"}
	dlqSASL := &SASLConfig{Mechanism: "SCRAM-SHA-512", Username: "du", Password: "dp"}
	tests := []struct {
		name string
		dlq  *DLQConfig
		want connConfig
	}{
		{
			name: "empty brokers inherits consumer connection",
			dlq:  &DLQConfig{Topic: "dlq"},
			want: connConfig{Brokers: []string{"a:9092"}, SSL: true, SASL: consumerSASL,
				Raw: map[string]any{"x": 1}},
		},
		{
			name: "set brokers isolates from consumer auth",
			dlq: &DLQConfig{Topic: "dlq", Brokers: []string{"b:9092"}, SASL: dlqSASL,
				Raw: map[string]any{"y": 2}},
			want: connConfig{Brokers: []string{"b:9092"}, SASL: dlqSASL,
				Raw: map[string]any{"y": 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ConsumerConfig{Brokers: []string{"a:9092"}, SSL: true, SASL: consumerSASL,
				Raw: map[string]any{"x": 1}, DLQ: tt.dlq}
			if got := c.dlqConn(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dlqConn() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBuildConsumerConfig(t *testing.T) {
	base := func() *ConsumerConfig {
		return &ConsumerConfig{Brokers: []string{"b:9092"}, GroupID: "g", Topics: []string{"t"},
			SessionTimeout: defaultSessionTimeout}
	}
	t.Run("auto offset store is always off (at-least-once)", func(t *testing.T) {
		cm, err := buildConsumerConfig(base())
		if err != nil {
			t.Fatalf("buildConsumerConfig: %v", err)
		}
		if v, _ := cm.Get("enable.auto.offset.store", nil); v != false {
			t.Errorf("enable.auto.offset.store = %v, want false", v)
		}
	})
	t.Run("max.poll.interval.ms floors at librdkafka's 300s default", func(t *testing.T) {
		// default retry budget is 7s; 7s*1.5 = 10.5s < 300s → floor 300000
		cm, err := buildConsumerConfig(base())
		if err != nil {
			t.Fatalf("buildConsumerConfig: %v", err)
		}
		if v, _ := cm.Get("max.poll.interval.ms", nil); v != 300000 {
			t.Errorf("max.poll.interval.ms = %v, want 300000", v)
		}
	})
	t.Run("heavy RetryConfig raises max.poll.interval.ms", func(t *testing.T) {
		// budget 10*30s = 300s; *1.5 → 450000
		c := base()
		c.Retry = &RetryConfig{MaxRetries: 10, InitialInterval: 30 * time.Second, Multiplier: 1}
		cm, err := buildConsumerConfig(c)
		if err != nil {
			t.Fatalf("buildConsumerConfig: %v", err)
		}
		if v, _ := cm.Get("max.poll.interval.ms", nil); v != 450000 {
			t.Errorf("max.poll.interval.ms = %v, want 450000", v)
		}
	})
	t.Run("DLQ delivery timeout is part of the max.poll.interval.ms floor", func(t *testing.T) {
		// (300s retry budget + 30s DLQ timeout) * 1.5 → 495000
		c := base()
		c.Retry = &RetryConfig{MaxRetries: 10, InitialInterval: 30 * time.Second, Multiplier: 1}
		c.DLQ = &DLQConfig{Topic: "d"}
		cm, err := buildConsumerConfig(c)
		if err != nil {
			t.Fatalf("buildConsumerConfig: %v", err)
		}
		if v, _ := cm.Get("max.poll.interval.ms", nil); v != 495000 {
			t.Errorf("max.poll.interval.ms = %v, want 495000", v)
		}
	})
	t.Run("explicit low RebalanceTimeout cannot drop below the 300s floor", func(t *testing.T) {
		c := base()
		c.RebalanceTimeout = 10 * time.Second
		cm, err := buildConsumerConfig(c)
		if err != nil {
			t.Fatalf("buildConsumerConfig: %v", err)
		}
		if v, _ := cm.Get("max.poll.interval.ms", nil); v != 300000 {
			t.Errorf("max.poll.interval.ms = %v, want 300000 (floor)", v)
		}
	})
	t.Run("explicit RebalanceTimeout wins when above the floor", func(t *testing.T) {
		c := base()
		c.RebalanceTimeout = 600 * time.Second
		cm, err := buildConsumerConfig(c)
		if err != nil {
			t.Fatalf("buildConsumerConfig: %v", err)
		}
		if v, _ := cm.Get("max.poll.interval.ms", nil); v != 600000 {
			t.Errorf("max.poll.interval.ms = %v, want 600000", v)
		}
	})
	t.Run("session timeout at or above max.poll.interval is rejected", func(t *testing.T) {
		c := base()
		c.SessionTimeout = 500 * time.Second
		c.Retry = &RetryConfig{MaxRetries: 10, InitialInterval: 30 * time.Second, Multiplier: 1} // raises mpi to 450s
		if _, err := buildConsumerConfig(c); err == nil {
			t.Error("buildConsumerConfig() = nil error with session.timeout >= max.poll.interval, want error")
		}
	})
}

func TestBuildProducerConfig(t *testing.T) {
	t.Run("acks and compression and timeouts map to librdkafka keys", func(t *testing.T) {
		cm := buildProducerConfig(&ProducerConfig{
			Brokers: []string{"b:9092"}, Acks: AcksAll, Compression: CompressionZSTD,
			Idempotent: true, ConnectionTimeout: 5_000_000_000, RequestTimeout: 30_000_000_000,
		})
		for k, w := range map[string]any{
			"acks": -1, "compression.type": "zstd", "enable.idempotence": true,
			"socket.connection.setup.timeout.ms": 5000, "request.timeout.ms": 30000,
		} {
			v, err := cm.Get(k, nil)
			if err != nil || v != w {
				t.Errorf("key %q = %v (%v), want %v", k, v, err, w)
			}
		}
	})
	t.Run("empty and none compression omit compression.type", func(t *testing.T) {
		for name, c := range map[string]Compression{"empty": "", "none": CompressionNone} {
			cm := buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"}, Compression: c})
			if v, _ := cm.Get("compression.type", nil); v != nil {
				t.Errorf("%s: compression.type = %v, want omitted", name, v)
			}
		}
	})
	t.Run("zero timeouts and none compression omit their keys", func(t *testing.T) {
		cm := buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"}})
		if v, _ := cm.Get("request.timeout.ms", nil); v != nil {
			t.Errorf("request.timeout.ms should be omitted when zero, got %v", v)
		}
	})
}

func TestDLQTimeout(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ConsumerConfig
		want time.Duration
	}{
		{"no DLQ", &ConsumerConfig{}, 0},
		{"default", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}}, 30 * time.Second},
		{"raw int", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"message.timeout.ms": 5000}}, 5 * time.Second},
		{"raw string", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"message.timeout.ms": "5000"}}, 5 * time.Second},
		{"separate cluster uses DLQ raw", &ConsumerConfig{Raw: map[string]any{"message.timeout.ms": 5000},
			DLQ: &DLQConfig{Topic: "d", Brokers: []string{"x:9092"}}}, 30 * time.Second},
		{"delivery.timeout.ms alias", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"delivery.timeout.ms": 7000}}, 7 * time.Second},
		{"delivery.timeout.ms alias as string", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"delivery.timeout.ms": "7000"}}, 7 * time.Second},
		{"message.timeout.ms wins over alias", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"},
			Raw: map[string]any{"message.timeout.ms": 5000, "delivery.timeout.ms": 7000}}, 5 * time.Second},
		{"unparsable string falls back", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"message.timeout.ms": "soon"}}, 30 * time.Second},
		{"other type falls back", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"message.timeout.ms": 5.5}}, 30 * time.Second},
		{"zero falls back", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"message.timeout.ms": 0}}, 30 * time.Second},
		{"negative falls back", &ConsumerConfig{DLQ: &DLQConfig{Topic: "d"}, Raw: map[string]any{"message.timeout.ms": -1}}, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.dlqTimeout(); got != tt.want {
				t.Errorf("dlqTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSASLConfig_RedactsPassword(t *testing.T) {
	s := &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "s3cret"}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("cfg", "sasl", s)

	// encoding/json, directly and nested: slog's JSON handler marshals a
	// struct attribute with encoding/json, which bypasses LogValue.
	jsonOf := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("json.Marshal(%T): %v", v, err)
		}
		return string(b)
	}
	var jsonBuf bytes.Buffer
	slog.New(slog.NewJSONHandler(&jsonBuf, nil)).Info("cfg", "dlq", &DLQConfig{Topic: "d", SASL: s})
	type userConfig struct {
		Name string
		Auth *SASLConfig
	}

	for _, out := range []string{fmt.Sprintf("%v", s), fmt.Sprintf("%+v", s), fmt.Sprintf("%v", *s),
		fmt.Sprintf("%#v", s), fmt.Sprintf("%#v", *s), buf.String(),
		jsonOf(*s), jsonOf(s), jsonBuf.String(), jsonOf(userConfig{Name: "app", Auth: s})} {
		if strings.Contains(out, "s3cret") || !strings.Contains(out, "[REDACTED]") {
			t.Errorf("password leaked or not redacted in %q", out)
		}
	}
	if out := jsonOf(s); !strings.Contains(out, `"Mechanism":"PLAIN"`) || !strings.Contains(out, `"Username":"u"`) {
		t.Errorf("json = %s, want mechanism and username kept", out)
	}

	// Decoding keeps the default behaviour: a JSON password is read in.
	var decoded SASLConfig
	if err := json.Unmarshal([]byte(`{"Mechanism":"PLAIN","Username":"u","Password":"p"}`), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded != (SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"}) {
		t.Errorf("decoded = {%s %s passwordFilled=%v}, want {PLAIN u passwordFilled=true}",
			decoded.Mechanism, decoded.Username, decoded.Password == "p")
	}
}

func TestWarnPlaintextSASL(t *testing.T) {
	const password = "hunter2-secret"
	sasl := &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: password}
	tests := []struct {
		name string
		cc   connConfig
		want bool
	}{
		{"sasl without ssl warns", connConfig{Brokers: []string{"b"}, SASL: sasl}, true},
		{"sasl with ssl is silent", connConfig{Brokers: []string{"b"}, SASL: sasl, SSL: true}, false},
		{"raw sasl_ssl is silent", connConfig{Brokers: []string{"b"}, SASL: sasl,
			Raw: map[string]any{"security.protocol": "sasl_ssl"}}, false},
		{"no sasl is silent", connConfig{Brokers: []string{"b"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			warnPlaintextSASL(slog.New(slog.NewTextHandler(&buf, nil)), tt.cc)
			if got := strings.Count(buf.String(), "level=WARN"); (got == 1) != tt.want || got > 1 {
				t.Errorf("warn records = %d, want warned=%v (log: %q)", got, tt.want, buf.String())
			}
			if strings.Contains(buf.String(), password) || strings.Contains(buf.String(), "username") {
				t.Errorf("warning must not contain credentials: %q", buf.String())
			}
		})
	}
}

func TestWarnPlaintextSASL_NilLogger(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	warnPlaintextSASL(nil, connConfig{Brokers: []string{"b"}, SASL: &SASLConfig{Mechanism: "PLAIN"}})
	// Raw-forced sasl_plaintext without a SASLConfig must not panic either.
	warnPlaintextSASL(nil, connConfig{Brokers: []string{"b"}, Raw: map[string]any{"security.protocol": "sasl_plaintext"}})
	if got := strings.Count(buf.String(), "level=WARN"); got != 2 {
		t.Errorf("warn records = %d, want 2 (log: %q)", got, buf.String())
	}
}

func TestConfig_IgnoredDLQConnFields(t *testing.T) {
	sasl := &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"}
	tests := []struct {
		name string
		dlq  *DLQConfig
		want []string
	}{
		{"no DLQ", nil, nil},
		{"topic only", &DLQConfig{Topic: "d"}, nil},
		{"all three without brokers", &DLQConfig{Topic: "d", SSL: true, SASL: sasl, Raw: map[string]any{"x": 1}},
			[]string{"SSL", "SASL", "Raw"}},
		{"raw only without brokers", &DLQConfig{Topic: "d", Raw: map[string]any{"x": 1}}, []string{"Raw"}},
		{"empty raw map is not set", &DLQConfig{Topic: "d", Raw: map[string]any{}}, nil},
		{"brokers set uses them", &DLQConfig{Topic: "d", Brokers: []string{"b"}, SSL: true, SASL: sasl,
			Raw: map[string]any{"x": 1}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ignoredDLQConnFields(tt.dlq); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ignoredDLQConnFields() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfig_WarnIgnoredDLQConn(t *testing.T) {
	const password = "hunter2-secret"
	var buf bytes.Buffer
	warnIgnoredDLQConn(slog.New(slog.NewTextHandler(&buf, nil)), &DLQConfig{Topic: "d",
		SASL: &SASLConfig{Mechanism: "PLAIN", Username: "dlq-user", Password: password},
		Raw:  map[string]any{"message.timeout.ms": 5000}})
	out := buf.String()
	if got := strings.Count(out, "level=WARN"); got != 1 {
		t.Fatalf("warn records = %d, want 1 (log: %q)", got, out)
	}
	if !strings.Contains(out, "SASL") || !strings.Contains(out, "Raw") || !strings.Contains(out, "Brokers") {
		t.Errorf("warning %q must name the ignored fields (SASL, Raw) and DLQConfig.Brokers", out)
	}
	for _, secret := range []string{password, "dlq-user", "5000"} {
		if strings.Contains(out, secret) {
			t.Errorf("warning %q must not log field values (found %q)", out, secret)
		}
	}

	buf.Reset()
	warnIgnoredDLQConn(slog.New(slog.NewTextHandler(&buf, nil)), &DLQConfig{Topic: "d"})
	warnIgnoredDLQConn(slog.New(slog.NewTextHandler(&buf, nil)), nil)
	if buf.Len() != 0 {
		t.Errorf("nothing ignored must log nothing, got %q", buf.String())
	}
}
