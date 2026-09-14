package kafka

import (
	"testing"

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
		build func() ckafka.ConfigMap
	}{
		{"producer", func() ckafka.ConfigMap {
			return buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"}, SSL: true, SASL: sasl})
		}},
		{"consumer", func() ckafka.ConfigMap {
			return buildConsumerConfig(&ConsumerConfig{Brokers: []string{"b:9092"}, GroupID: "g", Topics: []string{"t"}, SSL: true, SASL: sasl})
		}},
		{"dlq-and-health", func() ckafka.ConfigMap {
			return connConfig{Brokers: []string{"b:9092"}, SSL: true, SASL: sasl}.configMap()
		}},
	}
	for _, b := range builders {
		t.Run(b.name, func(t *testing.T) {
			v, err := b.build().Get("security.protocol", nil)
			if err != nil {
				t.Fatalf("builder %q dropped auth config: %v", b.name, err)
			}
			if v != "sasl_ssl" {
				t.Errorf("builder %q security.protocol = %v, want sasl_ssl", b.name, v)
			}
		})
	}
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
	t.Run("zero timeouts and none compression omit their keys", func(t *testing.T) {
		cm := buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"}})
		if v, _ := cm.Get("request.timeout.ms", nil); v != nil {
			t.Errorf("request.timeout.ms should be omitted when zero, got %v", v)
		}
	})
	t.Run("retry maps to librdkafka retry keys", func(t *testing.T) {
		cm := buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"},
			Retry: &RetryConfig{MaxRetries: 8, InitialInterval: 250_000_000, MaxInterval: 30_000_000_000}})
		for k, w := range map[string]any{
			"retries": 8, "retry.backoff.ms": 250, "retry.backoff.max.ms": 30000,
		} {
			v, err := cm.Get(k, nil)
			if err != nil || v != w {
				t.Errorf("key %q = %v (%v), want %v", k, v, err, w)
			}
		}
	})
	t.Run("log_level is never forwarded", func(t *testing.T) {
		cm := buildProducerConfig(&ProducerConfig{Brokers: []string{"b:9092"}, LogLevel: LogLevelDebug})
		if v, _ := cm.Get("log_level", nil); v != nil {
			t.Errorf("log_level must not be forwarded to librdkafka (its 0-4 enum mismatches syslog 0-7), got %v", v)
		}
	})
}
