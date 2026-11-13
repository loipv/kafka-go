package kafka

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

func TestConsumerOptions_NormalizeRetry(t *testing.T) {
	def := RetryConfig{MaxRetries: 3, InitialInterval: time.Second, MaxInterval: 30 * time.Second, Multiplier: 2}
	if got := normalizeRetry(nil); got != def {
		t.Errorf("normalizeRetry(nil) = %+v, want %+v", got, def)
	}
	if got := normalizeRetry(&RetryConfig{}); got != def {
		t.Errorf("normalizeRetry(zero) = %+v, want defaults", got)
	}
	got := normalizeRetry(&RetryConfig{MaxRetries: 5, InitialInterval: 10 * time.Millisecond, SkipOnMaxRetries: true})
	want := RetryConfig{MaxRetries: 5, InitialInterval: 10 * time.Millisecond, MaxInterval: 30 * time.Second, Multiplier: 2, SkipOnMaxRetries: true}
	if got != want {
		t.Errorf("normalizeRetry(partial) = %+v, want %+v", got, want)
	}
}

type consumerOptionCase struct {
	name string
	opt  ConsumerOption
	got  func(*ConsumerConfig) any
	want any
}

func runConsumerOptionCases(t *testing.T, cases []consumerOptionCase) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := newDefaultConsumerConfig()
			tt.opt(c)
			if got := tt.got(c); !sameValue(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestConsumerOptions_Connection(t *testing.T) {
	runConsumerOptionCases(t, []consumerOptionCase{
		{"brokers single", ConsumerWithBrokers("a:9092"), func(c *ConsumerConfig) any { return c.Brokers }, []string{"a:9092"}},
		{"brokers several", ConsumerWithBrokers("a:9092", "b:9092"), func(c *ConsumerConfig) any { return c.Brokers }, []string{"a:9092", "b:9092"}},
		{"group id", ConsumerWithGroupID("g1"), func(c *ConsumerConfig) any { return c.GroupID }, "g1"},
		{"topics single", ConsumerWithTopics("t1"), func(c *ConsumerConfig) any { return c.Topics }, []string{"t1"}},
		{"topics several", ConsumerWithTopics("t1", "t2"), func(c *ConsumerConfig) any { return c.Topics }, []string{"t1", "t2"}},
	})
}

func TestConsumerOptions_Security(t *testing.T) {
	sasl := &SASLConfig{Mechanism: "SCRAM-SHA-256", Username: "u", Password: "p"}
	runConsumerOptionCases(t, []consumerOptionCase{
		{"ssl on", ConsumerWithSSL(true), func(c *ConsumerConfig) any { return c.SSL }, true},
		{"sasl", ConsumerWithSASL(sasl), func(c *ConsumerConfig) any { return c.SASL }, sasl},
	})
}

func TestConsumerOptions_Session(t *testing.T) {
	runConsumerOptionCases(t, []consumerOptionCase{
		{"session timeout", ConsumerWithSessionTimeout(45 * time.Second), func(c *ConsumerConfig) any { return c.SessionTimeout }, 45 * time.Second},
		{"heartbeat interval", ConsumerWithHeartbeatInterval(2 * time.Second), func(c *ConsumerConfig) any { return c.HeartbeatInterval }, 2 * time.Second},
		{"rebalance timeout", ConsumerWithRebalanceTimeout(90 * time.Second), func(c *ConsumerConfig) any { return c.RebalanceTimeout }, 90 * time.Second},
	})
}

func TestConsumerOptions_Batch(t *testing.T) {
	runConsumerOptionCases(t, []consumerOptionCase{
		{"batch size", ConsumerWithBatchSize(250), func(c *ConsumerConfig) any { return c.BatchSize }, 250},
		{"batch timeout", ConsumerWithBatchTimeout(750 * time.Millisecond), func(c *ConsumerConfig) any { return c.BatchTimeout }, 750 * time.Millisecond},
	})
}

func TestConsumerOptions_Commit(t *testing.T) {
	runConsumerOptionCases(t, []consumerOptionCase{
		{"auto commit off", ConsumerWithAutoCommit(false), func(c *ConsumerConfig) any { return c.AutoCommit }, false},
		{"auto commit interval", ConsumerWithAutoCommitInterval(time.Second), func(c *ConsumerConfig) any { return c.AutoCommitInterval }, time.Second},
		{"from beginning", ConsumerWithFromBeginning(true), func(c *ConsumerConfig) any { return c.FromBeginning }, true},
	})
}

func TestConsumerOptions_PartitionAssignor(t *testing.T) {
	runConsumerOptionCases(t, []consumerOptionCase{
		{"range", ConsumerWithPartitionAssignor(AssignorRange), func(c *ConsumerConfig) any { return c.PartitionAssignor }, AssignorRange},
		{"roundrobin", ConsumerWithPartitionAssignor(AssignorRoundRobin), func(c *ConsumerConfig) any { return c.PartitionAssignor }, AssignorRoundRobin},
		{"cooperative-sticky", ConsumerWithPartitionAssignor(AssignorCooperativeSticky), func(c *ConsumerConfig) any { return c.PartitionAssignor }, AssignorCooperativeSticky},
	})
}

func TestConsumerOptions_IdempotencyTTL(t *testing.T) {
	runConsumerOptionCases(t, []consumerOptionCase{
		{"idempotency ttl", ConsumerWithIdempotencyTTL(10 * time.Minute), func(c *ConsumerConfig) any { return c.IdempotencyTTL }, 10 * time.Minute},
	})
}

func TestConsumerOptions_DLQAndRetry(t *testing.T) {
	dlq := &DLQConfig{Topic: "dead", IncludeErrorInfo: true}
	retry := &RetryConfig{MaxRetries: 7, SkipOnMaxRetries: true}
	runConsumerOptionCases(t, []consumerOptionCase{
		{"dlq", ConsumerWithDLQ(dlq), func(c *ConsumerConfig) any { return c.DLQ }, dlq},
		{"retry", ConsumerWithRetry(retry), func(c *ConsumerConfig) any { return c.Retry }, retry},
	})
}

func TestConsumerOptions_LoggingAndTracing(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	tracing := &TracingConfig{Enabled: true, TracerName: "tracer"}
	runConsumerOptionCases(t, []consumerOptionCase{
		{"logger", ConsumerWithLogger(logger), func(c *ConsumerConfig) any { return c.Logger }, logger},
		{"tracing", ConsumerWithTracing(tracing), func(c *ConsumerConfig) any { return c.Tracing }, tracing},
	})
}

func TestConsumerOptions_RawConfig(t *testing.T) {
	raw := map[string]any{"fetch.min.bytes": 1024}
	runConsumerOptionCases(t, []consumerOptionCase{
		{"raw", ConsumerWithRawConfig(raw), func(c *ConsumerConfig) any { return c.Raw }, map[string]any{"fetch.min.bytes": 1024}},
	})
}

// Function-typed fields: the stored function must be the one passed, observed
// by calling it.
func TestConsumerOptions_Callbacks(t *testing.T) {
	t.Run("idempotency key", func(t *testing.T) {
		c := newDefaultConsumerConfig()
		ConsumerWithIdempotencyKey(func(m *Message) string { return "k-" + m.Topic })(c)
		if c.IdempotencyKey == nil {
			t.Fatal("IdempotencyKey not stored")
		}
		if got := c.IdempotencyKey(&Message{Topic: "orders"}); got != "k-orders" {
			t.Errorf("IdempotencyKey() = %q, want %q", got, "k-orders")
		}
	})
	t.Run("error handler", func(t *testing.T) {
		boom := errors.New("boom")
		var seen *Message
		c := newDefaultConsumerConfig()
		ConsumerWithErrorHandler(func(_ context.Context, m *Message, err error) error {
			seen = m
			return err
		})(c)
		if c.ErrorHandler == nil {
			t.Fatal("ErrorHandler not stored")
		}
		msg := &Message{Topic: "t"}
		if got := c.ErrorHandler(context.Background(), msg, boom); !errors.Is(got, boom) || seen != msg {
			t.Errorf("stored handler is not the one passed: returned %v, saw %v", got, seen)
		}
	})
	t.Run("rebalance callback", func(t *testing.T) {
		var got RebalanceEvent
		c := newDefaultConsumerConfig()
		ConsumerWithRebalanceCallback(func(e RebalanceEvent) error {
			got = e
			return nil
		})(c)
		if c.RebalanceCallback == nil {
			t.Fatal("RebalanceCallback not stored")
		}
		ev := RebalanceEvent{Type: RebalanceRevoked, Partitions: []TopicPartition{{Topic: "t", Partition: 2}}}
		if err := c.RebalanceCallback(ev); err != nil {
			t.Fatalf("RebalanceCallback() = %v", err)
		}
		if !reflect.DeepEqual(got, ev) {
			t.Errorf("stored callback saw %+v, want %+v", got, ev)
		}
	})
}

// Options apply in order and each touches only its own field.
func TestConsumerOptions_LastWinsAndIndependent(t *testing.T) {
	c := newDefaultConsumerConfig()
	for _, opt := range []ConsumerOption{
		ConsumerWithTopics("a"), ConsumerWithTopics("b", "c"),
		ConsumerWithAutoCommit(false), ConsumerWithAutoCommit(true),
	} {
		opt(c)
	}
	if !reflect.DeepEqual(c.Topics, []string{"b", "c"}) || !c.AutoCommit {
		t.Errorf("last option must win: Topics=%v AutoCommit=%v", c.Topics, c.AutoCommit)
	}
	if c.GroupID != "" || c.BatchSize != 100 || c.SessionTimeout != 30*time.Second {
		t.Errorf("unrelated fields changed: %+v", c)
	}
}

func TestConsumerConfig_Defaults(t *testing.T) {
	c := newDefaultConsumerConfig()
	durations := []struct {
		name      string
		got, want time.Duration
	}{
		{"SessionTimeout", c.SessionTimeout, 30 * time.Second},
		{"HeartbeatInterval", c.HeartbeatInterval, 3 * time.Second},
		{"RebalanceTimeout", c.RebalanceTimeout, 0},
		{"BatchTimeout", c.BatchTimeout, 5 * time.Second},
		{"IdempotencyTTL", c.IdempotencyTTL, time.Hour},
		{"AutoCommitInterval", c.AutoCommitInterval, 5 * time.Second},
	}
	for _, d := range durations {
		if d.got != d.want {
			t.Errorf("%s = %v, want %v", d.name, d.got, d.want)
		}
	}
	if c.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", c.BatchSize)
	}
	if !c.AutoCommit {
		t.Error("AutoCommit = false, want true")
	}
	if c.PartitionAssignor != PartitionAssignor("range") {
		t.Errorf("PartitionAssignor = %q, want \"range\"", c.PartitionAssignor)
	}
	if c.Brokers != nil || c.GroupID != "" || c.Topics != nil || c.SSL || c.SASL != nil || c.FromBeginning {
		t.Errorf("connection/security/offset defaults must be zero: %+v", c)
	}
	if c.IdempotencyKey != nil || c.DLQ != nil || c.Retry != nil || c.ErrorHandler != nil ||
		c.RebalanceCallback != nil || c.Tracing != nil || c.Logger != nil || c.Raw != nil {
		t.Errorf("optional features must be off by default: %+v", c)
	}
}
