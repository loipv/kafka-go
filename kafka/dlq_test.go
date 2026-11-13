package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestIdempotencyStore(t *testing.T) {
	t.Run("duplicate within ttl", func(t *testing.T) {
		s := newIdempotencyStore(time.Minute)
		defer s.close()
		s.add("k")
		if !s.isDuplicate("k") {
			t.Error("isDuplicate(k) = false after Add")
		}
		if s.isDuplicate("other") {
			t.Error("isDuplicate(other) = true, want false")
		}
	})
	t.Run("expired keys are not duplicates", func(t *testing.T) {
		s := newIdempotencyStore(40 * time.Millisecond)
		defer s.close()
		s.add("k")
		time.Sleep(120 * time.Millisecond) // > ttl + one cleanup tick (ttl/10)
		if s.isDuplicate("k") {
			t.Error("isDuplicate(k) = true after ttl expiry")
		}
	})
	t.Run("zero ttl is clamped, not panicked", func(t *testing.T) {
		s := newIdempotencyStore(0) // used to panic: time.NewTicker(0)
		defer s.close()
		s.add("k")
		if !s.isDuplicate("k") {
			t.Error("isDuplicate(k) = false with clamped ttl")
		}
	})
	t.Run("double close does not panic", func(_ *testing.T) {
		s := newIdempotencyStore(time.Minute)
		s.close()
		s.close() // used to panic: close of closed channel
	})
}

func TestMessageSetHeader(t *testing.T) {
	var m Message // nil Headers
	m.SetHeader("k", []byte("v"))
	if string(m.Headers["k"]) != "v" {
		t.Errorf("SetHeader on nil map: got %v", m.Headers)
	}
	m.SetHeader("k", []byte("v2"))
	if len(m.Headers) != 1 || string(m.Headers["k"]) != "v2" {
		t.Errorf("SetHeader overwrite: got %v", m.Headers)
	}
}

// A DLQ-parked message is counted under its SOURCE topic, never the DLQ topic.
func TestDLQ_MetricsKeyedBySourceTopic(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	source := uniqueTopic(t, mc, 1)
	dlqTopic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(source),
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic, IncludeErrorInfo: true}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return errors.New("handler always fails") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := p.Produce(pctx, source, &Message{Value: []byte("poison")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	waitFor(t, 20*time.Second, func() bool {
		return topicHighWatermark(t, mc.BootstrapServers(), dlqTopic) == 1
	}, "message never arrived on the DLQ topic")
	// The metric is recorded on the delivery report, which precedes the offset
	// store; wait for the parked source offset so the counter is settled.
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, source, 0) >= 1
	}, "source offset never advanced past the DLQ-parked message")

	m := c.Metrics()
	if m.SentToDLQ != 1 {
		t.Errorf("SentToDLQ = %d, want 1", m.SentToDLQ)
	}
	if m.ByTopic[source].SentToDLQ != 1 {
		t.Errorf("ByTopic[source].SentToDLQ = %d, want 1", m.ByTopic[source].SentToDLQ)
	}
	if _, ok := m.ByTopic[dlqTopic]; ok {
		t.Errorf("metrics must be keyed by source topic, found entry for DLQ topic %q", dlqTopic)
	}
}

func TestDLQ_BatchFailureSendsWholeBatch(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	source := uniqueTopic(t, mc, 1)
	dlqTopic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(source),
		ConsumerWithBatchSize(5), ConsumerWithBatchTimeout(200*time.Millisecond),
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnBatch(func(context.Context, []*Message) error { return errors.New("batch always fails") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	for i := range 5 {
		if err := p.Produce(pctx, source, &Message{Value: []byte{byte('0' + i)}}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	waitFor(t, 20*time.Second, func() bool {
		return topicHighWatermark(t, mc.BootstrapServers(), dlqTopic) == 5
	}, "the whole batch never arrived on the DLQ topic")
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, source, 0) == 5
	}, "source offset never advanced past the DLQ-parked batch")
	if got := c.Metrics().SentToDLQ; got != 5 {
		t.Errorf("SentToDLQ = %d, want 5", got)
	}
}

func TestMetricsSnapshotCounts(t *testing.T) {
	m := newMetricsCollector()
	m.incHandlerFailures("a")
	m.incHandlerFailures("a")
	m.incHandlerFailures("b")
	m.incSentToDLQ("a")
	got := m.snapshot()
	if got.HandlerFailures != 3 || got.SentToDLQ != 1 {
		t.Errorf("totals = (%d failures, %d dlq), want (3, 1)", got.HandlerFailures, got.SentToDLQ)
	}
	if a := got.ByTopic["a"]; a != (TopicMetrics{HandlerFailures: 2, SentToDLQ: 1}) {
		t.Errorf("ByTopic[a] = %+v, want {2 1}", a)
	}
	if b := got.ByTopic["b"]; b != (TopicMetrics{HandlerFailures: 1}) {
		t.Errorf("ByTopic[b] = %+v, want {1 0}", b)
	}
}

func TestMetricsBlockedGauge(t *testing.T) {
	m := newMetricsCollector()
	m.setBlocked([]TopicPartition{{Topic: "t", Partition: 1}})
	if got := m.snapshot().BlockedPartitions; len(got) != 1 || got[0].Partition != 1 {
		t.Errorf("BlockedPartitions = %v, want one entry for t[1]", got)
	}
	m.setBlocked(nil)
	if got := m.snapshot().BlockedPartitions; len(got) != 0 {
		t.Errorf("BlockedPartitions after clear = %v, want empty", got)
	}
}

func TestDLQProducerConfig(t *testing.T) {
	cm := dlqProducerConfig(connConfig{Brokers: []string{"b:9092"}})
	if cm["acks"] != -1 || cm["message.timeout.ms"] != 30000 {
		t.Errorf("defaults = acks %v, timeout %v; want -1, 30000", cm["acks"], cm["message.timeout.ms"])
	}
	cm = dlqProducerConfig(connConfig{Brokers: []string{"b:9092"}, Raw: map[string]any{"message.timeout.ms": 5000}})
	if cm["message.timeout.ms"] != 5000 {
		t.Errorf("raw override lost: %v", cm["message.timeout.ms"])
	}
	// delivery.timeout.ms is a librdkafka alias of message.timeout.ms: the
	// user's alias must not be shadowed by our default.
	cm = dlqProducerConfig(connConfig{Brokers: []string{"b:9092"}, Raw: map[string]any{"delivery.timeout.ms": 7000}})
	if _, ok := cm["message.timeout.ms"]; ok || cm["delivery.timeout.ms"] != 7000 {
		t.Errorf("alias config = %v, want delivery.timeout.ms=7000 and no message.timeout.ms", cm)
	}
	cm = dlqProducerConfig(connConfig{Brokers: []string{"b:9092"},
		Raw: map[string]any{"message.timeout.ms": 5000, "delivery.timeout.ms": 7000}})
	if cm["message.timeout.ms"] != 5000 {
		t.Errorf("message.timeout.ms = %v, want 5000 (it wins over the alias)", cm["message.timeout.ms"])
	}
}

func TestTruncateUTF8(t *testing.T) {
	if got := truncateUTF8("short", 1024); got != "short" {
		t.Errorf("short string changed: %q", got)
	}
	if got := truncateUTF8(strings.Repeat("a", 2000), 1024); len(got) != 1024 {
		t.Errorf("len = %d, want 1024", len(got))
	}
	got := truncateUTF8(strings.Repeat("a", 1023)+"é"+"tail", 1024) // "é" is 2 bytes, straddles the limit
	if len(got) != 1023 || !utf8.ValidString(got) {
		t.Errorf("len = %d valid = %v, want 1023 bytes of valid UTF-8", len(got), utf8.ValidString(got))
	}
}

func TestConfirmDLQEvent(t *testing.T) {
	topic := "dlq"
	ok := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic}, Opaque: "orders"}
	failed := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Error: errors.New("boom")}, Opaque: "orders"}

	if source, err := confirmDLQEvent(ok); err != nil || source != "orders" {
		t.Errorf("confirmed message = (%q, %v), want (orders, nil)", source, err)
	}
	if _, err := confirmDLQEvent(failed); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("failed message err = %v, want one wrapping boom", err)
	}
	// An event that is not a delivery report confirms nothing: it must be an
	// error, never a silent success.
	if _, err := confirmDLQEvent(ckafka.NewError(ckafka.ErrFail, "x", false)); err == nil {
		t.Error("non-message event err = nil, want an error")
	}
}
