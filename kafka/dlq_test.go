package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
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

	m := c.DLQMetrics()
	if m.Global.MessagesSentToDLQ != 1 {
		t.Errorf("Global.MessagesSentToDLQ = %d, want 1", m.Global.MessagesSentToDLQ)
	}
	if m.ByTopic[source].SentToDLQ != 1 {
		t.Errorf("ByTopic[source].SentToDLQ = %d, want 1", m.ByTopic[source].SentToDLQ)
	}
	if _, ok := m.ByTopic[dlqTopic]; ok {
		t.Errorf("metrics must be keyed by source topic, found entry for DLQ topic %q", dlqTopic)
	}
}

func TestDLQMetricsBlockedGauge(t *testing.T) {
	m := newMetricsCollector()
	m.setBlocked([]TopicPartition{{Topic: "t", Partition: 1}})
	if got := m.getMetrics().BlockedPartitions; len(got) != 1 || got[0].Partition != 1 {
		t.Errorf("BlockedPartitions = %v, want one entry for t[1]", got)
	}
	m.setBlocked(nil)
	if got := m.getMetrics().BlockedPartitions; len(got) != 0 {
		t.Errorf("BlockedPartitions after clear = %v, want empty", got)
	}
}
