package kafka

import (
	"context"
	"testing"
	"time"
)

func TestProducer_CloseAfterAsyncReturnsPromptly(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := p.ProduceAsync(topic, &Message{Value: []byte("v")}); err != nil {
			t.Fatalf("ProduceAsync: %v", err)
		}
	}
	start := time.Now()
	_ = p.Close()
	d := time.Since(start)
	t.Logf("Close() took %v", d)
	if d > 5*time.Second {
		t.Fatalf("Close() took %v, want < 5s (flush stalled on unread delivery reports)", d)
	}
	if got := topicHighWatermark(t, mc.BootstrapServers(), topic); got != 100 {
		t.Errorf("high watermark = %d, want 100", got)
	}
}

func TestProducer_CloseReportsAsyncFailures(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	errs := make(chan error, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"message.timeout.ms": 2000}),
		ProducerWithDeliveryErrorHandler(func(_ *Message, err error) { errs <- err }))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	if err := p.ProduceAsync(topic, &Message{Value: []byte("bad")}); err != nil {
		t.Fatalf("ProduceAsync: %v", err)
	}
	start := time.Now()
	_ = p.Close()
	t.Logf("Close() took %v", time.Since(start))
	if len(errs) != 1 {
		t.Fatalf("delivery error handler calls during Close = %d, want 1", len(errs))
	}
}

func TestProducer_ProduceBatchLargerThanQueue(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"queue.buffering.max.messages": 100}))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	msgs := make([]*Message, 1000)
	for i := range msgs {
		msgs[i] = &Message{Value: []byte("v")}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.ProduceBatch(ctx, topic, msgs); err != nil {
		t.Fatalf("ProduceBatch(1000 msgs, queue 100) = %v, want nil", err)
	}
	if got := topicHighWatermark(t, mc.BootstrapServers(), topic); got != 1000 {
		t.Errorf("high watermark = %d, want 1000", got)
	}
}
