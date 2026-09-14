package kafka

import (
	"context"
	"testing"
	"time"
)

func TestProduceDeliveryReportOK(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Produce(ctx, topic, &Message{Key: []byte("k1"), Value: []byte("v1"),
		Headers: Headers{"h": []byte("v")}}); err != nil {
		t.Fatalf("Produce() = %v, want nil", err)
	}
}

func TestProduceAsyncDeliveryErrorHandler(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)

	errs := make(chan error, 4)
	// message.timeout.ms is shortened so the delivery error fires within the
	// waitFor deadline (librdkafka retries until this timeout; 5m default).
	p, err := NewProducer(
		ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"message.timeout.ms": 5000}),
		ProducerWithDeliveryErrorHandler(func(msg *Message, err error) { errs <- err }),
	)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	if err := p.ProduceAsync(topic, &Message{Value: []byte("ok")}); err != nil {
		t.Fatalf("ProduceAsync() = %v, want nil", err)
	}
	p.Flush(10 * time.Second)

	// MockCluster broker ids start at 1 (not 0); newMockCluster creates one broker.
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	if err := p.ProduceAsync(topic, &Message{Value: []byte("bad")}); err != nil {
		t.Fatalf("ProduceAsync() = %v, want nil (failure is async)", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		p.Flush(5 * time.Second)
		return len(errs) > 0
	}, "delivery error handler never fired for failed async produce")
	if e := <-errs; e == nil {
		t.Error("handler received nil error, want delivery failure")
	}
}

func TestProduceBatchReportsFailureWhenBrokerDown(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	// Pin the delivery-ERROR branch: without the shorter message timeout,
	// librdkafka would retry for the 5m default and the test would pass via
	// the ctx-deadline branch instead.
	p, err := NewProducer(
		ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"message.timeout.ms": 5000}),
	)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	// MockCluster broker ids start at 1 (not 0); newMockCluster creates one broker.
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msgs := []*Message{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}}
	err = p.ProduceBatch(ctx, topic, msgs)
	if err == nil {
		t.Fatal("ProduceBatch() = nil with broker down, want joined delivery error")
	}
}
