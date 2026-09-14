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

func TestProduceBatchReportsFailureWhenBrokerDown(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
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
