package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestProducer_DeliveryReportOK(t *testing.T) {
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

func TestProducer_AsyncDeliveryErrorHandler(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)

	errs := make(chan error, 4)
	// message.timeout.ms is shortened so the delivery error fires within the
	// waitFor deadline (librdkafka retries until this timeout; 5m default).
	p, err := NewProducer(
		ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"message.timeout.ms": 5000}),
		ProducerWithDeliveryErrorHandler(func(_ *Message, err error) { errs <- err }),
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

func TestProducer_BatchReportsFailureWhenBrokerDown(t *testing.T) {
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

// A message that can never fit the queue must fail fast, not retry forever.
func TestProducer_ProduceBatchMessageLargerThanQueueFailsFast(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"queue.buffering.max.kbytes": 1}))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	done := make(chan error, 1)
	go func() {
		done <- p.ProduceBatch(context.Background(), topic, []*Message{{Value: make([]byte, 4096)}})
	}()
	select {
	case err := <-done:
		var kerr ckafka.Error
		if !errors.As(err, &kerr) || kerr.Code() != ckafka.ErrQueueFull {
			t.Fatalf("ProduceBatch(oversized message) = %v, want ErrQueueFull", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ProduceBatch did not return within 5s: queue-full retry never terminates")
	}
}

// A flush that times out leaves messages undelivered; Close must say so.
func TestProducer_CloseWarnsWhenFlushTimesOut(t *testing.T) {
	skipIfShort(t)
	old := closeFlushTimeout
	closeFlushTimeout = 500 * time.Millisecond
	t.Cleanup(func() { closeFlushTimeout = old })

	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	var buf bytes.Buffer
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	if err := p.ProduceAsync(topic, &Message{Value: []byte("stuck")}); err != nil {
		t.Fatalf("ProduceAsync: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if got := buf.String(); !strings.Contains(got, "level=WARN") || !strings.Contains(got, "remaining=1") {
		t.Errorf("log = %q, want a Warn with remaining=1", got)
	}
}

func TestProducer_RequiresBrokers(t *testing.T) {
	for name, opts := range map[string][]ProducerOption{
		"no options":    nil,
		"empty brokers": {ProducerWithBrokers()},
	} {
		t.Run(name, func(t *testing.T) {
			p, err := NewProducer(opts...)
			if !errors.Is(err, ErrBrokersRequired) {
				if p != nil {
					_ = p.Close()
				}
				t.Fatalf("NewProducer() error = %v, want %v", err, ErrBrokersRequired)
			}
		})
	}
}
