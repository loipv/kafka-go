package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
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

// With the broker down and more messages than the queue holds, ProduceBatch
// waits for queue space; a context deadline must end that wait promptly.
func TestProducer_ProduceBatchBackPressureHonoursContext(t *testing.T) {
	skipIfShort(t)
	old := closeFlushTimeout // the queue never drains: keep Close from flushing for its full default
	closeFlushTimeout = 500 * time.Millisecond
	t.Cleanup(func() { closeFlushTimeout = old })

	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithRawConfig(map[string]any{"queue.buffering.max.messages": 10}))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}

	msgs := make([]*Message, 50)
	for i := range msgs {
		msgs[i] = &Message{Value: []byte("v")}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err = p.ProduceBatch(ctx, topic, msgs)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ProduceBatch() = %v, want an error wrapping context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("ProduceBatch took %v after a 2s deadline, want it to return promptly", d)
	}
}

// Flush after Close must not touch the destroyed librdkafka handle (a cgo
// fault that kills the process); it returns ErrProducerClosed.
func TestProducer_FlushAfterCloseReturnsErrProducerClosed(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Flush(time.Second); !errors.Is(err, ErrProducerClosed) {
		t.Errorf("Flush() after Close = %v, want ErrProducerClosed", err)
	}
}

// A delivery event that is not a message report confirms nothing: it must be
// an error, never a silent success (as for the DLQ, see confirmDLQEvent).
func TestProducer_FinishDeliveryNonMessageEventIsError(t *testing.T) {
	if err := finishDelivery(ckafka.NewError(ckafka.ErrFail, "x", false)); err == nil {
		t.Error("finishDelivery(non-message event) = nil, want an error")
	}
	topic := "t"
	if err := finishDelivery(&ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic}}); err != nil {
		t.Errorf("finishDelivery(delivered message) = %v, want nil", err)
	}
}

// endCountingProvider is a tracer provider whose spans count End calls. The
// SDK ignores a second End, but other providers need not.
type endCountingProvider struct {
	noop.TracerProvider
	ends *int
}

func (p endCountingProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return endCountingTracer{ends: p.ends}
}

type endCountingTracer struct {
	noop.Tracer
	ends *int
}

func (t endCountingTracer) Start(ctx context.Context, _ string, _ ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ctx, endCountingSpan{ends: t.ends}
}

type endCountingSpan struct {
	noop.Span
	ends *int
}

func (s endCountingSpan) End(...trace.SpanEndOption) { *s.ends++ }

// The delivery report is a different *ckafka.Message carrying the same span
// closure, so a message that reported and is then aborted (ctx ended while
// other messages were pending) reaches the closure twice; the span must end
// once.
func TestProducer_SpanEndsOnceWhenReportedThenAborted(t *testing.T) {
	orig := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(orig) })
	var ends int
	otel.SetTracerProvider(endCountingProvider{ends: &ends})

	p := &Producer{tracer: newTracingService(&TracingConfig{Enabled: true})}
	km := p.prepareMessage(context.Background(), "t", &Message{Value: []byte("v")})
	report := *km // librdkafka hands back a copy with the same Opaque
	if err := finishDelivery(&report); err != nil {
		t.Fatalf("finishDelivery: %v", err)
	}
	endSpan(km, context.Canceled) // what abort() does for every message
	if ends != 1 {
		t.Errorf("span End calls = %d, want 1", ends)
	}
}

// drainReports must count a report that is already in the channel as delivered.
func TestProducer_DrainReportsConsumesBufferedReports(t *testing.T) {
	topic := "t"
	ch := make(chan ckafka.Event, 3)
	ch <- &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic}}
	ch <- &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Error: errors.New("boom")}}
	errs, left := drainReports(ch, 3)
	if left != 1 || len(errs) != 1 {
		t.Fatalf("drainReports() = %d errors, %d left; want 1 error, 1 left", len(errs), left)
	}
}

// A Produce still waiting for its delivery report when Close ends must not
// wait for its context: no report can arrive from a closed client.
func TestProducer_AwaitingProduceReturnsClosedAfterClose(t *testing.T) {
	skipIfShort(t)
	old := closeFlushTimeout
	closeFlushTimeout = 300 * time.Millisecond
	t.Cleanup(func() { closeFlushTimeout = old })

	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Produce(context.Background(), topic, &Message{Value: []byte("stuck")}) }()
	waitFor(t, 5*time.Second, func() bool { return p.producer.Len() > 0 }, "the message was never queued")

	if err := p.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrProducerClosed) {
			t.Errorf("Produce() = %v, want an error wrapping ErrProducerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Produce did not return within 5s of Close")
	}
}

// Produce, ProduceBatch and ProduceAsync may race Close (also a doubled Close):
// a call that loses the race reports ErrProducerClosed and nothing touches the
// client once it is gone.
func TestProducer_ConcurrentProduceAndClose(t *testing.T) {
	skipIfShort(t)
	old := closeFlushTimeout
	closeFlushTimeout = 2 * time.Second
	t.Cleanup(func() { closeFlushTimeout = old })

	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	msg := func() *Message { return &Message{Value: []byte("v")} }
	async := func() error { return p.ProduceAsync(topic, msg()) }
	sync1 := func() error { return p.Produce(context.Background(), topic, msg()) }
	batch := func() error { return p.ProduceBatch(context.Background(), topic, []*Message{msg(), msg()}) }
	calls := []func() error{async, async, async, sync1, sync1, batch, batch}

	failures := make(chan error, len(calls))
	finished := make(chan struct{})
	var wg sync.WaitGroup
	for _, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := call()
				var ke ckafka.Error
				if errors.As(err, &ke) && ke.Code() == ckafka.ErrQueueFull {
					time.Sleep(time.Millisecond) // ProduceAsync has no back-pressure
					continue
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(finished) }()

	time.Sleep(200 * time.Millisecond) // let every goroutine get going
	closed := make(chan error, 2)
	for range 2 {
		go func() { closed <- p.Close() }()
	}
	for range 2 {
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("Close() = %v, want nil", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("Close did not return within 15s")
		}
	}
	select {
	case <-finished:
	case <-time.After(15 * time.Second):
		t.Fatal("produce goroutines did not finish within 15s of Close")
	}
	close(failures)
	for err := range failures {
		if !errors.Is(err, ErrProducerClosed) {
			t.Errorf("produce call failed with %v, want an error wrapping ErrProducerClosed", err)
		}
	}
}

// downProducer returns a producer whose broker is unreachable, with Close's
// flush shortened so tests do not wait out the default.
func downProducer(t *testing.T, opts ...ProducerOption) (*Producer, string) {
	t.Helper()
	old := closeFlushTimeout
	closeFlushTimeout = 200 * time.Millisecond
	t.Cleanup(func() { closeFlushTimeout = old })
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, err := NewProducer(append([]ProducerOption{ProducerWithBrokers(mc.BootstrapServers()),
		ProducerWithLogger(slog.New(slog.DiscardHandler))}, opts...)...)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	return p, topic
}

// Close must not destroy the client under a Flush that is still running: it
// waits for that Flush to return.
func TestProducer_CloseWaitsForRunningFlush(t *testing.T) {
	skipIfShort(t)
	p, topic := downProducer(t)
	if err := p.ProduceAsync(topic, &Message{Value: []byte("stuck")}); err != nil {
		t.Fatalf("ProduceAsync: %v", err)
	}
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		_ = p.Flush(1500 * time.Millisecond) // cannot drain: the broker is down
	}()
	time.Sleep(200 * time.Millisecond) // Flush is inside the client

	if err := p.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	select {
	case <-flushed:
	default:
		t.Error("Close returned while Flush was still using the client")
	}
}

// A ProduceBatch stuck in back-pressure (queue full, nothing draining) when
// Close ends must return ErrProducerClosed instead of waiting for its context.
func TestProducer_BatchInBackPressureReturnsClosedAfterClose(t *testing.T) {
	skipIfShort(t)
	p, topic := downProducer(t, ProducerWithRawConfig(map[string]any{"queue.buffering.max.messages": 1}))
	done := make(chan error, 1)
	go func() {
		done <- p.ProduceBatch(context.Background(), topic,
			[]*Message{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}})
	}()
	waitFor(t, 5*time.Second, func() bool { return p.producer.Len() > 0 }, "the first message was never queued")
	time.Sleep(100 * time.Millisecond) // the second message is now waiting for room

	if err := p.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrProducerClosed) {
			t.Errorf("ProduceBatch() = %v, want an error wrapping ErrProducerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ProduceBatch did not return within 5s of Close")
	}
}
