package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// A failing handler with DLQ config parks via a CONFIRMED DLQ
// produce — the DLQ copy carries the diagnostic headers and the source
// offset then advances.
func TestDLQ_RoundTrip(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	source := uniqueTopic(t, mc, 1)
	dlqTopic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	handlerErr := errors.New("handler always fails")
	var errHandlerCalls int32
	c, err := NewConsumer(append(fastAtLeastOnce(
		ConsumerWithErrorHandler(func(_ context.Context, _ *Message, err error) error {
			atomic.AddInt32(&errHandlerCalls, 1)
			return err // recorded, but defer to the DLQ
		}),
	),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(source),
		ConsumerWithDLQ(&DLQConfig{
			Topic: dlqTopic, IncludeErrorInfo: true,
		}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return handlerErr })
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

	// Read the DLQ copy with a scratch consumer.
	scratch, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": mc.BootstrapServers(),
		"group.id":          uniqueGroupName(t),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("scratch consumer: %v", err)
	}
	defer scratch.Close()
	if err := scratch.Subscribe(dlqTopic, nil); err != nil {
		t.Fatalf("subscribe dlq: %v", err)
	}
	var dlqMsg *ckafka.Message
	deadline := time.Now().Add(20 * time.Second)
	for dlqMsg == nil && time.Now().Before(deadline) {
		if m, err := scratch.ReadMessage(200 * time.Millisecond); err == nil {
			dlqMsg = m
		}
	}
	if dlqMsg == nil {
		t.Fatal("message never arrived on the DLQ topic")
	}

	hdrs := map[string][]byte{}
	for _, h := range dlqMsg.Headers {
		hdrs[h.Key] = h.Value
	}
	if got := string(hdrs["x-dlq-original-topic"]); got != source {
		t.Errorf("x-dlq-original-topic = %q, want %q", got, source)
	}
	if msg := string(hdrs["x-dlq-error-message"]); !strings.Contains(msg, handlerErr.Error()) {
		t.Errorf("x-dlq-error-message = %q, want it to contain %q", msg, handlerErr.Error())
	}
	attempts := 2 // fastAtLeastOnce pins Retry.MaxRetries=1 → 2 handler invocations
	if got := string(hdrs["x-dlq-handler-retry-count"]); got != strconv.Itoa(attempts) {
		t.Errorf("x-dlq-handler-retry-count = %q, want %q", got, strconv.Itoa(attempts))
	}
	if string(dlqMsg.Value) != "poison" {
		t.Errorf("dlq value = %q, want the original payload", dlqMsg.Value)
	}
	if n := atomic.LoadInt32(&errHandlerCalls); n == 0 {
		t.Error("error handler never recorded the failure")
	}

	// Parked via the confirmed DLQ produce: the source offset must advance.
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, source, 0) >= 1
	}, "source offset never advanced past the DLQ-parked message")
}

// DLQ on a separate cluster: the parked copy lands on DLQConfig.Brokers'
// cluster, never on the consumer's.
func TestDLQ_SeparateCluster(t *testing.T) {
	skipIfShort(t)
	src := newMockCluster(t)
	dst := newMockCluster(t)
	source := uniqueTopic(t, src, 1)
	dlqTopic := uniqueTopic(t, dst, 1)
	if err := src.CreateTopic(dlqTopic, 1, 1); err != nil {
		t.Fatalf("CreateTopic on source cluster: %v", err)
	}
	group := uniqueGroupName(t)

	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(src.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(source),
		ConsumerWithDLQ(&DLQConfig{
			Topic: dlqTopic, Brokers: []string{dst.BootstrapServers()},
		}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return errors.New("fail") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(src.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := p.Produce(pctx, source, &Message{Value: []byte("poison")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Parked once the source offset advances — by then the DLQ produce is confirmed.
	waitFor(t, 20*time.Second, func() bool {
		return committedOffset(t, src.BootstrapServers(), group, source, 0) >= 1
	}, "source offset never advanced past the DLQ-parked message")

	if n := topicHighWatermark(t, dst.BootstrapServers(), dlqTopic); n != 1 {
		t.Errorf("DLQ cluster %s high watermark = %d, want 1", dlqTopic, n)
	}
	if n := topicHighWatermark(t, src.BootstrapServers(), dlqTopic); n != 0 {
		t.Errorf("consumer cluster %s high watermark = %d, want 0 (DLQ leaked to source cluster)", dlqTopic, n)
	}
}

// TestDLQ_BrokerDownBlocksInsteadOfDropping pins that when the DLQ produce
// cannot be CONFIRMED (broker unreachable) the source partition BLOCKS — the
// message is never dropped. The handler is
// gated so the poison message is fetched while the broker is up; the broker
// then goes down before the handler starts failing, so every DLQ produce
// attempt times out (message.timeout.ms is pinned low for that).
func TestDLQ_BrokerDownBlocksInsteadOfDropping(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	source := uniqueTopic(t, mc, 1)
	dlqTopic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	// message.timeout.ms flows through connConfig.Raw into the DLQ producer:
	// without it the dead-broker produce would hang on librdkafka's 5m
	// default and the block would never be recorded in the test window.
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(source),
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic}),
		ConsumerWithRawConfig(map[string]any{"message.timeout.ms": 5000}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	c.OnMessage(func(context.Context, *Message) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
		return errors.New("handler always fails")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer c.Close(context.Background()) // LIFO: cancel runs first, unblocking the DLQ produce wait
	defer cancel()
	go c.Start(ctx)

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

	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("poison message never fetched while the broker was up")
	}
	// MockCluster broker ids start at 1 (not 0). Taking the broker down
	// kills source and DLQ alike — with the shared connection config that
	// is exactly the unreachable-DLQ scenario.
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	close(gate) // ...and let the handler start failing

	waitFor(t, 30*time.Second, func() bool {
		for _, tp := range c.Metrics().BlockedPartitions {
			if tp.Topic == source && tp.Partition == 0 {
				return true
			}
		}
		return false
	}, "source partition never blocked while the DLQ broker was down")
	if got := c.Metrics().SentToDLQ; got != 0 {
		t.Errorf("SentToDLQ = %d with the broker down, want 0 (no confirmed produce)", got)
	}

	// Blocked means blocked: no commit for ~3s. With the broker down nothing
	// can commit at all — reading the group offsets fails, reported as -1.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if off := committedOffset(t, mc.BootstrapServers(), group, source, 0); off >= 1 {
			t.Fatalf("committed offset = %d while the DLQ was down, want no advance (block, not drop)", off)
		}
		time.Sleep(200 * time.Millisecond)
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

func TestDLQ_ProducerConfig(t *testing.T) {
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

func TestDLQ_TruncateUTF8(t *testing.T) {
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

func TestDLQ_ConfirmEvent(t *testing.T) {
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

func TestDLQ_TruncateUTF8EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		limit int
		want  string
	}{
		{"zero limit", "abc", 0, ""},
		{"negative limit", "abc", -5, ""},
		{"exact fit is unchanged", "abcd", 4, "abcd"},
		{"3-byte rune straddling the limit is dropped whole", strings.Repeat("a", 3) + "€", 4, "aaa"},
		{"3-byte rune cut after its 2nd byte is dropped whole", strings.Repeat("a", 3) + "€", 5, "aaa"},
		{"4-byte rune straddling the limit is dropped whole", strings.Repeat("a", 2) + "😀", 4, "aa"},
		{"rune ending exactly on the limit is kept", strings.Repeat("a", 3) + "€", 6, "aaa€"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateUTF8(tt.in, tt.limit)
			if got != tt.want || !utf8.ValidString(got) {
				t.Errorf("truncateUTF8(%q, %d) = %q (valid=%v), want %q", tt.in, tt.limit, got, utf8.ValidString(got), tt.want)
			}
		})
	}
}

// The header cap is wired into the DLQ record: an over-long handler error
// reaches the x-dlq-error-message header cut to maxDLQErrorBytes, and the
// caller's Message is left untouched.
func TestDLQ_BuildMessageCapsErrorHeader(t *testing.T) {
	s := &dlqService{config: &DLQConfig{Topic: "dlq", IncludeErrorInfo: true}}
	msg := &Message{Topic: "orders", Value: []byte("v"), Headers: Headers{"h": []byte("1")}}
	cause := errors.New(strings.Repeat("e", 3000))

	out := s.buildDLQMessage(msg, cause, 3)

	hdrs := map[string][]byte{}
	for _, h := range out.Headers {
		hdrs[h.Key] = h.Value
	}
	if got := len(hdrs["x-dlq-error-message"]); got != maxDLQErrorBytes {
		t.Errorf("x-dlq-error-message = %d bytes, want %d", got, maxDLQErrorBytes)
	}
	if got := string(hdrs["x-dlq-handler-retry-count"]); got != "3" {
		t.Errorf("x-dlq-handler-retry-count = %q, want 3", got)
	}
	if got := string(hdrs["h"]); got != "1" {
		t.Errorf("original header h = %q, want it carried over", got)
	}
	if _, ok := msg.Headers["x-dlq-error-message"]; ok {
		t.Error("buildDLQMessage mutated the caller's Message headers")
	}

	s.config.IncludeErrorInfo = false
	for _, h := range s.buildDLQMessage(msg, cause, 1).Headers {
		if h.Key == "x-dlq-error-message" {
			t.Error("x-dlq-error-message present with IncludeErrorInfo=false")
		}
	}
}

// With the broker unreachable the delivery report never arrives before the
// context ends: produceAll must return an error wrapping the context's and
// must not count the message as sent.
func TestDLQ_ProduceAllContextEndsBeforeDeliveryReports(t *testing.T) {
	skipIfShort(t)
	shortDLQCloseFlush(t) // the broker stays down: Close's flush can only time out
	mc := newMockCluster(t)
	dlqTopic := uniqueTopic(t, mc, 1)
	metrics := newMetricsCollector()
	s, err := newDLQService(connConfig{Brokers: []string{mc.BootstrapServers()}}, &DLQConfig{Topic: dlqTopic}, metrics)
	if err != nil {
		t.Fatalf("newDLQService: %v", err)
	}
	defer s.Close()
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	msgs := []*Message{{Topic: "src", Value: []byte("a")}, {Topic: "src", Value: []byte("b")}}
	err = s.produceAll(ctx, msgs, errors.New("boom"), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("produceAll() = %v, want an error wrapping context.DeadlineExceeded", err)
	}
	if got := metrics.snapshot().SentToDLQ; got != 0 {
		t.Errorf("SentToDLQ = %d, want 0 (nothing confirmed)", got)
	}
}

// shortDLQCloseFlush shortens dlqService.Close's flush for one test, for tests
// whose broker is down so the flush can only time out.
func shortDLQCloseFlush(t *testing.T) {
	t.Helper()
	old := dlqCloseFlushTimeout
	dlqCloseFlushTimeout = 500 * time.Millisecond
	t.Cleanup(func() { dlqCloseFlushTimeout = old })
}

// With the DLQ unreachable and a context that never ends, closing the service
// must release a produceAll that is waiting for delivery reports: after Close
// destroys the producer no report can arrive, and the poll loop would hang.
func TestDLQ_ProduceAllReturnsWhenServiceCloses(t *testing.T) {
	skipIfShort(t)
	shortDLQCloseFlush(t)
	mc := newMockCluster(t)
	dlqTopic := uniqueTopic(t, mc, 1)
	s, err := newDLQService(connConfig{Brokers: []string{mc.BootstrapServers()}}, &DLQConfig{Topic: dlqTopic}, newMetricsCollector())
	if err != nil {
		t.Fatalf("newDLQService: %v", err)
	}
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}

	errc := make(chan error, 1)
	go func() {
		errc <- s.produceAll(context.Background(), []*Message{{Topic: "src", Value: []byte("a")}}, errors.New("boom"), 1)
	}()
	waitFor(t, 5*time.Second, func() bool { return s.producer.Len() > 0 }, "the DLQ record was never queued")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, errDLQClosed) {
			t.Errorf("produceAll() = %v, want an error wrapping the DLQ-closed sentinel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("produceAll did not return after the DLQ service closed")
	}
}
