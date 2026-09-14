package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

func TestBatchFlushByTimeout(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	got := make(chan int, 4)
	c, err := NewConsumer(ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
		ConsumerWithBatchProcessing(true), ConsumerWithBatchSize(10),
		ConsumerWithBatchTimeout(300*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnBatch(func(_ context.Context, msgs []*Message) error {
		got <- len(msgs)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, _ := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	defer p.Close()
	for i := 0; i < 3; i++ {
		if err := p.Produce(context.Background(), topic, &Message{Value: []byte("x")}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	// Sum the drained batches: the three produces may straddle a flush tick,
	// so asserting the FIRST flush == 3 has a partial-batch flake window.
	total := 0
	waitFor(t, 10*time.Second, func() bool {
		for {
			select {
			case n := <-got:
				total += n
			default:
				return total >= 3
			}
		}
	}, "batch never flushed >=3 messages by timeout")
}

// recvMessages reads n messages from ch before the deadline. The handler-side
// channel is buffered, so the poll loop never blocks on the test.
func recvMessages(t *testing.T, ch <-chan *Message, n int, d time.Duration) []*Message {
	t.Helper()
	deadline := time.Now().Add(d)
	var out []*Message
	for time.Now().Before(deadline) && len(out) < n {
		select {
		case m := <-ch:
			out = append(out, m)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if len(out) < n {
		t.Fatalf("received %d messages, want %d within %v", len(out), n, d)
	}
	return out
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	got := make(chan *Message, 4)
	c.OnMessage(func(_ context.Context, m *Message) error { got <- m; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	sent := &Message{
		Key:     []byte("round-trip-key"),
		Value:   []byte("round-trip-value"),
		Headers: Headers{"h1": []byte("v1"), "h2": []byte("v2")},
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := p.Produce(pctx, topic, sent); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	msg := recvMessages(t, got, 1, 15*time.Second)[0]
	if string(msg.Key) != string(sent.Key) {
		t.Errorf("Key = %q, want %q", msg.Key, sent.Key)
	}
	if string(msg.Value) != string(sent.Value) {
		t.Errorf("Value = %q, want %q", msg.Value, sent.Value)
	}
	if len(msg.Headers) != len(sent.Headers) {
		t.Fatalf("Headers = %v, want %v", msg.Headers, sent.Headers)
	}
	for k, v := range sent.Headers {
		if string(msg.Headers[k]) != string(v) {
			t.Errorf("header %q = %q, want %q", k, msg.Headers[k], v)
		}
	}
	if msg.Topic != topic {
		t.Errorf("Topic = %q, want %q", msg.Topic, topic)
	}
	if msg.Partition != 0 {
		t.Errorf("Partition = %d, want 0", msg.Partition)
	}
	if msg.Offset != 0 {
		t.Errorf("Offset = %d, want 0", msg.Offset)
	}
}

func TestProduceMultiTopicBatch(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topicA := uniqueTopic(t, mc, 1)
	topicB := uniqueTopic(t, mc, 1)

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topicA, topicB),
		ConsumerWithFromBeginning(true),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	got := make(chan *Message, 4)
	c.OnMessage(func(_ context.Context, m *Message) error { got <- m; return nil })
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
	err = p.ProduceMultiTopicBatch(pctx, []TopicBatch{
		{Topic: topicA, Messages: []*Message{{Key: []byte("a"), Value: []byte("from-a")}}},
		{Topic: topicB, Messages: []*Message{{Key: []byte("b"), Value: []byte("from-b")}}},
	})
	if err != nil {
		t.Fatalf("ProduceMultiTopicBatch: %v", err)
	}

	msgs := recvMessages(t, got, 2, 15*time.Second)
	byTopic := map[string]*Message{}
	for _, m := range msgs {
		byTopic[m.Topic] = m
	}
	for topic, want := range map[string]string{topicA: "from-a", topicB: "from-b"} {
		m, ok := byTopic[topic]
		if !ok {
			t.Fatalf("no message consumed from topic %s", topic)
		}
		if string(m.Value) != want {
			t.Errorf("%s value = %q, want %q", topic, m.Value, want)
		}
	}
}

func TestOnGroupedBatchPreservesFirstSeenKeyOrder(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
		ConsumerWithBatchProcessing(true), ConsumerWithBatchSize(10),
		ConsumerWithBatchTimeout(300*time.Millisecond), ConsumerWithGroupByKey(true),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	groups := make(chan []GroupedBatch, 4)
	c.OnGroupedBatch(func(_ context.Context, gs []GroupedBatch) error { groups <- gs; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	// One partition keeps the c,a,c,b,a production order stable in the log.
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	keys := []string{"c", "a", "c", "b", "a"}
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	for _, k := range keys {
		if err := p.Produce(pctx, topic, &Message{Key: []byte(k), Value: []byte(k)}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	// Merge flushes in arrival order: each flush independently preserves
	// first-seen order, so the merged sequence must be c,a,b even if the
	// five produces straddle a flush tick.
	var order []string
	counts := map[string]int{}
	total := 0
	deadline := time.Now().Add(15 * time.Second)
	for total < len(keys) && time.Now().Before(deadline) {
		select {
		case gs := <-groups:
			for _, g := range gs {
				if _, seen := counts[g.Key]; !seen {
					order = append(order, g.Key)
				}
				counts[g.Key] += len(g.Messages)
				total += len(g.Messages)
			}
		case <-time.After(50 * time.Millisecond):
		}
	}
	if total != len(keys) {
		t.Fatalf("grouped handler saw %d messages, want %d", total, len(keys))
	}
	wantOrder := []string{"c", "a", "b"}
	if len(order) != len(wantOrder) {
		t.Fatalf("group order = %v, want %v", order, wantOrder)
	}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Fatalf("group order = %v, want %v (first-seen)", order, wantOrder)
		}
	}
	for key, want := range map[string]int{"c": 2, "a": 2, "b": 1} {
		if counts[key] != want {
			t.Errorf("group %q got %d messages, want %d", key, counts[key], want)
		}
	}
}

// Proof #3: explicit Commit with NO rebalance callback (dead in v1.0.0) —
// the committed offset persists and a fresh consumer in the same group
// resumes right after it.
func TestCommitResumeWithoutRebalanceCallback(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	newC := func() *Consumer {
		c, err := NewConsumer(
			ConsumerWithBrokers(mc.BootstrapServers()),
			ConsumerWithGroupID(group), ConsumerWithTopics(topic),
			ConsumerWithFromBeginning(true),
			ConsumerWithAutoCommit(false),
			// Short session: the mock coordinator completes B's join only
			// after A's session expires, so the default 30s would stall the
			// handover for the whole recv window.
			ConsumerWithSessionTimeout(6*time.Second),
			ConsumerWithHeartbeatInterval(2*time.Second),
			// NO ConsumerWithRebalanceCallback — that is the point.
		)
		if err != nil {
			t.Fatalf("NewConsumer: %v", err)
		}
		return c
	}

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	produce := func() {
		pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer pcancel()
		for i := 0; i < 5; i++ {
			if err := p.Produce(pctx, topic, &Message{Value: []byte("v")}); err != nil {
				t.Fatalf("Produce: %v", err)
			}
		}
	}

	// First run: consume exactly the 5 messages that exist, commit them.
	produce()
	a := newC()
	gotA := make(chan *Message, 8)
	a.OnMessage(func(_ context.Context, m *Message) error { gotA <- m; return nil })
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	go a.Start(ctxA)
	// Registered right after Start so a failed assertion below still tears A
	// down. The explicit mid-test Close below wins the CAS; this deferred one
	// is a no-op by then.
	defer a.Close(context.Background())
	first := recvMessages(t, gotA, 5, 15*time.Second)
	if err := a.Commit(first...); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 0) == 5
	}, "explicit Commit without a rebalance callback never persisted offset 5")
	cancelA()
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("close first consumer: %v", err)
	}

	// Second run, same group: only offsets 5..9 may arrive.
	produce()
	b := newC()
	gotB := make(chan *Message, 8)
	b.OnMessage(func(_ context.Context, m *Message) error { gotB <- m; return nil })
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go b.Start(ctxB)
	defer b.Close(context.Background())

	resumed := recvMessages(t, gotB, 5, 15*time.Second)
	for i, m := range resumed {
		if m.Offset != int64(5+i) {
			t.Errorf("resumed message %d has offset %d, want %d (group must resume at committed 5)", i, m.Offset, 5+i)
		}
	}
}

// Proof #2: a failing handler with DLQ config parks via a CONFIRMED DLQ
// produce — the DLQ copy carries the diagnostic headers and the source
// offset then advances.
func TestDLQRoundTrip(t *testing.T) {
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
			MaxRetries: 1, RetryDelay: 10 * time.Millisecond, RetryBackoffMultiplier: 1,
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

// Proof #8b: when the DLQ produce cannot be CONFIRMED (broker unreachable)
// the source partition BLOCKS — the message is never dropped. The handler is
// gated so the poison message is fetched while the broker is up; the broker
// then goes down before the handler starts failing, so every DLQ produce
// attempt times out (message.timeout.ms is pinned low for that).
func TestDLQBrokerDown_BlocksInsteadOfDropping(t *testing.T) {
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
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic, MaxRetries: 0,
			RetryDelay: time.Millisecond, RetryBackoffMultiplier: 1}),
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
		for _, tp := range c.DLQMetrics().BlockedPartitions {
			if tp.Topic == source && tp.Partition == 0 {
				return true
			}
		}
		return false
	}, "source partition never blocked while the DLQ broker was down")
	if got := c.DLQMetrics().Global.MessagesSentToDLQ; got != 0 {
		t.Errorf("MessagesSentToDLQ = %d with the broker down, want 0 (no confirmed produce)", got)
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

// The Seek/pause buffer caveat: while partition 0 sits blocked, messages
// queued behind it must never let the committed offset drift past the
// blocked message — and the sibling partition keeps flowing.
func TestBlockedPartition_NoOffsetDrift(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 2)
	group := uniqueGroupName(t)

	var p1Count int32
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(_ context.Context, m *Message) error {
		if m.Partition == 0 {
			return errors.New("partition 0 always fails") // stays unparkable
		}
		atomic.AddInt32(&p1Count, 1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	// The library producer always uses PartitionAny; explicit partitions need
	// the raw confluent producer.
	raw, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": mc.BootstrapServers()})
	if err != nil {
		t.Fatalf("raw producer: %v", err)
	}
	defer raw.Close()
	produce := func(part int32, n int) {
		for i := 0; i < n; i++ {
			_ = raw.Produce(&ckafka.Message{
				TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: part},
				Value:          []byte("v"),
			}, nil)
		}
		raw.Flush(5000)
	}
	produce(0, 1) // the poison message: offset 0 on partition 0
	produce(1, 1)

	waitFor(t, 10*time.Second, func() bool { return atomic.LoadInt32(&p1Count) == 1 },
		"partition 1 message never processed while partition 0 was failing")
	waitFor(t, 10*time.Second, func() bool {
		for _, tp := range c.DLQMetrics().BlockedPartitions {
			if tp.Topic == topic && tp.Partition == 0 {
				return true
			}
		}
		return false
	}, "partition 0 never reported blocked")
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 1) >= 1
	}, "partition 1 committed offset never advanced")

	// Queue more messages behind the blocked one, then watch for drift: a
	// committed offset of 1 on partition 0 would mean the blocked message at
	// offset 0 was silently "dealt with".
	produce(0, 3)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 1 {
			t.Fatalf("partition 0 committed offset = %d while message 0 is blocked, want no drift", off)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if off := committedOffset(t, mc.BootstrapServers(), group, topic, 1); off < 1 {
		t.Errorf("partition 1 committed offset = %d, want >= 1 (sibling unaffected)", off)
	}
}

func TestHealthCheckAgainstMockCluster(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	hc, err := NewHealthChecker(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	defer hc.Close()
	hc.SetTimeout(2 * time.Second) // DOWN must be detected well inside waitFor

	if res := hc.Check(context.Background()); res.Status != HealthStatusUp {
		t.Fatalf("Check() = %s (%s), want UP", res.Status, res.Error)
	}

	// MockCluster broker ids start at 1 (not 0).
	if err := mc.SetBrokerDown(1); err != nil {
		t.Fatalf("SetBrokerDown: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return hc.Check(context.Background()).Status == HealthStatusDown
	}, "Check() never reported DOWN with the only broker down")
}
