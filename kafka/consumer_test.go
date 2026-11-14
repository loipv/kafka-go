package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConsumer_BatchFlushByTimeout(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	got := make(chan int, 4)
	c, err := NewConsumer(ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
		ConsumerWithBatchSize(10),
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

func TestConsumer_ProduceConsumeRoundTrip(t *testing.T) {
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

func TestConsumer_OnGroupedBatchPreservesFirstSeenKeyOrder(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
		ConsumerWithBatchSize(10),
		ConsumerWithBatchTimeout(300*time.Millisecond),
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

// TestConsumer_CommitResumeWithoutRebalanceCallback pins that an explicit
// Commit with NO rebalance callback (dead in v1.0.0) persists the committed
// offset, and a fresh consumer in the same group resumes right after it.
func TestConsumer_CommitResumeWithoutRebalanceCallback(t *testing.T) {
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

func TestConsumer_CommitOffsets(t *testing.T) {
	msgs := []*Message{
		{Topic: "t", Partition: 0, Offset: 5},
		{Topic: "t", Partition: 0, Offset: 9},
		{Topic: "t", Partition: 1, Offset: 3},
		{Topic: "t", Partition: 2, Offset: 0}, // Kafka's first offset must not be dropped
	}
	got := commitOffsets(msgs)
	// map iteration order is random — compare as a set. (Keyed by
	// topic-string+partition: ckafka.TopicPartition holds a *string, which
	// would compare pointer identity and make every lookup miss.)
	byKey := map[string]ckafka.Offset{}
	for _, tp := range got {
		if tp.Topic == nil {
			t.Fatal("commitOffsets returned a nil Topic")
		}
		byKey[fmt.Sprintf("%s/%d", *tp.Topic, tp.Partition)] = tp.Offset
	}
	if len(byKey) != 3 {
		t.Fatalf("commitOffsets produced %d partitions, want 3 (deduped): %+v", len(byKey), got)
	}
	if o := byKey["t/0"]; o != ckafka.Offset(10) {
		t.Errorf("partition 0 offset = %v, want 10 (max 9 + 1)", o)
	}
	if o := byKey["t/1"]; o != ckafka.Offset(4) {
		t.Errorf("partition 1 offset = %v, want 4 (max 3 + 1)", o)
	}
	if o := byKey["t/2"]; o != ckafka.Offset(1) {
		t.Errorf("partition 2 offset = %v, want 1 (offset 0 must be stored as 0+1)", o)
	}
}

func TestConsumer_CommitOffsetsEmpty(t *testing.T) {
	if got := commitOffsets(nil); len(got) != 0 {
		t.Errorf("commitOffsets(nil) = %+v, want empty", got)
	}
}

// Start-while-running must fail WITHOUT replacing the channel the running
// loop will close: an orphaned loopExited stalls Close for its full 5s
// fallback and then destroys the consumer handle while the loop may still be
// inside ReadMessage (cgo) — the segfault the handshake exists to prevent.
func TestConsumer_StartWhileRunningKeepsCloseHandshake(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithRetry(&RetryConfig{MaxRetries: 1, InitialInterval: 10 * time.Millisecond}),
		ConsumerWithAutoCommit(true), ConsumerWithAutoCommitInterval(200*time.Millisecond),
		ConsumerWithFromBeginning(true),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- c.Start(ctx) }()

	waitFor(t, 10*time.Second, func() bool { return c.running.Load() },
		"first Start never began running")

	if err := c.Start(context.Background()); err == nil {
		t.Fatal("second Start() = nil while already running, want error")
	}

	closed := make(chan error, 1)
	closeStart := time.Now()
	go func() { closed <- c.Close(context.Background()) }()
	select {
	case <-closed:
		if elapsed := time.Since(closeStart); elapsed > 2*time.Second {
			t.Fatalf("Close took %v, want <2s: the loop-exit channel was orphaned", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close stalled >2s: Start-while-running orphaned the loop-exit channel")
	}
	select {
	case <-startErr:
	case <-time.After(2 * time.Second):
		t.Fatal("first Start never returned after Close")
	}
}

func TestConsumer_StartRequiresHandler(t *testing.T) {
	skipIfShort(t) // no broker needed, but keeps policy uniform
	c, err := NewConsumer(ConsumerWithBrokers("localhost:9092"),
		ConsumerWithGroupID("g"), ConsumerWithTopics("t"))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close(context.Background())
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start() = nil with no handler registered, want error")
	}
}

func TestConsumer_HandlerRegistrationLastWins(t *testing.T) {
	m := func(context.Context, *Message) error { return nil }
	b := func(context.Context, []*Message) error { return nil }
	g := func(context.Context, []GroupedBatch) error { return nil }
	c := &Consumer{}
	c.OnMessage(m)
	c.OnBatch(b)
	if c.messageHandler != nil || c.batchHandler == nil || !c.batchMode() {
		t.Fatal("OnBatch must replace OnMessage and enable batch mode")
	}
	c.OnGroupedBatch(g)
	if c.batchHandler != nil || c.groupedBatchHandler == nil || !c.batchMode() {
		t.Fatal("OnGroupedBatch must replace OnBatch")
	}
	c.OnMessage(m)
	if c.groupedBatchHandler != nil || c.batchMode() {
		t.Fatal("OnMessage must replace OnGroupedBatch and leave batch mode")
	}
}

func TestConsumer_NilHandlerAfterRegistration(t *testing.T) {
	c := &Consumer{}
	c.OnMessage(func(context.Context, *Message) error { return nil })
	c.OnBatch(nil)
	if err := c.Start(context.Background()); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("Start() = %v, want ErrNoHandler", err)
	}
}

func TestConsumer_ClosedConsumerRejectsStartAndCommit(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	c, err := NewConsumer(ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(uniqueTopic(t, mc, 1)))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return nil })
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Start(context.Background()); !errors.Is(err, ErrConsumerClosed) {
		t.Errorf("Start() after Close = %v, want ErrConsumerClosed", err)
	}
	if err := c.Commit(); !errors.Is(err, ErrConsumerClosed) {
		t.Errorf("Commit() after Close = %v, want ErrConsumerClosed", err)
	}
	if err := c.Commit(&Message{Topic: "t"}); !errors.Is(err, ErrConsumerClosed) {
		t.Errorf("Commit(msg) after Close = %v, want ErrConsumerClosed", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("second Close() = %v, want nil", err)
	}
}

// A handler that commits manually must still be able to commit while Close
// flushes the final batch: the handle is alive until Close tears it down.
func TestConsumer_CommitInsideHandlerDuringClose(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic, group := uniqueTopic(t, mc, 1), uniqueGroupName(t)
	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()), ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true), ConsumerWithAutoCommit(false),
		ConsumerWithBatchSize(100), ConsumerWithBatchTimeout(time.Hour), // only Close flushes
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	commitErrs := make(chan error, 1)
	c.OnBatch(func(_ context.Context, msgs []*Message) error {
		commitErrs <- c.Commit(msgs...)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	if err := p.Produce(ctx, topic, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	waitFor(t, 15*time.Second, func() bool {
		c.batchMu.Lock()
		defer c.batchMu.Unlock()
		return len(c.batch) == 1
	}, "message never reached the pending batch")

	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-commitErrs:
		if err != nil {
			t.Fatalf("Commit inside the handler during Close = %v, want nil", err)
		}
	default:
		t.Fatal("handler was not called by Close's final flush")
	}
	if got := committedOffset(t, mc.BootstrapServers(), group, topic, 0); got != 1 {
		t.Errorf("committed offset = %d, want 1", got)
	}
}

func TestConsumer_FilterDuplicates(t *testing.T) {
	msg := func(k string) *Message { return &Message{Key: []byte(k)} }
	byKey := func(m *Message) string { return string(m.Key) }
	tests := []struct {
		name     string
		seen     []string
		keyFn    IdempotencyKeyFunc
		in       []string
		wantKept []string
	}{
		{"duplicate inside batch keeps first", nil, byKey, []string{"a", "b", "a"}, []string{"a", "b"}},
		{"already processed key is dropped", []string{"a"}, byKey, []string{"a", "b"}, []string{"b"}},
		{"all duplicates leaves nothing", []string{"a", "b"}, byKey, []string{"a", "b"}, nil},
		{"empty key is never filtered", nil, func(*Message) string { return "" }, []string{"a", "a", "a"}, []string{"a", "a", "a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newIdempotencyStore(time.Hour)
			defer s.close()
			for _, k := range tt.seen {
				s.add(k)
			}
			c := &Consumer{config: &ConsumerConfig{IdempotencyKey: tt.keyFn}, idempotencyStore: s}
			var in []*Message
			for _, k := range tt.in {
				in = append(in, msg(k))
			}
			kept, keys := c.filterDuplicates(in)
			if len(kept) != len(tt.wantKept) || len(keys) != len(kept) {
				t.Fatalf("kept %d msgs / %d keys, want %d", len(kept), len(keys), len(tt.wantKept))
			}
			for i, m := range kept {
				if string(m.Key) != tt.wantKept[i] {
					t.Errorf("kept[%d] = %q, want %q", i, m.Key, tt.wantKept[i])
				}
			}
		})
	}
}

func TestConsumer_BatchIdempotencySkipsProcessedKeys(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic, group := uniqueTopic(t, mc, 1), uniqueGroupName(t)
	p, _ := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	defer p.Close()
	var mu sync.Mutex
	var got []string
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()), ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithBatchSize(2), ConsumerWithBatchTimeout(200*time.Millisecond),
		ConsumerWithIdempotencyKey(func(m *Message) string { return string(m.Key) }))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnBatch(func(_ context.Context, msgs []*Message) error {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			got = append(got, string(m.Value))
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer c.Close(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	produce := func(k, v string) {
		if err := p.Produce(ctx, topic, &Message{Key: []byte(k), Value: []byte(v)}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	produce("a", "1")
	produce("b", "2")
	waitFor(t, 15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 2 }, "first batch never handled")
	produce("a", "3") // both already processed: the whole batch is filtered out
	produce("b", "4")
	waitFor(t, 15*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 0) == 4
	}, "offsets did not advance past an all-duplicate batch")
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Errorf("handler saw %v, want only the first two values", got)
	}
}

func TestConsumer_HandleBatchErrorHonoursErrorHandler(t *testing.T) {
	batch := []*Message{{Topic: "t"}, {Topic: "t"}}
	discard := slog.New(slog.DiscardHandler)
	claimAll := &Consumer{logger: discard, metrics: newMetricsCollector(), config: &ConsumerConfig{
		ErrorHandler: func(context.Context, *Message, error) error { return nil }}}
	if claimAll.handleBatchError(context.Background(), errors.New("boom"), batch) {
		t.Error("blocked = true, want false: the error handler took ownership of every message")
	}
	none := &Consumer{logger: discard, metrics: newMetricsCollector(), config: &ConsumerConfig{}}
	if !none.handleBatchError(context.Background(), errors.New("boom"), batch) {
		t.Error("blocked = false, want true: no error handler and no DLQ")
	}
}

func TestConsumer_HandleBatchErrorCountsHandlerFailures(t *testing.T) {
	c := &Consumer{
		config: &ConsumerConfig{
			ErrorHandler: func(context.Context, *Message, error) error { return nil },
		},
		logger:  slog.New(slog.DiscardHandler),
		metrics: newMetricsCollector(),
	}
	batch := []*Message{{Topic: "t1"}, {Topic: "t1"}, {Topic: "t2"}}
	c.handleBatchError(context.Background(), errors.New("boom"), batch)

	m := c.Metrics()
	if m.HandlerFailures != 3 {
		t.Errorf("HandlerFailures = %d, want 3 (one per message of the failed batch)", m.HandlerFailures)
	}
	if m.ByTopic["t1"].HandlerFailures != 2 || m.ByTopic["t2"].HandlerFailures != 1 {
		t.Errorf("ByTopic = %+v, want t1:2 t2:1", m.ByTopic)
	}
}

// failingBatchConsumer starts a mock-broker consumer (batch size 2) whose
// OnBatch handler fails while *fail is set, with an error handler that records
// every value it receives and claims the message.
func failingBatchConsumer(t *testing.T, mc *ckafka.MockCluster, topic string, fail *atomic.Bool) (handled, errored func() []string, stop func()) {
	t.Helper()
	var mu sync.Mutex
	var okVals, errVals []string
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()), ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithBatchSize(2), ConsumerWithBatchTimeout(200*time.Millisecond),
		ConsumerWithIdempotencyKey(func(m *Message) string { return string(m.Key) }),
		ConsumerWithErrorHandler(func(_ context.Context, m *Message, _ error) error {
			mu.Lock()
			defer mu.Unlock()
			errVals = append(errVals, string(m.Value))
			return nil
		}))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnBatch(func(_ context.Context, msgs []*Message) error {
		if fail.Load() {
			return errors.New("batch fails")
		}
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			okVals = append(okVals, string(m.Value))
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()
	snap := func(src *[]string) func() []string {
		return func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), (*src)...) }
	}
	return snap(&okVals), snap(&errVals), func() { cancel(); _ = c.Close(context.Background()) }
}

func TestConsumer_FailedBatchSendsOnlyNonDuplicatesToErrorHandler(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, _ := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	defer p.Close()
	var fail atomic.Bool
	handled, errored, stop := failingBatchConsumer(t, mc, topic, &fail)
	defer stop()
	produce := func(k, v string) {
		if err := p.Produce(context.Background(), topic, &Message{Key: []byte(k), Value: []byte(v)}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	produce("a", "1")
	produce("b", "2")
	waitFor(t, 15*time.Second, func() bool { return len(handled()) == 2 }, "first batch never handled")
	fail.Store(true)
	produce("a", "3") // duplicate of a processed key: filtered before the handler
	produce("c", "4")
	waitFor(t, 15*time.Second, func() bool { return len(errored()) > 0 }, "error handler never called")
	if got := errored(); len(got) != 1 || got[0] != "4" {
		t.Errorf("error handler received %v, want only [4]", got)
	}
}

func TestConsumer_FailedBatchRecordsNoKeys(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	p, _ := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	defer p.Close()
	var fail atomic.Bool
	fail.Store(true)
	handled, errored, stop := failingBatchConsumer(t, mc, topic, &fail)
	defer stop()
	produce := func(k, v string) {
		if err := p.Produce(context.Background(), topic, &Message{Key: []byte(k), Value: []byte(v)}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	produce("c", "1")
	produce("d", "2")
	waitFor(t, 15*time.Second, func() bool { return len(errored()) == 2 }, "failed batch never reached the error handler")
	fail.Store(false)
	produce("c", "3") // same keys again: must not be filtered as duplicates
	produce("d", "4")
	waitFor(t, 15*time.Second, func() bool { return len(handled()) == 2 },
		"keys of the failed batch were recorded: the retry batch never reached the handler")
}

func TestConsumer_Validation(t *testing.T) {
	base := []ConsumerOption{ConsumerWithBrokers("127.0.0.1:1"), ConsumerWithGroupID("g"), ConsumerWithTopics("t")}
	tests := []struct {
		name string
		opt  ConsumerOption
		want error
	}{
		{"empty DLQ topic", ConsumerWithDLQ(&DLQConfig{}), ErrDLQTopicRequired},
		{"zero batch size", ConsumerWithBatchSize(0), ErrInvalidBatchSize},
		{"negative batch size", ConsumerWithBatchSize(-1), ErrInvalidBatchSize},
		{"zero batch timeout", ConsumerWithBatchTimeout(0), ErrInvalidBatchTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(append(base, tt.opt)...)
			if !errors.Is(err, tt.want) {
				if c != nil {
					_ = c.Close(context.Background())
				}
				t.Fatalf("NewConsumer() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func BenchmarkCommitOffsets_Single(b *testing.B) {
	msgs := []*Message{{Topic: "t", Partition: 0, Offset: 42}}
	b.ReportAllocs()
	for b.Loop() {
		_ = commitOffsets(msgs)
	}
}

// Message keys, values and headers must never reach the logs, and an
// idempotency key is derived from them.
func TestConsumer_ProcessMessageDuplicateLogOmitsIdempotencyKey(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := newIdempotencyStore(time.Hour)
	defer s.close()
	s.add("user-4711")
	c := &Consumer{
		logger:           logger,
		config:           &ConsumerConfig{IdempotencyKey: func(m *Message) string { return string(m.Key) }},
		idempotencyStore: s,
	}
	out := c.processMessage(context.Background(), &Message{Key: []byte("user-4711"), Topic: "orders", Partition: 2, Offset: 9})
	if out != outcomeParked {
		t.Fatalf("outcome = %v, want parked", out)
	}
	got := buf.String()
	if strings.Contains(got, "user-4711") {
		t.Errorf("log leaks the idempotency key: %q", got)
	}
	for _, want := range []string{"topic=orders", "partition=2", "offset=9"} {
		if !strings.Contains(got, want) {
			t.Errorf("log %q missing %s", got, want)
		}
	}
}

func TestConsumer_IsFatalClientError(t *testing.T) {
	fatal := ckafka.NewError(ckafka.ErrFencedInstanceID, "fenced", true)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"fatal", fatal, true},
		{"non-fatal", ckafka.NewError(ckafka.ErrTransport, "x", false), false},
		{"wrapped fatal", fmt.Errorf("wrap: %w", fatal), true},
		{"plain", errors.New("plain"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isFatalClientError(tt.err); got != tt.want {
				t.Errorf("isFatalClientError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestConsumer_IgnoreNoOffset(t *testing.T) {
	noOffset := ckafka.NewError(ckafka.ErrNoOffset, "no offset", false)
	transport := ckafka.NewError(ckafka.ErrTransport, "x", false)
	plain := errors.New("plain")
	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{"nil", nil, true},
		{"no offset", noOffset, true},
		{"wrapped no offset", fmt.Errorf("wrap: %w", noOffset), true},
		{"transport", transport, false},
		{"plain", plain, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ignoreNoOffset(tt.err)
			if tt.wantNil && got != nil {
				t.Errorf("ignoreNoOffset(%v) = %v, want nil", tt.err, got)
			}
			if !tt.wantNil && !errors.Is(got, tt.err) {
				t.Errorf("ignoreNoOffset(%v) = %v, want the error unchanged", tt.err, got)
			}
		})
	}
}

func TestConsumer_RequiresBrokersGroupAndTopics(t *testing.T) {
	tests := []struct {
		name string
		opts []ConsumerOption
		want error
	}{
		{"no options", nil, ErrBrokersRequired},
		{"empty brokers", []ConsumerOption{ConsumerWithBrokers(), ConsumerWithGroupID("g"), ConsumerWithTopics("t")}, ErrBrokersRequired},
		{"missing group id", []ConsumerOption{ConsumerWithBrokers("127.0.0.1:1"), ConsumerWithTopics("t")}, ErrGroupIDRequired},
		{"missing topics", []ConsumerOption{ConsumerWithBrokers("127.0.0.1:1"), ConsumerWithGroupID("g")}, ErrTopicsRequired},
		{"empty topics", []ConsumerOption{ConsumerWithBrokers("127.0.0.1:1"), ConsumerWithGroupID("g"), ConsumerWithTopics()}, ErrTopicsRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(tt.opts...)
			if !errors.Is(err, tt.want) {
				if c != nil {
					_ = c.Close(context.Background())
				}
				t.Fatalf("NewConsumer() error = %v, want %v", err, tt.want)
			}
		})
	}
}

// Pause stops the broker fetch (applied by the poll loop once a partition is
// assigned) and Resume brings delivery back.
func TestConsumer_PauseResume(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	got := make(chan *Message, 4)
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(_ context.Context, m *Message) error { got <- m; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	c.Pause()
	// The poll loop applies the pause only once the assignment exists; waiting
	// on pauseApplied (instead of a fixed sleep) means the produce below can
	// never race ahead of the pause.
	waitFor(t, 15*time.Second, func() bool { return c.pauseApplied.Load() },
		"poll loop never applied the pause")

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := p.Produce(context.Background(), topic, &Message{Value: []byte("held")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	select {
	case m := <-got:
		t.Fatalf("handler received %q while paused", m.Value)
	case <-time.After(time.Second):
	}

	c.Resume()
	msgs := recvMessages(t, got, 1, 15*time.Second)
	if string(msgs[0].Value) != "held" {
		t.Errorf("after Resume got %q, want held", msgs[0].Value)
	}
}

// shutdownMidHandler runs one consumer with a DLQ topic and an error handler
// that records its calls and declines every message, registers the handler set
// by register, produces n messages and, once the handler has started, cancels
// the Start context and closes the consumer. It returns the error handler's
// call count and the DLQ topic.
func shutdownMidHandler(t *testing.T, mc *ckafka.MockCluster, topic, group string, n int,
	opts []ConsumerOption, register func(c *Consumer, started chan<- struct{})) (errHandlerCalls int32, dlqTopic string) {
	t.Helper()
	dlqTopic = uniqueTopic(t, mc, 1)
	var calls atomic.Int32
	c, err := NewConsumer(append(fastAtLeastOnce(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic}),
		// Short session: the mock coordinator completes the next member's join
		// only after this one's session expires.
		ConsumerWithSessionTimeout(6*time.Second), ConsumerWithHeartbeatInterval(2*time.Second),
		ConsumerWithErrorHandler(func(_ context.Context, _ *Message, err error) error {
			calls.Add(1)
			return err // declines the message, so a wrongly failed one reaches the DLQ
		}),
	), opts...)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	started := make(chan struct{}, 1)
	register(c, started)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	for i := range n {
		if err := p.Produce(context.Background(), topic, &Message{Value: []byte{byte('0' + i)}}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	if err := c.Close(context.Background()); err != nil { // waits for the poll loop to exit
		t.Fatalf("Close: %v", err)
	}
	return calls.Load(), dlqTopic
}

// assertRedelivered starts a second consumer in group with a succeeding
// handler and asserts it receives n messages: the shutdown left them unstored.
func assertRedelivered(t *testing.T, mc *ckafka.MockCluster, topic, group string, n int) {
	t.Helper()
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithSessionTimeout(6*time.Second), ConsumerWithHeartbeatInterval(2*time.Second))...)
	if err != nil {
		t.Fatalf("NewConsumer (second): %v", err)
	}
	got := make(chan *Message, n+4)
	c.OnMessage(func(_ context.Context, m *Message) error { got <- m; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer c.Close(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	for i, m := range recvMessages(t, got, n, 20*time.Second) {
		if m.Offset != int64(i) {
			t.Errorf("redelivered message %d has offset %d, want %d", i, m.Offset, i)
		}
	}
}

// A context-honouring handler interrupted by shutdown is not a handler
// failure: the error handler and the DLQ are not invoked, nothing is stored,
// and the message is redelivered to the group's next consumer.
func TestConsumer_ShutdownMidHandlerDoesNotParkOrDeadLetter(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic, group := uniqueTopic(t, mc, 1), uniqueGroupName(t)

	calls, dlqTopic := shutdownMidHandler(t, mc, topic, group, 1, nil, func(c *Consumer, started chan<- struct{}) {
		c.OnMessage(func(ctx context.Context, _ *Message) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		})
	})

	if calls != 0 {
		t.Errorf("error handler called %d times, want 0: shutdown is not a handler failure", calls)
	}
	if n := topicHighWatermark(t, mc.BootstrapServers(), dlqTopic); n != 0 {
		t.Errorf("DLQ high watermark = %d, want 0", n)
	}
	if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 0 {
		t.Errorf("committed offset = %d, want nothing committed", off)
	}
	assertRedelivered(t, mc, topic, group, 1)
}

// Batch mode: a batch interrupted by shutdown is neither parked nor
// dead-lettered; the whole batch is redelivered.
func TestConsumer_ShutdownMidBatchDoesNotParkOrDeadLetter(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic, group := uniqueTopic(t, mc, 1), uniqueGroupName(t)

	opts := []ConsumerOption{ConsumerWithBatchSize(3), ConsumerWithBatchTimeout(time.Hour)}
	calls, dlqTopic := shutdownMidHandler(t, mc, topic, group, 3, opts, func(c *Consumer, started chan<- struct{}) {
		c.OnBatch(func(ctx context.Context, _ []*Message) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		})
	})

	if calls != 0 {
		t.Errorf("error handler called %d times, want 0: shutdown is not a handler failure", calls)
	}
	if n := topicHighWatermark(t, mc.BootstrapServers(), dlqTopic); n != 0 {
		t.Errorf("DLQ high watermark = %d, want 0", n)
	}
	if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 0 {
		t.Errorf("committed offset = %d, want nothing committed", off)
	}
	assertRedelivered(t, mc, topic, group, 3)
}

// Start may be called again on the same Consumer after its context ended. A
// message or batch that shutdown interrupted must come back on that second run
// even when the assignor keeps the partition (cooperative-sticky): the fetch
// position survives the restart, so only a seek back redelivers it.
func TestConsumer_RestartAfterInterruptRedelivers(t *testing.T) {
	skipIfShort(t)
	tests := []struct {
		name     string
		opts     []ConsumerOption
		register func(c *Consumer, h BatchHandler)
	}{
		{"single", nil, func(c *Consumer, h BatchHandler) {
			c.OnMessage(func(ctx context.Context, m *Message) error { return h(ctx, []*Message{m}) })
		}},
		{"batch", []ConsumerOption{ConsumerWithBatchSize(2), ConsumerWithBatchTimeout(time.Hour)},
			func(c *Consumer, h BatchHandler) { c.OnBatch(h) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := newMockCluster(t)
			topic := uniqueTopic(t, mc, 1)
			c, err := NewConsumer(append(fastAtLeastOnce(
				ConsumerWithBrokers(mc.BootstrapServers()),
				ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
				ConsumerWithPartitionAssignor(AssignorCooperativeSticky),
			), tt.opts...)...)
			if err != nil {
				t.Fatalf("NewConsumer: %v", err)
			}
			defer c.Close(context.Background())

			var interrupt atomic.Bool
			interrupt.Store(true)
			started := make(chan struct{}, 1)
			got := make(chan *Message, 8)
			tt.register(c, func(ctx context.Context, msgs []*Message) error {
				if interrupt.Load() {
					select {
					case started <- struct{}{}:
					default:
					}
					<-ctx.Done()
					return ctx.Err()
				}
				for _, m := range msgs {
					got <- m
				}
				return nil
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exited := make(chan error, 1)
			go func() { exited <- c.Start(ctx) }()

			p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
			if err != nil {
				t.Fatalf("NewProducer: %v", err)
			}
			defer p.Close()
			for i := range 2 {
				if err := p.Produce(context.Background(), topic, &Message{Value: []byte{byte('0' + i)}}); err != nil {
					t.Fatalf("Produce: %v", err)
				}
			}
			select {
			case <-started:
			case <-time.After(15 * time.Second):
				t.Fatal("handler never started")
			}
			interrupt.Store(false)
			cancel()
			select {
			case err := <-exited:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("first Start() = %v, want context.Canceled", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("first Start did not return")
			}

			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			go func() { _ = c.Start(ctx2) }()
			for i, m := range recvMessages(t, got, 2, 20*time.Second) {
				if m.Offset != int64(i) {
					t.Errorf("message %d of the second run has offset %d, want %d", i, m.Offset, i)
				}
			}
		})
	}
}

// Close interrupting a retry wait, with the Start context still live, is
// shutdown too: the message is reported interrupted and is not offered to the
// error handler.
func TestConsumer_ProcessMessageInterruptedByCloseSkipsErrorHandler(t *testing.T) {
	calls := 0
	c := &Consumer{
		config: &ConsumerConfig{
			ErrorHandler: func(context.Context, *Message, error) error { calls++; return nil },
		},
		retry:   normalizeRetry(&RetryConfig{MaxRetries: 5, InitialInterval: time.Hour}),
		done:    make(chan struct{}),
		logger:  slog.New(slog.DiscardHandler),
		metrics: newMetricsCollector(),
	}
	c.after = func(time.Duration) <-chan time.Time {
		close(c.done) // Close is called while the retry wait is in progress
		return make(chan time.Time)
	}
	c.messageHandler = func(context.Context, *Message) error { return errors.New("boom") }

	if got := c.processMessage(context.Background(), &Message{Topic: "t"}); got != outcomeInterrupted {
		t.Errorf("processMessage() = %v, want outcomeInterrupted", got)
	}
	if calls != 0 {
		t.Errorf("error handler called %d times, want 0", calls)
	}
}

// The final batch flush in Close runs with Close's ctx. When that ctx has ended
// and the batch handler then fails, the batch is interrupted, not failed: the
// error handler is not called, nothing is stored and the batch is redelivered.
func TestConsumer_CloseFlushWithEndedContextLeavesBatchUnstored(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic, group := uniqueTopic(t, mc, 1), uniqueGroupName(t)
	var errCalls atomic.Int32
	c, err := NewConsumer(fastAtLeastOnce(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithSessionTimeout(6*time.Second), ConsumerWithHeartbeatInterval(2*time.Second),
		ConsumerWithBatchSize(10), ConsumerWithBatchTimeout(time.Hour),
		ConsumerWithErrorHandler(func(context.Context, *Message, error) error {
			errCalls.Add(1)
			return nil // would claim the message: park it and move on
		}),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnBatch(func(ctx context.Context, _ []*Message) error { return ctx.Err() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	for i := range 2 {
		if err := p.Produce(context.Background(), topic, &Message{Value: []byte{byte('0' + i)}}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}
	waitFor(t, 15*time.Second, func() bool {
		c.batchMu.Lock()
		defer c.batchMu.Unlock()
		return len(c.batch) == 2
	}, "the two messages never reached the pending batch")

	cancel()
	ended, end := context.WithCancel(context.Background())
	end()
	if err := c.Close(ended); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if n := errCalls.Load(); n != 0 {
		t.Errorf("error handler called %d times, want 0: shutdown is not a handler failure", n)
	}
	if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 0 {
		t.Errorf("committed offset = %d, want nothing committed", off)
	}
	assertRedelivered(t, mc, topic, group, 2)
}

// When the consume loop outlives Close's wait (a handler still running, the
// Start context never cancelled), Close must not destroy the client under it:
// it reports ErrCloseTimeout and the loop releases everything when Start
// returns.
func TestConsumer_CloseTimeoutHandsTeardownToLoop(t *testing.T) {
	skipIfShort(t)
	old := closeLoopWait
	closeLoopWait = 200 * time.Millisecond
	t.Cleanup(func() { closeLoopWait = old })

	mc := newMockCluster(t)
	topic, group := uniqueTopic(t, mc, 1), uniqueGroupName(t)
	c, err := NewConsumer(fastAtLeastOnce(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithLogger(slog.New(slog.DiscardHandler)),
	)...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	c.OnMessage(func(context.Context, *Message) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	exited := make(chan error, 1)
	go func() { exited <- c.Start(context.Background()) }()

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := p.Produce(context.Background(), topic, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	if err := c.Close(context.Background()); !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() = %v, want ErrCloseTimeout", err)
	}
	if c.handleClosed.Load() {
		t.Fatal("the client was closed while the consume loop was still running")
	}
	if err := c.Start(context.Background()); !errors.Is(err, ErrConsumerClosed) {
		t.Errorf("Start() after a timed-out Close = %v, want ErrConsumerClosed", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Errorf("second Close() = %v, want nil", err)
	}

	close(release)
	select {
	case err := <-exited:
		if err != nil {
			t.Errorf("Start() = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the handler was released")
	}
	if !c.handleClosed.Load() {
		t.Error("the consume loop did not release the client when Start returned")
	}
	if err := c.Commit(); !errors.Is(err, ErrConsumerClosed) {
		t.Errorf("Commit() = %v, want ErrConsumerClosed", err)
	}
	// The offset was stored on a live client and committed by the loop's teardown.
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 0) == 1
	}, "the message's offset was never committed")
}

func TestConsumer_DropPendingForRemovesRevokedPartitions(t *testing.T) {
	t0, other := "t", "u"
	c := &Consumer{batch: []*Message{
		{Topic: "t", Partition: 0, Offset: 5}, {Topic: "t", Partition: 1, Offset: 7},
		{Topic: "u", Partition: 0, Offset: 1}, {Topic: "t", Partition: 0, Offset: 6},
	}}
	n := c.dropPendingFor([]ckafka.TopicPartition{
		{Topic: &t0, Partition: 0}, {Partition: 1}, {Topic: &other, Partition: 9},
	})
	if n != 2 {
		t.Errorf("dropPendingFor() = %d, want 2", n)
	}
	if len(c.batch) != 2 || c.batch[0].Partition != 1 || c.batch[1].Topic != "u" {
		t.Errorf("pending batch = %+v, want t/1 then u/0", c.batch)
	}
	if n := c.dropPendingFor([]ckafka.TopicPartition{{Topic: &other, Partition: 9}, {Partition: 0}}); n != 0 || len(c.batch) != 2 {
		t.Errorf("a revoke naming no pending partition changed the batch: removed %d, len %d", n, len(c.batch))
	}
}

// A revoked partition's messages must leave the pending batch: the partition
// belongs to another member now, which gets them redelivered.
func TestConsumer_RevokeDropsPendingBatchMessages(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 2)
	c, err := NewConsumer(ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)), ConsumerWithTopics(topic),
		ConsumerWithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer c.Close(context.Background())
	c.OnBatch(func(context.Context, []*Message) error { return nil })
	c.batch = []*Message{
		{Topic: topic, Partition: 0, Offset: 0}, {Topic: topic, Partition: 1, Offset: 0},
		{Topic: topic, Partition: 0, Offset: 1},
	}

	_ = c.rebalanceCallback()(c.consumer, ckafka.RevokedPartitions{
		Partitions: []ckafka.TopicPartition{{Topic: &topic, Partition: 0}},
	})

	c.batchMu.Lock()
	defer c.batchMu.Unlock()
	if len(c.batch) != 1 || c.batch[0].Partition != 1 {
		t.Errorf("pending batch after the revoke = %+v, want only the partition 1 message", c.batch)
	}
}
