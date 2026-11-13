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
	claimAll := &Consumer{logger: discard, config: &ConsumerConfig{
		ErrorHandler: func(context.Context, *Message, error) error { return nil }}}
	if claimAll.handleBatchError(context.Background(), errors.New("boom"), batch) {
		t.Error("blocked = true, want false: the error handler took ownership of every message")
	}
	none := &Consumer{logger: discard, config: &ConsumerConfig{}}
	if !none.handleBatchError(context.Background(), errors.New("boom"), batch) {
		t.Error("blocked = false, want true: no error handler and no DLQ")
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
