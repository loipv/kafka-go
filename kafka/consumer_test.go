package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// committedOffset reads the group's committed offset for one partition via the
// admin API. Returns -1 (or librdkafka's OffsetInvalid) when nothing is
// committed yet.
func committedOffset(t *testing.T, brokers, group, topic string, partition int32) int64 {
	t.Helper()
	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := admin.ListConsumerGroupOffsets(ctx, []ckafka.ConsumerGroupTopicPartitions{{Group: group,
		Partitions: []ckafka.TopicPartition{{Topic: &topic, Partition: partition}}}})
	if err != nil || len(res.ConsumerGroupsTopicPartitions) == 0 {
		return -1
	}
	for _, g := range res.ConsumerGroupsTopicPartitions {
		for _, tp := range g.Partitions {
			if tp.Partition == partition {
				return int64(tp.Offset)
			}
		}
	}
	return -1
}

// fastAtLeastOnce is the common option set for the T2 at-least-once tests:
// quick retry cycles and a short auto-commit interval so committed offsets
// move within waitFor deadlines.
func fastAtLeastOnce(extra ...ConsumerOption) []ConsumerOption {
	opts := []ConsumerOption{
		ConsumerWithRetry(&RetryConfig{MaxRetries: 1, InitialInterval: 10 * time.Millisecond}),
		ConsumerWithAutoCommit(true),
		ConsumerWithAutoCommitInterval(200 * time.Millisecond),
		ConsumerWithFromBeginning(true),
	}
	return append(opts, extra...)
}

func TestAtLeastOnce_NoDLQ_BlocksPartition(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return errors.New("always fails") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := p.Produce(context.Background(), topic, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// With no DLQ and no error handler the message is unparkable: the
	// partition must block and the committed offset must never advance.
	waitFor(t, 10*time.Second, func() bool {
		return len(c.DLQMetrics().BlockedPartitions) > 0
	}, "partition never reported blocked for unparkable message")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 1 {
			t.Fatalf("committed offset = %d while message is unparkable, want no commit", off)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestAtLeastOnce_SkipOnMaxRetries_Advances(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	handlerCalls := int32(0)
	errs := make(chan error, 4)
	c, err := NewConsumer(append(fastAtLeastOnce(
		ConsumerWithRetry(&RetryConfig{MaxRetries: 1, InitialInterval: 10 * time.Millisecond, SkipOnMaxRetries: true}),
		ConsumerWithErrorHandler(func(_ context.Context, _ *Message, err error) error {
			atomic.AddInt32(&handlerCalls, 1)
			errs <- err
			return nil // claim ownership: park and advance
		}),
	),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(context.Context, *Message) error { return errors.New("always fails") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := p.Produce(context.Background(), topic, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// SkipOnMaxRetries is the explicit opt-in to loss: the error handler is
	// notified once and the committed offset advances past the message.
	waitFor(t, 10*time.Second, func() bool {
		return len(errs) > 0
	}, "error handler never notified of skipped message")
	if err := <-errs; !errors.Is(err, ErrSkippedOnMaxRetries) {
		t.Errorf("error handler got %v, want ErrSkippedOnMaxRetries", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 0) >= 1
	}, "committed offset never advanced past skipped message")
	if n := atomic.LoadInt32(&handlerCalls); n != 1 {
		t.Errorf("error handler called %d times, want exactly 1", n)
	}
}

func TestPartitionIsolation(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 2) // 2 partitions
	group := uniqueGroupName(t)

	var p1Count int32
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(_ context.Context, msg *Message) error {
		if msg.Partition == 0 {
			return errors.New("partition 0 always fails")
		}
		atomic.AddInt32(&p1Count, 1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	// Produce with explicit partitions (the library producer always lets the
	// partitioner choose): 2 messages per partition.
	raw, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": mc.BootstrapServers()})
	if err != nil {
		t.Fatalf("raw producer: %v", err)
	}
	for part := int32(0); part < 2; part++ {
		for i := 0; i < 2; i++ {
			_ = raw.Produce(&ckafka.Message{
				TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: part},
				Value:          []byte("v"),
			}, nil)
		}
	}
	raw.Flush(5000)
	raw.Close()

	// Partition 1 keeps flowing while partition 0 is blocked.
	waitFor(t, 10*time.Second, func() bool { return atomic.LoadInt32(&p1Count) == 2 },
		"partition 1 messages never processed while partition 0 was failing")
	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 1) >= 2
	}, "partition 1 committed offset never advanced")
	waitFor(t, 10*time.Second, func() bool {
		for _, tp := range c.DLQMetrics().BlockedPartitions {
			if tp.Topic == topic && tp.Partition == 0 {
				return true
			}
		}
		return false
	}, "partition 0 never reported blocked")
	if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 1 {
		t.Errorf("partition 0 committed offset = %d, want no commit", off)
	}
}

func TestBlockedPartition_RecoversUnattended(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	// Fail the ENTIRE first deliver cycle (MaxRetries=1 → 2 handler
	// invocations), succeed from invocation 3 on. A third invocation is only
	// possible via block → seek → resume, which pins the recovery path.
	var mu sync.Mutex
	invocations := map[int64]int{}
	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic))...)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.OnMessage(func(_ context.Context, msg *Message) error {
		mu.Lock()
		invocations[msg.Offset]++
		n := invocations[msg.Offset]
		mu.Unlock()
		if n <= 2 {
			return errors.New("first cycle fails")
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	defer c.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := p.Produce(context.Background(), topic, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 0) >= 1
	}, "blocked partition never recovered unattended (offset not committed)")
	waitFor(t, 10*time.Second, func() bool {
		return len(c.DLQMetrics().BlockedPartitions) == 0
	}, "BlockedPartitions never drained after recovery")
	mu.Lock()
	got := invocations[0]
	mu.Unlock()
	if got != 3 {
		t.Errorf("message delivered %d handler invocations, want 3 (2 failing cycle + 1 successful redelivery)", got)
	}
}

func TestBlockedSnapshotAndDropBlockedFor(t *testing.T) {
	c := &Consumer{blocked: map[TopicPartition]blockState{
		{"t1", 0, 0}: {retryAt: time.Now(), blocks: 1},
		{"t1", 1, 0}: {retryAt: time.Now(), blocks: 2},
	}, metrics: newMetricsCollector()}

	snap := c.blockedSnapshot()
	if len(snap) != 2 {
		t.Fatalf("blockedSnapshot() = %+v, want 2 partitions", snap)
	}
	topic := "t1"
	c.dropBlockedFor([]ckafka.TopicPartition{{Topic: &topic, Partition: 0}})
	snap = c.blockedSnapshot()
	if len(snap) != 1 || snap[0].Topic != "t1" || snap[0].Partition != 1 {
		t.Fatalf("after dropBlockedFor(p0): %+v, want only t1/1", snap)
	}
	if m := c.metrics.getMetrics(); len(m.BlockedPartitions) != 1 || m.BlockedPartitions[0].Partition != 1 {
		t.Errorf("metrics blocked partitions = %+v, want [t1/1]", m.BlockedPartitions)
	}
	c.dropBlockedFor([]ckafka.TopicPartition{{Partition: 7}}) // nil Topic must be skipped, not panic
	if len(c.blockedSnapshot()) != 1 {
		t.Error("nil-topic partition must be ignored by dropBlockedFor")
	}
}

// A parked message at or past the blocked offset clears the escalation state;
// a park below it (an earlier offset) must not.
func TestClearBlockedOnPark(t *testing.T) {
	c := &Consumer{blocked: map[TopicPartition]blockState{
		{Topic: "t", Partition: 0}: {retryAt: time.Now(), blocks: 3, offset: 5},
	}, metrics: newMetricsCollector()}

	c.clearBlockedOnPark(&Message{Topic: "t", Partition: 0, Offset: 4})
	if len(c.blockedSnapshot()) != 1 {
		t.Fatal("park below the blocked offset must not clear state")
	}
	c.clearBlockedOnPark(&Message{Topic: "t", Partition: 0, Offset: 5})
	if len(c.blockedSnapshot()) != 0 {
		t.Error("park at the blocked offset must clear state")
	}
	if got := c.metrics.getMetrics().BlockedPartitions; len(got) != 0 {
		t.Errorf("blocked gauge = %+v, want drained", got)
	}
}

// Start-while-running must fail WITHOUT replacing the channel the running
// loop will close: an orphaned loopExited stalls Close for its full 5s
// fallback and then destroys the consumer handle while the loop may still be
// inside ReadMessage (cgo) — the segfault the handshake exists to prevent.
func TestStartWhileRunningKeepsCloseHandshake(t *testing.T) {
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

	waitFor(t, 10*time.Second, func() bool { return atomic.LoadInt32(&c.running) == 1 },
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

func TestStartRequiresHandler(t *testing.T) {
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

func TestNewConsumer_Validation(t *testing.T) {
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
