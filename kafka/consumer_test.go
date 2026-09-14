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

	// Produce with explicit partitions (the library producer always uses
	// PartitionAny): 2 messages per partition.
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
	}, metrics: NewDLQMetricsCollector()}

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
	if m := c.metrics.GetMetrics(); len(m.BlockedPartitions) != 1 || m.BlockedPartitions[0].Partition != 1 {
		t.Errorf("metrics blocked partitions = %+v, want [t1/1]", m.BlockedPartitions)
	}
	c.dropBlockedFor([]ckafka.TopicPartition{{Partition: 7}}) // nil Topic must be skipped, not panic
	if len(c.blockedSnapshot()) != 1 {
		t.Error("nil-topic partition must be ignored by dropBlockedFor")
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

func TestInvokeHandlerDispatch(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*Consumer)
		wantBy string // which handler must fire
	}{
		{"message handler wins", func(c *Consumer) {
			c.messageHandler = func(context.Context, *Message) error { return errors.New("m") }
			c.batchHandler = func(context.Context, []*Message) error { return errors.New("b") }
		}, "m"},
		{"batch handler wraps single message", func(c *Consumer) {
			c.batchHandler = func(_ context.Context, msgs []*Message) error {
				if len(msgs) != 1 {
					t.Errorf("batch handler got %d msgs, want 1", len(msgs))
				}
				return errors.New("b")
			}
		}, "b"},
		{"grouped handler wraps one group", func(c *Consumer) {
			c.groupedBatchHandler = func(_ context.Context, groups []GroupedBatch) error {
				if len(groups) != 1 || len(groups[0].Messages) != 1 {
					t.Errorf("grouped handler got %+v, want 1 group/1 msg", groups)
				}
				return errors.New("g")
			}
		}, "g"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Consumer{}
			tt.setup(c)
			if err := c.invokeHandler(context.Background(), &Message{Key: []byte("k")}); err == nil || err.Error() != tt.wantBy {
				t.Errorf("invokeHandler() = %v, want %q", err, tt.wantBy)
			}
		})
	}
}
