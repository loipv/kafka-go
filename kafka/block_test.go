package kafka

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// TestBlockedPartition_NoOffsetDrift covers the Seek/pause buffer caveat:
// while partition 0 sits blocked, messages queued behind it must never let
// the committed offset drift past the blocked message — and the sibling
// partition keeps flowing.
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

	// The library producer always lets the partitioner choose; explicit partitions need
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
		for _, tp := range c.Metrics().BlockedPartitions {
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

func TestBlockedPartition_Backoff(t *testing.T) {
	r := &RetryConfig{InitialInterval: time.Second, Multiplier: 2, MaxInterval: 30 * time.Second}
	tests := []struct {
		blocks int
		want   time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{6, 30 * time.Second}, // 32s would exceed the cap
		{20, 30 * time.Second},
	}
	for _, tt := range tests {
		if got := blockBackoff(tt.blocks, normalizeRetry(r)); got != tt.want {
			t.Errorf("blockBackoff(%d) = %v, want %v", tt.blocks, got, tt.want)
		}
	}
	// zero-value RetryConfig must not panic and must fall back to defaults
	if got := blockBackoff(1, normalizeRetry(nil)); got <= 0 {
		t.Errorf("blockBackoff(1, normalizeRetry(nil)) = %v, want positive default", got)
	}
}

func TestBlockedPartition_NoDLQBlocks(t *testing.T) {
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
		return len(c.Metrics().BlockedPartitions) > 0
	}, "partition never reported blocked for unparkable message")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 1 {
			t.Fatalf("committed offset = %d while message is unparkable, want no commit", off)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestBlockedPartition_Isolation(t *testing.T) {
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
		for _, tp := range c.Metrics().BlockedPartitions {
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
		return len(c.Metrics().BlockedPartitions) == 0
	}, "BlockedPartitions never drained after recovery")
	mu.Lock()
	got := invocations[0]
	mu.Unlock()
	if got != 3 {
		t.Errorf("message delivered %d handler invocations, want 3 (2 failing cycle + 1 successful redelivery)", got)
	}
}

func TestBlockedPartition_SnapshotAndDropBlockedFor(t *testing.T) {
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
	if m := c.metrics.snapshot(); len(m.BlockedPartitions) != 1 || m.BlockedPartitions[0].Partition != 1 {
		t.Errorf("metrics blocked partitions = %+v, want [t1/1]", m.BlockedPartitions)
	}
	c.dropBlockedFor([]ckafka.TopicPartition{{Partition: 7}}) // nil Topic must be skipped, not panic
	if len(c.blockedSnapshot()) != 1 {
		t.Error("nil-topic partition must be ignored by dropBlockedFor")
	}
}

// A parked message at or past the blocked offset clears the escalation state;
// a park below it (an earlier offset) must not.
func TestBlockedPartition_ClearedOnPark(t *testing.T) {
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
	if got := c.metrics.snapshot().BlockedPartitions; len(got) != 0 {
		t.Errorf("blocked gauge = %+v, want drained", got)
	}
}

// Batch mode: a failing OnBatch handler with no error handler and no DLQ leaves
// the batch unparkable, so the partition blocks and no offset is committed.
func TestBlockedPartition_NoDLQBatchBlocks(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithBatchSize(2), ConsumerWithBatchTimeout(200*time.Millisecond))...)
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
	for i := range 2 {
		if err := p.Produce(context.Background(), topic, &Message{Value: []byte{byte('0' + i)}}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	waitFor(t, 15*time.Second, func() bool {
		return len(c.Metrics().BlockedPartitions) > 0
	}, "partition never reported blocked for the unparkable batch")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 1 {
			t.Fatalf("committed offset = %d while the batch is unparkable, want no commit", off)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestBlockedPartition_ResumablePartitionsSkipsWaitingBlocks(t *testing.T) {
	topic := "t"
	tp := func(p int32) ckafka.TopicPartition { return ckafka.TopicPartition{Topic: &topic, Partition: p} }
	c := &Consumer{blocked: map[TopicPartition]blockState{
		{Topic: "t", Partition: 0}: {blocks: 1, resumed: false}, // still waiting out its backoff
		{Topic: "t", Partition: 1}: {blocks: 1, resumed: true},  // backoff elapsed, already resumed
	}}
	got := c.resumablePartitions([]ckafka.TopicPartition{tp(0), tp(1), tp(2), {Partition: 9}})

	var parts []int32
	for _, p := range got {
		parts = append(parts, p.Partition)
	}
	if !slices.Equal(parts, []int32{1, 2, 9}) {
		t.Fatalf("resumable partitions = %v, want [1 2 9] (partition 0 is blocked and waiting)", parts)
	}
}

// TestBlockedPartition_UserResumeDoesNotBypassBackoff pins that Pause followed
// by Resume leaves a blocked partition paused until its own backoff elapses.
func TestBlockedPartition_UserResumeDoesNotBypassBackoff(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)

	c, err := NewConsumer(
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(group), ConsumerWithTopics(topic),
		ConsumerWithFromBeginning(true),
		ConsumerWithAutoCommit(true),
		ConsumerWithAutoCommitInterval(200*time.Millisecond),
		// Handler tried twice 4s apart, then the partition blocks for 4s.
		ConsumerWithRetry(&RetryConfig{MaxRetries: 1, InitialInterval: 4 * time.Second}),
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	var calls atomic.Int32
	c.OnMessage(func(context.Context, *Message) error {
		calls.Add(1)
		return errors.New("always fails")
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

	waitFor(t, 20*time.Second, func() bool {
		return len(c.Metrics().BlockedPartitions) > 0
	}, "partition never reported blocked")
	n := calls.Load()

	c.Pause()
	waitFor(t, 5*time.Second, c.pauseApplied.Load, "Pause never applied")
	c.Resume()
	waitFor(t, 5*time.Second, func() bool { return !c.pauseApplied.Load() }, "Resume never applied")

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := calls.Load(); got != n {
			t.Fatalf("handler called %d times, want %d: user Resume bypassed the blocked partition's backoff", got, n)
		}
		time.Sleep(100 * time.Millisecond)
	}

	waitFor(t, 15*time.Second, func() bool { return calls.Load() > n },
		"message never retried after the block's own backoff elapsed")
}

// TestBlockedPartition_RevokedWhileBlockedIsRedeliveredToNewOwner pins that a
// partition revoked while blocked hands its blocked message to the next owner:
// the revoke commits nothing past it, and the new owner receives and parks it.
func TestBlockedPartition_RevokedWhileBlockedIsRedeliveredToNewOwner(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	topic := uniqueTopic(t, mc, 1)
	group := uniqueGroupName(t)
	newC := func() *Consumer {
		c, err := NewConsumer(append(fastAtLeastOnce(),
			ConsumerWithBrokers(mc.BootstrapServers()),
			ConsumerWithGroupID(group), ConsumerWithTopics(topic),
			// Short session: the mock coordinator completes a join only after a
			// departed member's session expires.
			ConsumerWithSessionTimeout(6*time.Second), ConsumerWithHeartbeatInterval(2*time.Second))...)
		if err != nil {
			t.Fatalf("NewConsumer: %v", err)
		}
		return c
	}

	a := newC()
	a.OnMessage(func(context.Context, *Message) error { return errors.New("always fails") })
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	go func() { _ = a.Start(ctxA) }()
	defer a.Close(context.Background())

	p, err := NewProducer(ProducerWithBrokers(mc.BootstrapServers()))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()
	if err := p.Produce(context.Background(), topic, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	waitFor(t, 15*time.Second, func() bool { return len(a.Metrics().BlockedPartitions) > 0 },
		"consumer A never blocked on the failing message")

	// B holds the message in its handler until the test has checked the
	// committed offset, so "before B handled it" is observable.
	received := make(chan int64, 4)
	gate := make(chan struct{})
	b := newC()
	b.OnMessage(func(ctx context.Context, m *Message) error {
		received <- m.Offset
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	ctxB, cancelB := context.WithCancel(context.Background())
	defer b.Close(context.Background())
	defer cancelB()
	go func() { _ = b.Start(ctxB) }()

	// Closing A revokes its partitions (if B's join has not already taken it).
	cancelA()
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("close A: %v", err)
	}

	select {
	case off := <-received:
		if off != 0 {
			t.Fatalf("B first received offset %d, want 0 (the blocked message)", off)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the blocked message was never redelivered to the new owner")
	}
	if off := committedOffset(t, mc.BootstrapServers(), group, topic, 0); off >= 1 {
		t.Fatalf("committed offset = %d before B handled the message, want no advance past the blocked message", off)
	}
	close(gate)
	waitFor(t, 15*time.Second, func() bool {
		return committedOffset(t, mc.BootstrapServers(), group, topic, 0) >= 1
	}, "B never parked the redelivered message")
}
