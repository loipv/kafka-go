package kafka

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

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

func topicHighWatermark(t *testing.T, brokers, topic string) int64 {
	t.Helper()
	ac, err := ckafka.NewConsumer(&ckafka.ConfigMap{"bootstrap.servers": brokers, "group.id": uniqueGroupName(t)})
	if err != nil {
		t.Fatalf("watermark consumer: %v", err)
	}
	defer ac.Close()
	_, high, err := ac.QueryWatermarkOffsets(topic, 0, 5000)
	if err != nil {
		t.Fatalf("QueryWatermarkOffsets(%s): %v", topic, err)
	}
	return high
}

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

// fastAtLeastOnce is the common option set for the at-least-once tests:
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

func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("mock-broker test")
	}
}

func newMockCluster(t *testing.T) *ckafka.MockCluster {
	t.Helper()
	mc, err := ckafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("NewMockCluster: %v", err)
	}
	t.Cleanup(mc.Close)
	return mc
}

func uniqueTopic(t *testing.T, mc *ckafka.MockCluster, partitions int) string {
	t.Helper()
	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_")) +
		"_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := mc.CreateTopic(name, partitions, 1); err != nil {
		t.Fatalf("CreateTopic(%s): %v", name, err)
	}
	return name
}

// uniqueGroupName returns a per-run consumer group ID. Group-ID reuse across
// tests is the top flake source (stale offsets from an earlier run).
func uniqueGroupName(t *testing.T) string {
	t.Helper()
	return strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_")) +
		"_" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// waitFor polls cond until deadline. Fixed sleeps are future flakes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}
