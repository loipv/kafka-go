package kafka

import (
	"testing"
)

func TestMetrics_SnapshotCounts(t *testing.T) {
	m := newMetricsCollector()
	m.incHandlerFailures("a")
	m.incHandlerFailures("a")
	m.incHandlerFailures("b")
	m.incSentToDLQ("a")
	got := m.snapshot()
	if got.HandlerFailures != 3 || got.SentToDLQ != 1 {
		t.Errorf("totals = (%d failures, %d dlq), want (3, 1)", got.HandlerFailures, got.SentToDLQ)
	}
	if a := got.ByTopic["a"]; a != (TopicMetrics{HandlerFailures: 2, SentToDLQ: 1}) {
		t.Errorf("ByTopic[a] = %+v, want {2 1}", a)
	}
	if b := got.ByTopic["b"]; b != (TopicMetrics{HandlerFailures: 1}) {
		t.Errorf("ByTopic[b] = %+v, want {1 0}", b)
	}
}

func TestMetrics_BlockedGauge(t *testing.T) {
	m := newMetricsCollector()
	m.setBlocked([]TopicPartition{{Topic: "t", Partition: 1}})
	if got := m.snapshot().BlockedPartitions; len(got) != 1 || got[0].Partition != 1 {
		t.Errorf("BlockedPartitions = %v, want one entry for t[1]", got)
	}
	m.setBlocked(nil)
	if got := m.snapshot().BlockedPartitions; len(got) != 0 {
		t.Errorf("BlockedPartitions after clear = %v, want empty", got)
	}
}
