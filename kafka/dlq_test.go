package kafka

import (
	"testing"
	"time"
)

func TestIdempotencyStore(t *testing.T) {
	t.Run("duplicate within ttl", func(t *testing.T) {
		s := newIdempotencyStore(time.Minute)
		defer s.close()
		s.add("k")
		if !s.isDuplicate("k") {
			t.Error("isDuplicate(k) = false after Add")
		}
		if s.isDuplicate("other") {
			t.Error("isDuplicate(other) = true, want false")
		}
	})
	t.Run("expired keys are not duplicates", func(t *testing.T) {
		s := newIdempotencyStore(40 * time.Millisecond)
		defer s.close()
		s.add("k")
		time.Sleep(120 * time.Millisecond) // > ttl + one cleanup tick (ttl/10)
		if s.isDuplicate("k") {
			t.Error("isDuplicate(k) = true after ttl expiry")
		}
	})
	t.Run("zero ttl is clamped, not panicked", func(t *testing.T) {
		s := newIdempotencyStore(0) // used to panic: time.NewTicker(0)
		defer s.close()
		s.add("k")
		if !s.isDuplicate("k") {
			t.Error("isDuplicate(k) = false with clamped ttl")
		}
	})
	t.Run("double close does not panic", func(_ *testing.T) {
		s := newIdempotencyStore(time.Minute)
		s.close()
		s.close() // used to panic: close of closed channel
	})
}

func TestMessageSetHeader(t *testing.T) {
	var m Message // nil Headers
	m.SetHeader("k", []byte("v"))
	if string(m.Headers["k"]) != "v" {
		t.Errorf("SetHeader on nil map: got %v", m.Headers)
	}
	m.SetHeader("k", []byte("v2"))
	if len(m.Headers) != 1 || string(m.Headers["k"]) != "v2" {
		t.Errorf("SetHeader overwrite: got %v", m.Headers)
	}
}

func TestDLQMetricsBlockedGauge(t *testing.T) {
	m := newMetricsCollector()
	m.setBlocked([]TopicPartition{{Topic: "t", Partition: 1}})
	if got := m.getMetrics().BlockedPartitions; len(got) != 1 || got[0].Partition != 1 {
		t.Errorf("BlockedPartitions = %v, want one entry for t[1]", got)
	}
	m.setBlocked(nil)
	if got := m.getMetrics().BlockedPartitions; len(got) != 0 {
		t.Errorf("BlockedPartitions after clear = %v, want empty", got)
	}
}
