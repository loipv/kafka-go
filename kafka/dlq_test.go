package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestCircuitBreakerStateMachine(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	cb := NewCircuitBreaker(&CircuitBreakerConfig{FailureThreshold: 3, SuccessThreshold: 2, Timeout: 30 * time.Second})
	cb.now = clock.Now

	steps := []struct {
		op   string // "success" | "failure" | "advance" | "reset"
		d    time.Duration
		want CircuitState
	}{
		{"failure", 0, CircuitClosed},
		{"failure", 0, CircuitClosed},
		{"failure", 0, CircuitOpen}, // threshold reached
		{"advance", 29 * time.Second, CircuitOpen},
		{"advance", time.Second, CircuitHalfOpen}, // exactly Timeout (>=) — State() mutates on read; assert ONCE per step
		{"success", 0, CircuitHalfOpen},
		{"success", 0, CircuitClosed}, // success threshold reached, counters zeroed
		{"failure", 0, CircuitClosed}, // fresh count after close
		{"failure", 0, CircuitClosed},
		{"failure", 0, CircuitOpen},
		{"advance", 30 * time.Second, CircuitHalfOpen},
		{"failure", 0, CircuitOpen}, // any failure in half-open reopens; clock restarts
	}
	for i, s := range steps {
		switch s.op {
		case "success":
			cb.RecordSuccess()
		case "failure":
			cb.RecordFailure()
		case "advance":
			clock.t = clock.t.Add(s.d)
		case "reset":
			cb.Reset()
		}
		if got := cb.State(); got != s.want { // exactly one state read per step — a read can itself transition
			t.Errorf("step %d (%s %v): state = %s, want %s", i, s.op, s.d, got, s.want)
		}
	}
}

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }

func TestIdempotencyStore(t *testing.T) {
	t.Run("duplicate within ttl", func(t *testing.T) {
		s := NewIdempotencyStore(time.Minute)
		defer s.Close()
		s.Add("k")
		if !s.IsDuplicate("k") {
			t.Error("IsDuplicate(k) = false after Add")
		}
		if s.IsDuplicate("other") {
			t.Error("IsDuplicate(other) = true, want false")
		}
	})
	t.Run("expired keys are not duplicates", func(t *testing.T) {
		s := NewIdempotencyStore(40 * time.Millisecond)
		defer s.Close()
		s.Add("k")
		time.Sleep(120 * time.Millisecond) // > ttl + one cleanup tick (ttl/10)
		if s.IsDuplicate("k") {
			t.Error("IsDuplicate(k) = true after ttl expiry")
		}
	})
	t.Run("zero ttl is clamped, not panicked", func(t *testing.T) {
		s := NewIdempotencyStore(0) // used to panic: time.NewTicker(0)
		defer s.Close()
		s.Add("k")
		if !s.IsDuplicate("k") {
			t.Error("IsDuplicate(k) = false with clamped ttl")
		}
	})
	t.Run("double close does not panic", func(_ *testing.T) {
		s := NewIdempotencyStore(time.Minute)
		s.Close()
		s.Close() // used to panic: close of closed channel
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
	m := NewDLQMetricsCollector()
	m.SetBlocked([]TopicPartition{{Topic: "t", Partition: 1}})
	if got := m.GetMetrics().BlockedPartitions; len(got) != 1 || got[0].Partition != 1 {
		t.Errorf("BlockedPartitions = %v, want one entry for t[1]", got)
	}
	m.SetBlocked(nil)
	if got := m.GetMetrics().BlockedPartitions; len(got) != 0 {
		t.Errorf("BlockedPartitions after clear = %v, want empty", got)
	}
}

// TestDLQRetryGraduatesToFinalDLQ pins the re-produce-then-commit contract: a
// message whose handler always fails must carry its reprocess count forward
// (re-produced to the DLQ topic each failed cycle, original committed only
// after that succeeds) until MaxRetries is reached and it lands on the final
// DLQ topic. Before the fix the count mutated only in memory — redelivery
// reset it to 0 and the message retried at base delay forever.
func TestDLQRetryGraduatesToFinalDLQ(t *testing.T) {
	skipIfShort(t)
	mc := newMockCluster(t)
	source := uniqueTopic(t, mc, 1)
	dlqTopic := uniqueTopic(t, mc, 1)
	finalTopic := uniqueTopic(t, mc, 1)

	c, err := NewConsumer(append(fastAtLeastOnce(),
		ConsumerWithBrokers(mc.BootstrapServers()),
		ConsumerWithGroupID(uniqueGroupName(t)),
		ConsumerWithTopics(source),
		ConsumerWithDLQ(&DLQConfig{Topic: dlqTopic, IncludeErrorInfo: true}),
		ConsumerWithDLQRetry(&DLQRetryConfig{
			Enabled:           true,
			MaxRetries:        2,
			Delay:             50 * time.Millisecond,
			BackoffMultiplier: 1,
			FinalDLQTopic:     finalTopic,
			FromBeginning:     true, // fresh group must not miss the first DLQ produce
		}),
	)...)
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
	if err := p.Produce(context.Background(), source, &Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	scratch, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers": mc.BootstrapServers(),
		"group.id":          uniqueGroupName(t),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		t.Fatalf("scratch consumer: %v", err)
	}
	defer scratch.Close()
	if err := scratch.Subscribe(finalTopic, nil); err != nil {
		t.Fatalf("subscribe final: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := scratch.ReadMessage(200 * time.Millisecond); err == nil {
			return // graduated: the message reached the final DLQ topic
		}
	}
	t.Fatal("message never graduated to the final DLQ topic")
}
