package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestExecuteWithRetryCapsDelay pins that the retry sleep sequence respects
// RetryConfig.MaxInterval. retryBudget (and the max.poll.interval.ms floor
// derived from it) models capped delays — uncapped sleeps would exceed the
// poll interval mid-retry and evict the consumer from the group.
func TestExecuteWithRetryCapsDelay(t *testing.T) {
	c := &Consumer{
		config: &ConsumerConfig{Retry: &RetryConfig{
			MaxRetries:      5,
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2,
			MaxInterval:     150 * time.Millisecond,
		}},
		logger:  NewNoopLogger(),
		metrics: NewDLQMetricsCollector(),
	}
	c.OnMessage(func(context.Context, *Message) error { return errors.New("boom") })

	start := time.Now()
	_, attempts := c.executeWithRetry(context.Background(), &Message{Topic: "t"})
	elapsed := time.Since(start)

	if attempts != 6 {
		t.Fatalf("attempts = %d, want 6 (1 initial + 5 retries)", attempts)
	}
	// Capped sleeps: 100+150+150+150+150 = 700ms. Uncapped would be
	// 100+200+400+800+1600 = 3100ms — well past the 2s bound.
	if elapsed < 650*time.Millisecond {
		t.Errorf("elapsed %v, delays shorter than specified", elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("elapsed %v, want <2s: MaxInterval cap not applied (uncapped sum is 3.1s)", elapsed)
	}
}
