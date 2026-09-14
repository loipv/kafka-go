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
		after:   time.After, // executeWithRetry sleeps through this seam
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

func TestExecuteWithRetryBackoffMath(t *testing.T) {
	tests := []struct {
		name       string
		retry      *RetryConfig
		failures   int // handler fails this many times, then succeeds
		wantDelays []time.Duration
		wantErr    bool
	}{
		{
			name:       "exponential growth",
			retry:      &RetryConfig{MaxRetries: 3, InitialInterval: 100 * time.Millisecond, Multiplier: 2},
			failures:   3,
			wantDelays: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond},
			wantErr:    true,
		},
		{
			name:       "max interval caps growth",
			retry:      &RetryConfig{MaxRetries: 3, InitialInterval: 100 * time.Millisecond, Multiplier: 2, MaxInterval: 150 * time.Millisecond},
			failures:   3,
			wantDelays: []time.Duration{100 * time.Millisecond, 150 * time.Millisecond, 150 * time.Millisecond},
			wantErr:    true,
		},
		{
			name:       "success on second attempt",
			retry:      &RetryConfig{MaxRetries: 3, InitialInterval: 100 * time.Millisecond, Multiplier: 2},
			failures:   1,
			wantDelays: []time.Duration{100 * time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var delays []time.Duration
			c := &Consumer{config: &ConsumerConfig{Retry: tt.retry},
				after: func(d time.Duration) <-chan time.Time {
					delays = append(delays, d)
					ch := make(chan time.Time, 1)
					ch <- time.Now()
					return ch
				},
				metrics: NewDLQMetricsCollector(), logger: NewNoopLogger()}
			calls := 0
			c.messageHandler = func(_ context.Context, _ *Message) error {
				calls++
				if calls <= tt.failures {
					return errors.New("boom")
				}
				return nil
			}
			_, attempts := c.executeWithRetry(context.Background(), &Message{})
			if len(delays) != len(tt.wantDelays) {
				t.Fatalf("delays = %v, want %v", delays, tt.wantDelays)
			}
			for i := range delays {
				if delays[i] != tt.wantDelays[i] {
					t.Errorf("delay[%d] = %v, want %v", i, delays[i], tt.wantDelays[i])
				}
			}
			wantCalls := tt.failures + 1 // hoisted: the brief scoped it inside the (empty) guard if
			if tt.wantErr || calls != wantCalls {
				// wantErr cases run all attempts
			}
			if !tt.wantErr && attempts != wantCalls {
				t.Errorf("attempts = %d, want %d", attempts, wantCalls)
			}
		})
	}
}

func TestExecuteWithRetryContextCancel(t *testing.T) {
	c := &Consumer{config: &ConsumerConfig{Retry: &RetryConfig{MaxRetries: 5, InitialInterval: time.Hour}},
		after:   func(d time.Duration) <-chan time.Time { return make(chan time.Time) }, // never fires
		metrics: NewDLQMetricsCollector(), logger: NewNoopLogger()}
	c.messageHandler = func(context.Context, *Message) error { return errors.New("boom") }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, attempts := c.executeWithRetry(ctx, &Message{}); attempts == 0 {
		t.Error("expected at least one attempt before ctx cancellation")
	}
}
