package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetry_Budget(t *testing.T) {
	// defaults: 3 retries, 1s initial, 2x => 1+2+4 = 7s
	if got, want := retryBudget(normalizeRetry(nil)), 7*time.Second; got != want {
		t.Errorf("retryBudget(normalizeRetry(nil)) = %v, want %v", got, want)
	}
	// MaxInterval caps each delay. 3 sleeps (1+2 would be followed by a capped
	// 3): 1+2+3 = 6s. (The brief's "1+2+3+3 = 9s" miscounted four sleeps for
	// three retries — executeWithRetry sleeps only between attempts.)
	r := &RetryConfig{MaxRetries: 3, InitialInterval: time.Second, Multiplier: 2, MaxInterval: 3 * time.Second}
	if got, want := retryBudget(normalizeRetry(r)), 6*time.Second; got != want {
		t.Errorf("retryBudget(capped) = %v, want %v", got, want)
	}
}

func TestRetry_SkipOnMaxRetriesAdvances(t *testing.T) {
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

// TestRetry_CapsDelay pins that the retry sleep sequence respects
// RetryConfig.MaxInterval. retryBudget (and the max.poll.interval.ms floor
// derived from it) models capped delays — uncapped sleeps would exceed the
// poll interval mid-retry and evict the consumer from the group.
func TestRetry_CapsDelay(t *testing.T) {
	c := &Consumer{
		retry: normalizeRetry(&RetryConfig{
			MaxRetries:      5,
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2,
			MaxInterval:     150 * time.Millisecond,
		}),
		logger:  slog.New(slog.DiscardHandler),
		metrics: newMetricsCollector(),
		after:   time.After, // executeWithRetry sleeps through this seam
	}
	c.OnMessage(func(context.Context, *Message) error { return errors.New("boom") })

	start := time.Now()
	attempts, _ := c.executeWithRetry(context.Background(), &Message{Topic: "t"})
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

func TestRetry_BackoffMath(t *testing.T) {
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
			c := &Consumer{retry: normalizeRetry(tt.retry),
				after: func(d time.Duration) <-chan time.Time {
					delays = append(delays, d)
					ch := make(chan time.Time, 1)
					ch <- time.Now()
					return ch
				},
				metrics: newMetricsCollector(), logger: slog.New(slog.DiscardHandler)}
			calls := 0
			c.messageHandler = func(_ context.Context, _ *Message) error {
				calls++
				if calls <= tt.failures {
					return errors.New("boom")
				}
				return nil
			}
			attempts, _ := c.executeWithRetry(context.Background(), &Message{})
			if len(delays) != len(tt.wantDelays) {
				t.Fatalf("delays = %v, want %v", delays, tt.wantDelays)
			}
			for i := range delays {
				if delays[i] != tt.wantDelays[i] {
					t.Errorf("delay[%d] = %v, want %v", i, delays[i], tt.wantDelays[i])
				}
			}
			wantCalls := tt.failures + 1
			if calls != wantCalls {
				t.Errorf("handler calls = %d, want %d", calls, wantCalls)
			}
			if !tt.wantErr && attempts != wantCalls {
				t.Errorf("attempts = %d, want %d", attempts, wantCalls)
			}
		})
	}
}

// Cancelling the context during a retry wait reports the run as interrupted
// by shutdown, wrapping the context's error.
func TestRetry_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Consumer{retry: normalizeRetry(&RetryConfig{MaxRetries: 5, InitialInterval: time.Hour}),
		after: func(time.Duration) <-chan time.Time {
			cancel()                    // the context ends while the retry wait is in progress
			return make(chan time.Time) // never fires
		},
		done:    make(chan struct{}),
		metrics: newMetricsCollector(), logger: slog.New(slog.DiscardHandler)}
	c.messageHandler = func(context.Context, *Message) error { return errors.New("boom") }

	attempts, err := c.executeWithRetry(ctx, &Message{})
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (the wait after the first attempt was interrupted)", attempts)
	}
	if !errors.Is(err, errInterrupted) || !errors.Is(err, context.Canceled) {
		t.Errorf("executeWithRetry() = %v, want an error wrapping errInterrupted and context.Canceled", err)
	}
}

// Close (c.done) during a retry wait also reports an interrupted run, wrapping
// the last handler error.
func TestRetry_CloseDuringWaitIsInterrupted(t *testing.T) {
	handlerErr := errors.New("boom")
	c := &Consumer{retry: normalizeRetry(&RetryConfig{MaxRetries: 5, InitialInterval: time.Hour}),
		done:    make(chan struct{}),
		metrics: newMetricsCollector(), logger: slog.New(slog.DiscardHandler)}
	c.after = func(time.Duration) <-chan time.Time {
		close(c.done) // Close is called while the retry wait is in progress
		return make(chan time.Time)
	}
	c.messageHandler = func(context.Context, *Message) error { return handlerErr }

	_, err := c.executeWithRetry(context.Background(), &Message{})
	if !errors.Is(err, errInterrupted) || !errors.Is(err, handlerErr) {
		t.Errorf("executeWithRetry() = %v, want an error wrapping errInterrupted and the handler error", err)
	}
}

// A handler error returned after the context ended is shutdown, not a handler
// failure: it is not counted, as in batch mode.
func TestRetry_ErrorAfterContextEndIsNotCountedAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Consumer{retry: normalizeRetry(&RetryConfig{MaxRetries: 5, InitialInterval: time.Hour}),
		after: time.After, done: make(chan struct{}),
		metrics: newMetricsCollector(), logger: slog.New(slog.DiscardHandler)}
	c.messageHandler = func(ctx context.Context, _ *Message) error {
		cancel() // shutdown arrives while the handler runs
		return ctx.Err()
	}

	if _, err := c.executeWithRetry(ctx, &Message{Topic: "t"}); !errors.Is(err, errInterrupted) {
		t.Errorf("executeWithRetry() = %v, want an error wrapping errInterrupted", err)
	}
	if n := c.Metrics().HandlerFailures; n != 0 {
		t.Errorf("HandlerFailures = %d, want 0: shutdown is not a handler failure", n)
	}
}
