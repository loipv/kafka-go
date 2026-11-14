package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// errInterrupted marks a handler run that shutdown cut short: the Start
// context was cancelled or Close was called during a retry wait. Such a message
// is neither parked nor blocked; it is left unstored and redelivered.
var errInterrupted = errors.New("interrupted by shutdown")

// executeWithRetry executes the handler with retry logic. The returned
// attempts count handler invocations (1 = first try, no retry). When a retry
// wait is cut short by the context ending or by Close, the error wraps
// errInterrupted together with the context's error or the last handler error.
func (c *Consumer) executeWithRetry(ctx context.Context, msg *Message) (int, error) {
	r := c.retry
	maxRetries, multiplier, maxInterval := r.MaxRetries, r.Multiplier, r.MaxInterval
	var lastErr error
	delay := r.InitialInterval

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := c.messageHandler(ctx, msg)
		if err == nil {
			return attempt + 1, nil
		}

		lastErr = err
		if ctx.Err() == nil { // an error after the context ended is shutdown, not a failure
			c.metrics.incHandlerFailures(msg.Topic)
		}

		if attempt < maxRetries {
			c.logger.Debug("retrying message", "attempt", attempt+1, "maxRetries", maxRetries, "error", err)
			select {
			case <-ctx.Done():
				return attempt + 1, fmt.Errorf("%w: %w", errInterrupted, ctx.Err())
			case <-c.done: // Close() without ctx cancel: stop retrying
				return attempt + 1, fmt.Errorf("%w: %w", errInterrupted, lastErr)
			case <-c.after(delay):
				delay = time.Duration(float64(delay) * multiplier)
				if delay > maxInterval {
					delay = maxInterval // retryBudget assumes capped sleeps — keep them capped
				}
			}
		}
	}

	if r.SkipOnMaxRetries {
		c.logger.Warn("max retries exceeded — skipping message", "error", lastErr)
		return maxRetries + 1, fmt.Errorf("%w: %w", ErrSkippedOnMaxRetries, lastErr)
	}
	return maxRetries + 1, lastErr
}

// retryBudgetLimit is where retryBudget stops adding: a budget beyond
// librdkafka's 24h max.poll.interval.ms limit is rejected whatever its exact
// value.
const retryBudgetLimit = maxPollIntervalLimitMs * time.Millisecond

// retryBudget returns the worst-case wall time of one executeWithRetry cycle
// (all sleeps, no handler time). Feeds the max.poll.interval.ms floor. It
// never overflows and never loops MaxRetries times: it stops as soon as the
// total passes retryBudgetLimit (returning what it has, or math.MaxInt64 when
// the next wait would not fit), and once the wait stops changing (it reached
// MaxInterval, or decayed to zero) it adds the remaining waits in one step.
// An absurd RetryConfig is therefore reported as over the limit instead of
// wrapping around or stalling NewConsumer.
func retryBudget(r RetryConfig) time.Duration {
	maxRetries, initial, multiplier, maxInterval := r.MaxRetries, r.InitialInterval, r.Multiplier, r.MaxInterval
	var total time.Duration
	delay := initial
	for i := 0; i < maxRetries; i++ {
		if delay > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += delay
		if total > retryBudgetLimit {
			return total
		}
		// Compare as floats: converting an out-of-range product to a
		// Duration first would wrap.
		next := maxInterval
		if f := float64(delay) * multiplier; f <= float64(maxInterval) {
			next = time.Duration(f)
		}
		if next == delay {
			// Every remaining wait equals this one.
			left := time.Duration(maxRetries - i - 1)
			if delay > 0 && left > (math.MaxInt64-total)/delay {
				return math.MaxInt64
			}
			return total + left*delay
		}
		delay = next
	}
	return total
}
