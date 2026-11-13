package kafka

import (
	"context"
	"errors"
	"fmt"
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
		c.metrics.incHandlerFailures(msg.Topic)

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

// retryBudget returns the worst-case wall time of one executeWithRetry cycle
// (all sleeps, no handler time). Feeds the max.poll.interval.ms floor.
func retryBudget(r RetryConfig) time.Duration {
	maxRetries, initial, multiplier, maxInterval := r.MaxRetries, r.InitialInterval, r.Multiplier, r.MaxInterval
	var total time.Duration
	delay := initial
	for i := 0; i < maxRetries; i++ {
		total += delay
		if d := time.Duration(float64(delay) * multiplier); d > maxInterval {
			delay = maxInterval
		} else {
			delay = d
		}
	}
	return total
}
