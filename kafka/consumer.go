package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Consumer is a handler-based Kafka consumer.
type Consumer struct {
	consumer *ckafka.Consumer
	config   *ConsumerConfig
	tracer   *tracingService
	logger   *slog.Logger

	// Handlers
	messageHandler      MessageHandler
	batchHandler        BatchHandler
	groupedBatchHandler GroupedBatchHandler

	// State - using atomic for hot path operations
	mu           sync.RWMutex
	running      int32 // atomic: 0=stopped, 1=running
	paused       int32 // atomic: 0=running, 1=paused
	pauseApplied int32 // atomic: pause/resume actually applied to librdkafka
	closed       int32 // atomic: 0=open, 1=closed
	handleClosed int32 // atomic: set once the librdkafka handle is about to be destroyed

	// Lifecycle / batch flush
	done       chan struct{} // closed by Close; stops helper goroutines
	loopExited chan struct{} // closed when the main consume loop returns
	flushCh    chan struct{} // cap 1: batch-flush signal consumed in the main loop (#18)

	// Batch processing
	batchMu sync.Mutex
	batch   []*Message

	// Idempotency
	idempotencyStore *idempotencyStore

	// DLQ
	dlqService *dlqService
	metrics    *metricsCollector

	// retry is the resolved RetryConfig (defaults applied once in NewConsumer).
	retry RetryConfig

	// after is the sleep seam for retry waits; tests swap it to make
	// backoff cycles instantaneous. Defaults to time.After.
	after func(time.Duration) <-chan time.Time

	// Blocked partitions (at-least-once): partitions paused pending an
	// escalating retry deadline because their message is unparkable.
	blockMu sync.Mutex
	blocked map[TopicPartition]blockState
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(opts ...ConsumerOption) (*Consumer, error) {
	config := newDefaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if len(config.Brokers) == 0 {
		return nil, ErrBrokersRequired
	}

	if config.GroupID == "" {
		return nil, ErrGroupIDRequired
	}

	if len(config.Topics) == 0 {
		return nil, ErrTopicsRequired
	}

	if config.DLQ != nil && config.DLQ.Topic == "" {
		return nil, ErrDLQTopicRequired
	}

	if config.BatchSize <= 0 {
		return nil, ErrInvalidBatchSize
	}

	if config.BatchTimeout <= 0 {
		return nil, ErrInvalidBatchTimeout
	}

	warnPlaintextSASL(config.Logger, config.conn())
	if config.DLQ != nil && len(config.DLQ.Brokers) > 0 {
		warnPlaintextSASL(config.Logger, config.dlqConn())
	}

	// Build kafka config map (connection/auth via the connConfig seam)
	configMap, err := buildConsumerConfig(config)
	if err != nil {
		return nil, err
	}

	consumer, err := ckafka.NewConsumer(&configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = slog.Default() // silence with slog.New(slog.DiscardHandler)
	}

	kc := &Consumer{
		consumer: consumer,
		config:   config,
		logger:   logger,
		done:     make(chan struct{}),
		flushCh:  make(chan struct{}, 1),
		batch:    make([]*Message, 0, config.BatchSize),
		metrics:  newMetricsCollector(),
		blocked:  make(map[TopicPartition]blockState),
		after:    time.After,
		retry:    normalizeRetry(config.Retry),
	}

	// Initialize tracing if enabled
	if config.Tracing != nil && config.Tracing.Enabled {
		kc.tracer = newTracingService(config.Tracing)
	}

	// Initialize idempotency store if configured
	if config.IdempotencyKey != nil {
		kc.idempotencyStore = newIdempotencyStore(config.IdempotencyTTL)
	}

	// Initialize DLQ service if configured
	if config.DLQ != nil {
		kc.dlqService, err = newDLQService(config.dlqConn(), config.DLQ, kc.metrics)
		if err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("failed to create DLQ service: %w", err)
		}
	}

	return kc, nil
}

// OnMessage registers a handler for single messages. Only one handler is
// kept: a later On* registration replaces the earlier one.
func (c *Consumer) OnMessage(handler MessageHandler) {
	c.messageHandler, c.batchHandler, c.groupedBatchHandler = handler, nil, nil
}

// OnBatch registers a handler for batch messages and turns batch mode on.
// Only one handler is kept: a later On* registration replaces the earlier one.
func (c *Consumer) OnBatch(handler BatchHandler) {
	c.messageHandler, c.batchHandler, c.groupedBatchHandler = nil, handler, nil
}

// OnGroupedBatch registers a handler for key-grouped batches and turns batch
// mode on. Only one handler is kept: a later On* registration replaces the
// earlier one.
func (c *Consumer) OnGroupedBatch(handler GroupedBatchHandler) {
	c.messageHandler, c.batchHandler, c.groupedBatchHandler = nil, nil, handler
}

// batchMode reports whether messages are delivered in batches, which is the
// case exactly when a batch or grouped batch handler is registered.
func (c *Consumer) batchMode() bool {
	return c.batchHandler != nil || c.groupedBatchHandler != nil
}

// isFatalClientError reports whether err is (or wraps) a fatal librdkafka error.
func isFatalClientError(err error) bool {
	var ke ckafka.Error
	return errors.As(err, &ke) && ke.IsFatal()
}

// Start starts consuming messages (blocking). It returns on context
// cancellation, on Close, or on a fatal client error.
func (c *Consumer) Start(ctx context.Context) error {
	if c.messageHandler == nil && c.batchHandler == nil && c.groupedBatchHandler == nil {
		return ErrNoHandler // #17
	}
	if atomic.LoadInt32(&c.closed) == 1 {
		return ErrConsumerClosed
	}
	// Use atomic CAS to ensure only one Start can succeed
	if !atomic.CompareAndSwapInt32(&c.running, 0, 1) {
		return ErrConsumerRunning
	}
	// Publish this run's exit signal only after winning the CAS: a failed
	// Start must never replace the channel the already-running loop closes,
	// or Close would wait on an orphan (guaranteed 5s stall, then a close
	// racing an in-flight poll).
	c.mu.Lock()
	c.loopExited = make(chan struct{})
	c.mu.Unlock()
	defer atomic.StoreInt32(&c.running, 0) // #8: every exit path releases
	defer close(c.loopExited)              // signals Close that the loop is gone

	// Subscribe to topics; the rebalance callback is always installed so the
	// library's own commit/assign logic runs even without a user callback (#31)
	if err := c.consumer.SubscribeTopics(c.config.Topics, c.rebalanceCallback()); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	// Start batch flush ticker if a batch handler is registered
	if c.batchMode() {
		go c.batchFlushLoop(ctx)
	}

	// Main consume loop
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return nil // Close() stops the loop without ctx cancellation
		case <-c.flushCh:
			c.processBatch(ctx)
		default:
			c.reconcilePause() // #12: real pause, reconciled every pass
			c.resumeBlocked()  // unblock partitions whose backoff elapsed

			msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Timeout is normal, continue
				var ke ckafka.Error
				if errors.As(err, &ke) && ke.Code() == ckafka.ErrTimedOut {
					continue
				}
				if isFatalClientError(err) {
					// The client is unusable; spinning on it would only spam the log.
					c.logger.Error("fatal consumer error", "error", err)
					return fmt.Errorf("fatal consumer error: %w", err)
				}
				// Log other errors but continue
				c.logger.Warn("read message failed", "error", err)
				continue
			}

			// Convert to our Message type
			message := c.convertMessage(msg)

			// Process message
			if c.batchMode() {
				c.addToBatch(ctx, message)
			} else {
				c.deliver(ctx, message)
			}
		}
	}
}

// Close closes the consumer. Cancel the Start context before calling Close
// when handlers or retries can exceed the internal wait: without a cancel,
// Close falls back to a bounded 5s wait for the consume loop and may then
// close the handle while a long retry is still in flight.
func (c *Consumer) Close(ctx context.Context) error {
	wasRunning := atomic.LoadInt32(&c.running) == 1
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	atomic.StoreInt32(&c.running, 0)

	// Stop helper goroutines (batch flush ticker)
	close(c.done)

	// Wait for the main loop to leave ReadMessage before destroying the
	// handle: consumer.Close() during an in-flight cgo poll segfaults.
	if wasRunning {
		c.mu.RLock()
		loopExited := c.loopExited
		c.mu.RUnlock()
		select {
		case <-loopExited:
		case <-time.After(5 * time.Second):
			c.logger.Warn("consumer loop did not exit within 5s; closing anyway")
		}
	}

	// Process remaining batch
	if c.batchMode() {
		c.processBatch(ctx)
	}

	// Close DLQ service
	if c.dlqService != nil {
		_ = c.dlqService.Close()
	}

	// Close idempotency store
	if c.idempotencyStore != nil {
		c.idempotencyStore.close()
	}

	atomic.StoreInt32(&c.handleClosed, 1)
	return c.consumer.Close()
}

// Pause pauses consumption
func (c *Consumer) Pause() {
	atomic.StoreInt32(&c.paused, 1)
}

// Resume resumes consumption
func (c *Consumer) Resume() {
	atomic.StoreInt32(&c.paused, 0)
}

// reconcilePause applies the user-visible Pause/Resume to the current
// assignment. librdkafka resets pause state on every rebalance, so this
// must run continuously rather than once — and Assignment() is empty before
// the first rebalance completes, which the len check absorbs.
func (c *Consumer) reconcilePause() {
	want := atomic.LoadInt32(&c.paused) == 1
	applied := atomic.LoadInt32(&c.pauseApplied) == 1
	if want == applied {
		return
	}
	partitions, err := c.consumer.Assignment()
	if err != nil || len(partitions) == 0 {
		return
	}
	var perr error
	if want {
		perr = c.consumer.Pause(partitions)
	} else {
		perr = c.consumer.Resume(partitions)
	}
	if perr == nil {
		atomic.StoreInt32(&c.pauseApplied, boolToInt32(want))
	}
}

func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// DLQMetrics returns DLQ metrics
func (c *Consumer) DLQMetrics() *DLQMetrics {
	return c.metrics.getMetrics()
}

// convertMessage converts ckafka.Message to Message
// Optimized to avoid allocation when there are no headers
func (c *Consumer) convertMessage(msg *ckafka.Message) *Message {
	var headers Headers
	if len(msg.Headers) > 0 {
		headers = make(Headers, len(msg.Headers)) // Pre-sized allocation
		for _, h := range msg.Headers {
			headers[h.Key] = h.Value
		}
	}

	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	return &Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Timestamp: msg.Timestamp,
		Topic:     topic,
	}
}

// processMessage processes a single message and reports whether it parked
// (durably dealt with — its offset may be stored) or blocked (unparkable —
// its partition must be paused and retried).
func (c *Consumer) processMessage(ctx context.Context, msg *Message) outcome {
	// Check idempotency BEFORE processing. A skipped duplicate is parked: its
	// offset may advance (it was already dealt with on a previous delivery).
	var idempotencyKey string
	if c.idempotencyStore != nil && c.config.IdempotencyKey != nil {
		idempotencyKey = c.config.IdempotencyKey(msg)
		if idempotencyKey != "" && c.idempotencyStore.isDuplicate(idempotencyKey) {
			c.logger.Debug("skipping duplicate message",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
			return outcomeParked
		}
	}

	// Start tracing span
	var endSpan func(error)
	if c.tracer != nil {
		ctx, endSpan = c.tracer.startConsumerSpan(ctx, c.config.GroupID, msg)
	}

	// Execute handler with retry
	attempts, err := c.executeWithRetry(ctx, msg)

	if err == nil {
		if endSpan != nil {
			endSpan(nil)
		}
		// Only mark as processed on SUCCESS — the idempotency-key store is
		// skipped on error; the offset is not stored either (the partition
		// blocks), so the message will be redelivered.
		if c.idempotencyStore != nil && idempotencyKey != "" {
			c.idempotencyStore.add(idempotencyKey)
		}
		return outcomeParked
	}

	if endSpan != nil {
		endSpan(err)
	}
	if errors.Is(err, ErrSkippedOnMaxRetries) {
		// The one explicit opt-in to loss: best-effort DLQ + notify, then advance.
		_ = c.handleError(ctx, err, msg, attempts)
		return outcomeParked
	}
	return c.handleError(ctx, err, msg, attempts)
}

// deliver is the poll-loop entry for a single message: park (store the offset)
// or block (pause the partition and seek back).
func (c *Consumer) deliver(ctx context.Context, msg *Message) {
	switch c.processMessage(ctx, msg) {
	case outcomeParked:
		c.clearBlockedOnPark(msg)
		c.storeOffsets([]*Message{msg})
	case outcomeBlocked:
		c.blockMessages(ctx, []*Message{msg})
	}
}

// executeWithRetry executes the handler with retry logic. The returned
// attempts count handler invocations (1 = first try, no retry).
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
		c.metrics.incrementHandlerRetries(msg.Topic)

		if attempt < maxRetries {
			c.logger.Debug("retrying message", "attempt", attempt+1, "maxRetries", maxRetries, "error", err)
			select {
			case <-ctx.Done():
				return attempt + 1, ctx.Err()
			case <-c.done: // Close() without ctx cancel: stop retrying, report the last error
				return attempt + 1, lastErr
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

// handleError parks or blocks a failed message: the error handler can claim
// ownership (nil return = parked), then the DLQ; with neither, or when the DLQ
// produce fails, the message is unparkable and its partition blocks.
func (c *Consumer) handleError(ctx context.Context, err error, msg *Message, attempts int) outcome {
	if c.config.ErrorHandler != nil {
		if handled := c.config.ErrorHandler(ctx, msg, err); handled == nil {
			return outcomeParked // user took ownership
		}
	}
	if c.dlqService != nil {
		if dlqErr := c.dlqService.produceToDLQ(ctx, msg, err, attempts); dlqErr != nil {
			c.logger.Error("dlq produce failed", "error", dlqErr)
			return outcomeBlocked
		}
		return outcomeParked
	}
	return outcomeBlocked
}

// addToBatch adds a message to the batch
func (c *Consumer) addToBatch(ctx context.Context, msg *Message) {
	c.batchMu.Lock()
	c.batch = append(c.batch, msg)
	batchSize := len(c.batch)
	c.batchMu.Unlock()

	// Process batch if full
	if batchSize >= c.config.BatchSize {
		c.processBatch(ctx)
	}
}

// processBatch processes the current batch and reports whether the batch
// parked or blocked. Whole-batch semantics: if ANY message is unparkable,
// nothing is stored and the whole batch blocks (everything is re-delivered;
// duplicates of the successes are what ConsumerWithIdempotencyKey is for).
func (c *Consumer) processBatch(ctx context.Context) outcome {
	c.batchMu.Lock()
	if len(c.batch) == 0 {
		c.batchMu.Unlock()
		return outcomeParked
	}
	batch := c.batch
	c.batch = make([]*Message, 0, c.config.BatchSize)
	c.batchMu.Unlock()

	// Drop messages whose idempotency key was already processed. Duplicates
	// are parked (offsets advance with the original batch) but never reach the
	// handler, the error handler or the DLQ.
	kept, keys := c.filterDuplicates(batch)
	if dropped := len(batch) - len(kept); dropped > 0 {
		c.logger.Debug("skipping duplicate messages in batch", "count", dropped)
	}

	var processingErr error
	if len(kept) > 0 {
		// Start tracing span for what the handler receives
		var endSpan func(error)
		if c.tracer != nil {
			ctx, endSpan = c.tracer.startBatchConsumerSpan(ctx, c.config.GroupID, kept)
		}

		if c.groupedBatchHandler != nil {
			processingErr = c.groupedBatchHandler(ctx, c.groupByKey(kept))
		} else {
			processingErr = c.batchHandler(ctx, kept)
		}

		// End span with error if any
		if endSpan != nil {
			endSpan(processingErr)
		}
	}

	// Batch handler error: only the messages the handler saw go to the error
	// handler / DLQ
	blocked := false
	if processingErr != nil {
		blocked = c.handleBatchError(ctx, processingErr, kept)
	}

	if blocked {
		c.blockMessages(ctx, batch)
		return outcomeBlocked
	}
	if processingErr == nil && c.idempotencyStore != nil {
		for _, key := range keys {
			if key != "" {
				c.idempotencyStore.add(key)
			}
		}
	}
	for _, msg := range batch {
		c.clearBlockedOnPark(msg)
	}
	c.storeOffsets(batch)
	return outcomeParked
}

// filterDuplicates drops messages whose idempotency key the store has already
// recorded, and messages repeating a key seen earlier in the same batch.
// keys[i] is the key of kept[i] ("" when the key function returns empty, which
// is never filtered). With no store the batch is returned unchanged.
func (c *Consumer) filterDuplicates(batch []*Message) (kept []*Message, keys []string) {
	if c.idempotencyStore == nil || c.config.IdempotencyKey == nil {
		return batch, make([]string, len(batch))
	}
	inBatch := make(map[string]struct{}, len(batch))
	for _, msg := range batch {
		key := c.config.IdempotencyKey(msg)
		if key != "" {
			if _, dup := inBatch[key]; dup || c.idempotencyStore.isDuplicate(key) {
				continue
			}
			inBatch[key] = struct{}{}
		}
		kept = append(kept, msg)
		keys = append(keys, key)
	}
	return kept, keys
}

// storeOffsets stores (does not commit) offsets for the given messages.
func (c *Consumer) storeOffsets(msgs []*Message) {
	if len(msgs) == 0 {
		return
	}
	if _, err := c.consumer.StoreOffsets(commitOffsets(msgs)); err != nil {
		c.logger.Warn("store offsets failed", "error", err)
	}
}

// groupByKey groups messages by key, preserving first-seen order.
func (c *Consumer) groupByKey(msgs []*Message) []GroupedBatch {
	if len(msgs) == 0 {
		return nil
	}

	groups := make(map[string][]*Message, len(msgs))
	order := make([]string, 0, len(msgs))

	for _, msg := range msgs {
		key := string(msg.Key)
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], msg)
	}

	result := make([]GroupedBatch, 0, len(groups))
	for _, key := range order {
		result = append(result, GroupedBatch{
			Key:      key,
			Messages: groups[key],
		})
	}

	return result
}

// handleBatchError routes a batch-level error: every message is first offered
// to the error handler (nil return = it took ownership), then all unclaimed
// messages go to the DLQ in one produce-then-await pass. It reports whether the
// batch is unparkable (no DLQ for an unclaimed message, or the DLQ produce
// failed). The batch handler runs once — no executeWithRetry — so attempts is 1.
func (c *Consumer) handleBatchError(ctx context.Context, err error, batch []*Message) bool {
	unclaimed := batch
	if c.config.ErrorHandler != nil {
		unclaimed = make([]*Message, 0, len(batch))
		for _, msg := range batch {
			if c.config.ErrorHandler(ctx, msg, err) != nil {
				unclaimed = append(unclaimed, msg)
			}
		}
	}
	if len(unclaimed) == 0 {
		return false
	}
	if c.dlqService == nil {
		return true
	}
	if dlqErr := c.dlqService.produceAll(ctx, unclaimed, err, 1); dlqErr != nil {
		c.logger.Error("dlq produce failed", "error", dlqErr)
		return true
	}
	return false
}

// batchFlushLoop ticks every BatchTimeout and signals the main loop to flush.
// It never processes the batch itself — flushing on two goroutines raced the
// main loop's own size-triggered flush (#18). The non-blocking send collapses
// multiple ticks into one pending flush.
func (c *Consumer) batchFlushLoop(ctx context.Context) {
	t := time.NewTicker(c.config.BatchTimeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-t.C:
			select {
			case c.flushCh <- struct{}{}:
			default: // a flush is already pending
			}
		}
	}
}

// rebalanceCallback is ALWAYS installed (even without a user callback) so the
// library's own commit-before-revoke runs (#31). Under a cooperative assignor,
// Assign/Unassign would replace the whole assignment mid-rebalance — an
// API violation — so the protocol is checked and incremental variants used (#32).
func (c *Consumer) rebalanceCallback() ckafka.RebalanceCb {
	return func(consumer *ckafka.Consumer, event ckafka.Event) error {
		cooperative := consumer.GetRebalanceProtocol() == "COOPERATIVE"
		switch e := event.(type) {
		case ckafka.AssignedPartitions:
			c.logger.Info("partitions assigned", "count", len(e.Partitions))
			atomic.StoreInt32(&c.pauseApplied, 0) // rebalance resets librdkafka pause state

			// Call user's callback
			if c.config.RebalanceCallback != nil {
				if err := c.config.RebalanceCallback(RebalanceEvent{Type: "assigned", Partitions: convertPartitions(e.Partitions)}); err != nil {
					c.logger.Error("rebalance callback error on assign", "error", err)
					return err
				}
			}

			if cooperative {
				return consumer.IncrementalAssign(e.Partitions) // #32
			}
			return consumer.Assign(e.Partitions)

		case ckafka.RevokedPartitions:
			c.logger.Info("partitions revoked", "count", len(e.Partitions))

			// Call user's callback
			if c.config.RebalanceCallback != nil {
				if err := c.config.RebalanceCallback(RebalanceEvent{Type: "revoked", Partitions: convertPartitions(e.Partitions)}); err != nil {
					c.logger.Error("rebalance callback error on revoke", "error", err)
					return err
				}
			}

			// Commit any pending offsets before unassigning (if auto-commit is disabled)
			if !c.config.AutoCommit {
				// "no offset stored" is not a failure
				if _, err := consumer.Commit(); ignoreNoOffset(err) != nil {
					c.logger.Warn("failed to commit offsets during rebalance", "error", err)
				}
			}

			c.dropBlockedFor(e.Partitions) // no-op until Task 9

			if cooperative {
				return consumer.IncrementalUnassign(e.Partitions) // #32
			}
			return consumer.Unassign()
		}

		return nil
	}
}

// outcome is what processMessage/processBatch report to the poll loop: the
// message(s) parked (offset may be stored) or blocked (partition must pause).
type outcome int

const (
	outcomeParked  outcome = iota // offset may be stored — message is durably dealt with
	outcomeBlocked                // unparkable — partition must block and retry
)

// ignoreNoOffset maps librdkafka's "no offset stored" (nothing to commit) to nil
// and returns every other error unchanged.
func ignoreNoOffset(err error) error {
	var ke ckafka.Error
	if errors.As(err, &ke) && ke.Code() == ckafka.ErrNoOffset {
		return nil
	}
	return err
}

// Commit stores and commits offsets for the given messages. With no
// arguments it commits everything stored so far (nil when there is nothing to
// commit) — offsets are stored only when messages are parked, not on
// consumption. Offsets are committed at max(msg.Offset)+1 per (topic,
// partition).
func (c *Consumer) Commit(msgs ...*Message) error {
	if atomic.LoadInt32(&c.handleClosed) == 1 {
		return ErrConsumerClosed
	}
	if len(msgs) == 0 {
		_, err := c.consumer.Commit()
		return ignoreNoOffset(err)
	}
	if _, err := c.consumer.StoreOffsets(commitOffsets(msgs)); err != nil {
		return fmt.Errorf("store offsets: %w", err)
	}
	_, err := c.consumer.CommitOffsets(commitOffsets(msgs))
	return err
}

// blockState is the per-partition blocked bookkeeping. offset is the lowest
// blocked offset seen — a forward seek must never overwrite it (queued
// messages can re-block with a higher offset, which would strand the
// original). Entries survive resumes: blocks escalate until the message
// finally parks (clearBlockedOnPark) or the partition is revoked.
type blockState struct {
	retryAt time.Time
	blocks  int
	offset  int64
	resumed bool
}

// blockMessages pauses the affected partitions, seeks them back to the
// failed messages' offsets, and records an escalating retry-at deadline.
// The poll loop keeps running — other partitions flow normally.
// ponytail: per-poll linear scan of c.blocked — fine for hundreds of
// partitions; index by time if it ever isn't.
func (c *Consumer) blockMessages(_ context.Context, msgs []*Message) {
	if len(msgs) == 0 {
		return
	}
	minOff := map[TopicPartition]int64{}
	for _, m := range msgs {
		tp := TopicPartition{Topic: m.Topic, Partition: m.Partition}
		if o, ok := minOff[tp]; !ok || m.Offset < o {
			minOff[tp] = m.Offset
		}
	}
	now := time.Now()
	var paused []ckafka.TopicPartition
	c.blockMu.Lock()
	for tp, off := range minOff {
		bs := c.blocked[tp]
		if bs.blocks == 0 || off < bs.offset {
			bs.offset = off
		}
		bs.blocks++
		bs.resumed = false
		bs.retryAt = now.Add(blockBackoff(bs.blocks, c.retry))
		c.blocked[tp] = bs
		topic := tp.Topic
		paused = append(paused, ckafka.TopicPartition{Topic: &topic, Partition: tp.Partition, Offset: ckafka.Offset(bs.offset)})
		c.logger.Warn("partition blocked — message unparkable",
			"topic", tp.Topic, "partition", tp.Partition, "offset", bs.offset,
			"blocks", bs.blocks, "retryIn", blockBackoff(bs.blocks, c.retry))
	}
	c.blockMu.Unlock()
	c.metrics.setBlocked(c.blockedSnapshot())
	if err := c.consumer.Pause(paused); err != nil {
		c.logger.Error("pause failed for blocked partition (next pass re-attempts)", "error", err)
	}
	for _, tp := range paused {
		if err := c.consumer.Seek(tp, 5000); err != nil {
			t := ""
			if tp.Topic != nil {
				t = *tp.Topic
			}
			c.logger.Error("seek failed for blocked partition",
				"topic", t, "partition", tp.Partition, "offset", tp.Offset, "error", err)
		}
	}
}

// resumeBlocked resumes partitions whose backoff elapsed. Called each poll
// pass. Skips partitions while the user-level Pause() is active. Entries are
// NOT deleted here: blocks must keep escalating across cycles until the
// message finally parks (clearBlockedOnPark) — deleting on resume would reset
// the backoff to InitialInterval and make a poison partition retry flat-out
// forever.
func (c *Consumer) resumeBlocked() {
	if atomic.LoadInt32(&c.paused) == 1 {
		return
	}
	now := time.Now()
	var toResume []ckafka.TopicPartition
	c.blockMu.Lock()
	for tp, bs := range c.blocked {
		if !bs.resumed && now.After(bs.retryAt) {
			bs.resumed = true
			c.blocked[tp] = bs
			topic := tp.Topic
			toResume = append(toResume, ckafka.TopicPartition{Topic: &topic, Partition: tp.Partition})
		}
	}
	c.blockMu.Unlock()
	if len(toResume) > 0 {
		if err := c.consumer.Resume(toResume); err != nil {
			c.logger.Error("resume failed for blocked partition (next pass re-attempts)", "error", err)
			// Roll the resumed flag back so the next pass really does
			// re-attempt: leaving it set would park the partition until a
			// rebalance drops the blocked state.
			c.blockMu.Lock()
			for _, tp := range toResume {
				if tp.Topic == nil {
					continue
				}
				key := TopicPartition{Topic: *tp.Topic, Partition: tp.Partition}
				if bs, ok := c.blocked[key]; ok {
					bs.resumed = false
					c.blocked[key] = bs
				}
			}
			c.blockMu.Unlock()
		}
	}
}

// clearBlockedOnPark drops the blocked state for a partition once its message
// parked at or past the blocked offset — a success resets the escalation.
func (c *Consumer) clearBlockedOnPark(msg *Message) {
	tp := TopicPartition{Topic: msg.Topic, Partition: msg.Partition}
	cleared := false
	c.blockMu.Lock()
	if bs, ok := c.blocked[tp]; ok && msg.Offset >= bs.offset {
		delete(c.blocked, tp)
		cleared = true
	}
	c.blockMu.Unlock()
	if cleared {
		c.metrics.setBlocked(c.blockedSnapshot())
	}
}

func (c *Consumer) blockedSnapshot() []TopicPartition {
	c.blockMu.Lock()
	defer c.blockMu.Unlock()
	out := make([]TopicPartition, 0, len(c.blocked))
	for tp := range c.blocked {
		out = append(out, tp)
	}
	return out
}

// dropBlockedFor drops blocked-partition bookkeeping for the given partitions
// (called on revoke — a revoked partition is no longer ours to resume).
func (c *Consumer) dropBlockedFor(ps []ckafka.TopicPartition) {
	c.blockMu.Lock()
	for _, tp := range ps {
		if tp.Topic == nil {
			continue
		}
		delete(c.blocked, TopicPartition{Topic: *tp.Topic, Partition: tp.Partition})
	}
	c.blockMu.Unlock()
	c.metrics.setBlocked(c.blockedSnapshot())
}

// convertPartitions converts ckafka.TopicPartition to our TopicPartition type
func convertPartitions(ps []ckafka.TopicPartition) []TopicPartition {
	out := make([]TopicPartition, len(ps))
	for i, tp := range ps {
		out[i] = TopicPartition{Partition: tp.Partition, Offset: int64(tp.Offset)}
		if tp.Topic != nil {
			out[i].Topic = *tp.Topic
		}
	}
	return out
}

// Helper functions

// commitOffsets converts messages to librdkafka TopicPartitions, deduplicating
// to the max offset per (topic, partition) and storing offset+1 — committing
// offset N would re-deliver message N on every restart.
func commitOffsets(msgs []*Message) []ckafka.TopicPartition {
	maxOffsets := make(map[TopicPartition]int64, len(msgs))
	for _, m := range msgs {
		tp := TopicPartition{Topic: m.Topic, Partition: m.Partition}
		// The ok-check matters: Kafka offsets start at 0, and 0 > 0 is false.
		if cur, ok := maxOffsets[tp]; !ok || m.Offset > cur {
			maxOffsets[tp] = m.Offset
		}
	}
	out := make([]ckafka.TopicPartition, 0, len(maxOffsets))
	for tp, off := range maxOffsets {
		topic := tp.Topic
		out = append(out, ckafka.TopicPartition{
			Topic: &topic, Partition: tp.Partition, Offset: ckafka.Offset(off + 1),
		})
	}
	return out
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

// blockBackoff is the escalating-with-cap wait before a blocked partition is
// resumed: InitialInterval doubling per consecutive block of the same message,
// capped at MaxInterval, reset when the message finally parks.
func blockBackoff(blocks int, r RetryConfig) time.Duration {
	initial, maxInterval := r.InitialInterval, r.MaxInterval
	d := initial
	for i := 1; i < blocks && d < maxInterval; i++ {
		d *= 2
	}
	if d > maxInterval {
		d = maxInterval
	}
	return d
}

func getOffsetReset(fromBeginning bool) string {
	if fromBeginning {
		return "earliest"
	}
	return "latest"
}

// appendTime formats time in RFC3339 format without allocating a string
// Uses time.AppendFormat for zero-allocation formatting
func appendTime(buf []byte, t time.Time) []byte {
	return t.AppendFormat(buf, time.RFC3339)
}
