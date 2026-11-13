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

// Consumer is a handler-based Kafka consumer with at-least-once delivery.
// Create one with NewConsumer, register exactly one handler (OnMessage,
// OnBatch or OnGroupedBatch), run it with Start and release it with Close.
//
// An offset is stored only when a message is parked: the handler succeeded, the
// message was confirmed on the DLQ, the error handler claimed it, or
// SkipOnMaxRetries dropped it. A message that cannot be parked blocks its
// partition (paused, retried with an escalating wait) instead of being skipped.
// All handlers are called from the single goroutine running Start. Pause,
// Resume, Metrics and Commit may be called from any goroutine; the On*
// registrations are not synchronised and must happen before Start.
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
	running      atomic.Bool // true while Start is active
	paused       atomic.Bool // true while Pause is in effect
	pauseApplied atomic.Bool // pause/resume actually applied to librdkafka
	closed       atomic.Bool // true once closed
	handleClosed atomic.Bool // set once the librdkafka handle is about to be destroyed

	// Lifecycle / batch flush
	done       chan struct{} // closed by Close; stops helper goroutines
	loopExited chan struct{} // closed when the main consume loop returns
	flushCh    chan struct{} // cap 1: batch-flush signal consumed in the main loop

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

// NewConsumer creates a Consumer from the given options on top of the defaults
// documented on each ConsumerWith* option. It validates the configuration and
// returns ErrBrokersRequired, ErrGroupIDRequired, ErrTopicsRequired,
// ErrDLQTopicRequired, ErrInvalidBatchSize or ErrInvalidBatchTimeout for the
// corresponding problem, or an error when the librdkafka configuration is
// rejected (including a session timeout not below max.poll.interval.ms). It
// does not connect or subscribe; that happens in Start.
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

	c := &Consumer{
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
		c.tracer = newTracingService(config.Tracing)
	}

	// Initialize idempotency store if configured
	if config.IdempotencyKey != nil {
		c.idempotencyStore = newIdempotencyStore(config.IdempotencyTTL)
	}

	// Initialize DLQ service if configured
	if config.DLQ != nil {
		c.dlqService, err = newDLQService(config.dlqConn(), config.DLQ, c.metrics)
		if err != nil {
			_ = consumer.Close()
			return nil, fmt.Errorf("failed to create DLQ service: %w", err)
		}
	}

	return c, nil
}

// OnMessage registers a handler for single messages, called once per message.
// Only one handler is active: a later OnMessage, OnBatch or OnGroupedBatch call
// replaces the earlier one. Register before Start; Start returns ErrNoHandler
// otherwise. When the handler returns an error it is retried as described on
// RetryConfig, then offered to the error handler and the DLQ, and finally
// blocks its partition.
func (c *Consumer) OnMessage(handler MessageHandler) {
	c.messageHandler, c.batchHandler, c.groupedBatchHandler = handler, nil, nil
}

// OnBatch registers a handler for batches of messages and turns batch mode on.
// A batch is flushed when it reaches ConsumerWithBatchSize messages, after
// ConsumerWithBatchTimeout, and in Close. Only one handler is active: a later
// OnMessage, OnBatch or OnGroupedBatch call replaces the earlier one. Register
// before Start; Start returns ErrNoHandler otherwise. The handler runs once per
// delivery, without in-place retries; if it returns an error the whole batch
// is treated as failed and, unless the error handler or the DLQ takes every
// message, the batch is redelivered, so handlers must tolerate duplicates (see
// ConsumerWithIdempotencyKey).
func (c *Consumer) OnBatch(handler BatchHandler) {
	c.messageHandler, c.batchHandler, c.groupedBatchHandler = nil, handler, nil
}

// OnGroupedBatch registers a handler for batches grouped by message key and
// turns batch mode on. Batching, replacement, registration and failure rules are
// those of OnBatch; the messages of a batch are grouped by string(Key) in
// first-seen order, keeping the order within each group.
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

// Start subscribes to the configured topics and consumes messages until it is
// stopped; it blocks. It returns the context's error when ctx is cancelled, nil
// when Close was called, and an error wrapping the client error when
// librdkafka reports a fatal one. It returns ErrNoHandler without a registered
// handler, ErrConsumerClosed after Close, ErrConsumerRunning when Start is
// already running, and an error when the subscription fails.
//
// Whichever way Start returns, call Close afterwards, including after a fatal
// error: Start does not release the client or flush the last partial batch.
func (c *Consumer) Start(ctx context.Context) error {
	if c.messageHandler == nil && c.batchHandler == nil && c.groupedBatchHandler == nil {
		return ErrNoHandler
	}
	if c.closed.Load() {
		return ErrConsumerClosed
	}
	// Use atomic CAS to ensure only one Start can succeed
	if !c.running.CompareAndSwap(false, true) {
		return ErrConsumerRunning
	}
	// Publish this run's exit signal only after winning the CAS: a failed
	// Start must never replace the channel the already-running loop closes,
	// or Close would wait on an orphan (guaranteed 5s stall, then a close
	// racing an in-flight poll).
	c.mu.Lock()
	c.loopExited = make(chan struct{})
	c.mu.Unlock()
	defer c.running.Store(false) // every exit path releases
	defer close(c.loopExited)    // signals Close that the loop is gone

	// Subscribe to topics; the rebalance callback is always installed so the
	// library's own commit/assign logic runs even without a user callback
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
			c.reconcilePause() // real pause, reconciled every pass
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

// Close stops the consumer and releases its resources. It signals Start to
// return and waits up to 5 seconds for the consume loop to leave, then (in
// batch mode) flushes the batch still in memory, closes the DLQ producer and
// the idempotency store, and closes the client.
//
// ctx bounds only that final batch flush: it is the context passed to the batch
// handler and the DLQ for the remaining messages, so a cancelled ctx cuts the
// flush short. Cancel the Start context before calling Close when handlers or
// retries can run long: without a cancel, Close falls back to the bounded 5s
// wait and may then close the handle while a long retry is still in flight.
//
// Close is safe to call more than once; later calls return nil and do nothing.
func (c *Consumer) Close(ctx context.Context) error {
	wasRunning := c.running.Load()
	// Use atomic CAS to ensure only one Close can succeed
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.running.Store(false)

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

	c.handleClosed.Store(true)
	return c.consumer.Close()
}

// Pause stops fetching new messages from the assigned partitions. The change is
// applied by the poll loop, which keeps polling, so the consumer stays in its
// group, a partial batch is still flushed on its timeout, and blocked
// partitions are not resumed until Resume. It also survives rebalances. Calling
// it before Start takes effect once partitions are assigned.
func (c *Consumer) Pause() {
	c.paused.Store(true)
}

// Resume undoes Pause: fetching restarts on the assigned partitions and
// blocked partitions whose wait has elapsed resume.
func (c *Consumer) Resume() {
	c.paused.Store(false)
}

// reconcilePause applies the user-visible Pause/Resume to the current
// assignment. librdkafka resets pause state on every rebalance, so this
// must run continuously rather than once — and Assignment() is empty before
// the first rebalance completes, which the len check absorbs.
func (c *Consumer) reconcilePause() {
	want := c.paused.Load()
	applied := c.pauseApplied.Load()
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
		c.pauseApplied.Store(want)
	}
}

// Metrics returns a snapshot of the consumer's handler-failure and DLQ counters
// and of its blocked partitions. The returned value is a copy that does not
// change later; call Metrics again for fresh numbers. See Metrics (the type) for
// what each counter counts.
func (c *Consumer) Metrics() *Metrics {
	return c.metrics.snapshot()
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
	for _, msg := range batch {
		c.metrics.incHandlerFailures(msg.Topic)
	}
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
// main loop's own size-triggered flush. The non-blocking send collapses
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
// library's own commit-before-revoke runs. Under a cooperative assignor,
// Assign/Unassign would replace the whole assignment mid-rebalance — an
// API violation — so the protocol is checked and incremental variants used.
func (c *Consumer) rebalanceCallback() ckafka.RebalanceCb {
	return func(consumer *ckafka.Consumer, event ckafka.Event) error {
		cooperative := consumer.GetRebalanceProtocol() == "COOPERATIVE"
		switch e := event.(type) {
		case ckafka.AssignedPartitions:
			c.logger.Info("partitions assigned", "count", len(e.Partitions))
			c.pauseApplied.Store(false) // rebalance resets librdkafka pause state

			// Call user's callback
			if c.config.RebalanceCallback != nil {
				if err := c.config.RebalanceCallback(RebalanceEvent{Type: RebalanceAssigned, Partitions: convertPartitions(e.Partitions)}); err != nil {
					c.logger.Error("rebalance callback error on assign", "error", err)
					return err
				}
			}

			if cooperative {
				return consumer.IncrementalAssign(e.Partitions)
			}
			return consumer.Assign(e.Partitions)

		case ckafka.RevokedPartitions:
			c.logger.Info("partitions revoked", "count", len(e.Partitions))

			// Call user's callback
			if c.config.RebalanceCallback != nil {
				if err := c.config.RebalanceCallback(RebalanceEvent{Type: RebalanceRevoked, Partitions: convertPartitions(e.Partitions)}); err != nil {
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

			c.dropBlockedFor(e.Partitions)

			if cooperative {
				return consumer.IncrementalUnassign(e.Partitions)
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

// Commit commits offsets synchronously. With messages, it stores and commits
// the offset after the highest-offset message per topic and partition (the
// position of the next message to read) and returns the first error from the
// store or the commit. Messages must come from this consumer. With no
// arguments it commits every offset stored so far, which are the offsets of
// messages already parked, and returns nil when there is nothing to commit.
//
// With ConsumerWithAutoCommit(false) the handler must call Commit itself,
// otherwise nothing is committed. With auto commit on it is optional, for
// committing sooner than the auto commit interval. It returns ErrConsumerClosed
// once the consumer has been closed.
func (c *Consumer) Commit(msgs ...*Message) error {
	if c.handleClosed.Load() {
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
