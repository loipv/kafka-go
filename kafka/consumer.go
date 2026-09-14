package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Consumer is a handler-based Kafka consumer.
type Consumer struct {
	consumer *ckafka.Consumer
	config   *ConsumerConfig
	tracer   *TracingService
	logger   Logger

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

	// Lifecycle / batch flush
	done       chan struct{} // closed by Close; stops helper goroutines
	loopExited chan struct{} // closed when the main consume loop returns
	flushCh    chan struct{} // cap 1: batch-flush signal consumed in the main loop (#18)

	// Batch processing
	batchMu sync.Mutex
	batch   []*Message

	// Idempotency
	idempotencyStore *IdempotencyStore

	// DLQ
	dlqService *DLQService
	metrics    *DLQMetricsCollector

	// Circuit breaker for DLQ
	circuitBreakers map[string]*CircuitBreaker
	cbMu            sync.RWMutex

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
		return nil, fmt.Errorf("brokers are required")
	}

	if config.GroupID == "" {
		return nil, fmt.Errorf("group ID is required")
	}

	if len(config.Topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
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
		logger = NewDefaultLogger(config.LogLevel)
	}

	kc := &Consumer{
		consumer:        consumer,
		config:          config,
		logger:          logger,
		done:            make(chan struct{}),
		flushCh:         make(chan struct{}, 1),
		batch:           make([]*Message, 0, config.BatchSize),
		circuitBreakers: make(map[string]*CircuitBreaker),
		metrics:         NewDLQMetricsCollector(),
		blocked:         make(map[TopicPartition]blockState),
	}

	// Initialize tracing if enabled
	if config.Tracing != nil && config.Tracing.Enabled {
		kc.tracer = NewTracingService(config.Tracing)
	}

	// Initialize idempotency store if configured
	if config.IdempotencyKey != nil {
		kc.idempotencyStore = NewIdempotencyStore(config.IdempotencyTTL)
	}

	// Initialize DLQ service if configured
	if config.DLQ != nil {
		kc.dlqService, err = newDLQService(config.conn(), config.DLQ, kc.metrics, logger)
		if err != nil {
			consumer.Close()
			return nil, fmt.Errorf("failed to create DLQ service: %w", err)
		}

		// Initialize circuit breaker for DLQ
		if config.DLQ.CircuitBreaker != nil {
			kc.circuitBreakers[config.DLQ.Topic] = NewCircuitBreaker(config.DLQ.CircuitBreaker)
		}
	}

	return kc, nil
}

// OnMessage registers a handler for single messages
func (c *Consumer) OnMessage(handler MessageHandler) {
	c.messageHandler = handler
}

// OnBatch registers a handler for batch messages
func (c *Consumer) OnBatch(handler BatchHandler) {
	c.batchHandler = handler
}

// OnGroupedBatch registers a handler for key-grouped batches
func (c *Consumer) OnGroupedBatch(handler GroupedBatchHandler) {
	c.groupedBatchHandler = handler
}

// Start starts consuming messages (blocking)
func (c *Consumer) Start(ctx context.Context) error {
	if c.messageHandler == nil && c.batchHandler == nil && c.groupedBatchHandler == nil {
		return fmt.Errorf("no handler registered; call OnMessage/OnBatch/OnGroupedBatch before Start") // #17
	}
	// Publish a fresh exit signal per Start (before the CAS, so Close can
	// never observe running==1 without a published channel): restarting a
	// consumer whose previous loop already exited must not double-close.
	c.mu.Lock()
	c.loopExited = make(chan struct{})
	c.mu.Unlock()
	// Use atomic CAS to ensure only one Start can succeed
	if !atomic.CompareAndSwapInt32(&c.running, 0, 1) {
		return fmt.Errorf("consumer is already running")
	}
	defer atomic.StoreInt32(&c.running, 0) // #8: every exit path releases
	defer close(c.loopExited)              // signals Close that the loop is gone

	// Subscribe to topics; the rebalance callback is always installed so the
	// library's own commit/assign logic runs even without a user callback (#31)
	if err := c.consumer.SubscribeTopics(c.config.Topics, c.rebalanceCallback()); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	// Start batch flush ticker if batch processing is enabled
	if c.config.BatchProcessing {
		go c.batchFlushLoop(ctx)
	}

	// Start DLQ retry consumer if configured
	if c.config.DLQRetry != nil && c.config.DLQRetry.Enabled {
		go c.startDLQRetryConsumer(ctx)
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
				if ke, ok := err.(ckafka.Error); ok && ke.Code() == ckafka.ErrTimedOut {
					continue
				}
				// Log other errors but continue
				c.logger.Warn("Error reading message: %v", err)
				continue
			}

			// Convert to our Message type
			message := c.convertMessage(msg)

			// Process message
			if c.config.BatchProcessing {
				c.addToBatch(ctx, message)
			} else {
				c.deliver(ctx, message)
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close(ctx context.Context) error {
	wasRunning := atomic.LoadInt32(&c.running) == 1
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	atomic.StoreInt32(&c.running, 0)

	// Stop helper goroutines (batch flush ticker, DLQ retry consumer)
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
	if c.config.BatchProcessing {
		c.processBatch(ctx)
	}

	// Close DLQ service
	if c.dlqService != nil {
		c.dlqService.Close()
	}

	// Close idempotency store
	if c.idempotencyStore != nil {
		c.idempotencyStore.Close()
	}

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
	return c.metrics.GetMetrics()
}

// CircuitState returns circuit breaker state
func (c *Consumer) CircuitState(dlqTopic string) CircuitState {
	c.cbMu.RLock()
	defer c.cbMu.RUnlock()
	if cb, ok := c.circuitBreakers[dlqTopic]; ok {
		return cb.State()
	}
	return CircuitClosed
}

// ResetCircuit resets the circuit breaker
func (c *Consumer) ResetCircuit(dlqTopic string) {
	c.cbMu.Lock()
	defer c.cbMu.Unlock()
	if cb, ok := c.circuitBreakers[dlqTopic]; ok {
		cb.Reset()
	}
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

	return &Message{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Timestamp: msg.Timestamp,
		Topic:     *msg.TopicPartition.Topic,
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
		if idempotencyKey != "" && c.idempotencyStore.IsDuplicate(idempotencyKey) {
			c.logger.Debug("Skipping duplicate message with key: %s", idempotencyKey)
			return outcomeParked
		}
	}

	// Start tracing span
	var endSpan func(error)
	if c.tracer != nil {
		ctx, endSpan = c.tracer.StartConsumerSpan(ctx, c.config.GroupID, msg)
	}

	// Execute handler with retry
	err, attempts := c.executeWithRetry(ctx, msg)

	if err == nil {
		if endSpan != nil {
			endSpan(nil)
		}
		// Only mark as processed on SUCCESS — the idempotency-key store is
		// skipped on error; the offset is not stored either (the partition
		// blocks), so the message will be redelivered.
		if c.idempotencyStore != nil && idempotencyKey != "" {
			c.idempotencyStore.Add(idempotencyKey)
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

// invokeHandler dispatches a single message to whichever handler the user
// registered, wrapping it into a batch or grouped batch as needed. Start
// rejects a handler-less consumer, so exactly one branch fires.
func (c *Consumer) invokeHandler(ctx context.Context, msg *Message) error {
	switch {
	case c.messageHandler != nil:
		return c.messageHandler(ctx, msg)
	case c.batchHandler != nil:
		return c.batchHandler(ctx, []*Message{msg})
	case c.groupedBatchHandler != nil:
		return c.groupedBatchHandler(ctx, []GroupedBatch{{Key: string(msg.Key), Messages: []*Message{msg}}})
	}
	return nil
}

// executeWithRetry executes the handler with retry logic. The returned
// attempts count handler invocations (1 = first try, no retry).
func (c *Consumer) executeWithRetry(ctx context.Context, msg *Message) (error, int) {
	maxRetries := DefaultRetryMaxRetries
	initialInterval := DefaultRetryInitialInterval
	multiplier := DefaultRetryMultiplier
	maxInterval := DefaultRetryMaxInterval
	skipOnMaxRetries := false

	if c.config.Retry != nil {
		if c.config.Retry.MaxRetries > 0 {
			maxRetries = c.config.Retry.MaxRetries
		}
		if c.config.Retry.InitialInterval > 0 {
			initialInterval = c.config.Retry.InitialInterval
		}
		if c.config.Retry.Multiplier > 0 {
			multiplier = c.config.Retry.Multiplier
		}
		if c.config.Retry.MaxInterval > 0 {
			maxInterval = c.config.Retry.MaxInterval
		}
		skipOnMaxRetries = c.config.Retry.SkipOnMaxRetries
	}

	var lastErr error
	delay := initialInterval

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := c.invokeHandler(ctx, msg)
		if err == nil {
			return nil, attempt + 1
		}

		lastErr = err
		c.metrics.IncrementHandlerRetries(msg.Topic)

		if attempt < maxRetries {
			c.logger.Debug("Retrying message (attempt %d/%d): %v", attempt+1, maxRetries, err)
			select {
			case <-ctx.Done():
				return ctx.Err(), attempt + 1
			case <-time.After(delay):
				delay = time.Duration(float64(delay) * multiplier)
				if delay > maxInterval {
					delay = maxInterval // retryBudget assumes capped sleeps — keep them capped
				}
			}
		}
	}

	if skipOnMaxRetries {
		c.logger.Warn("Max retries exceeded — skipping message: %v", lastErr)
		return fmt.Errorf("%w: %v", ErrSkippedOnMaxRetries, lastErr), maxRetries + 1
	}
	return lastErr, maxRetries + 1
}

// handleError parks or blocks a failed message: the error handler can claim
// ownership (nil return = parked), then the DLQ; with neither, or with the
// circuit breaker open, the message is unparkable and its partition blocks.
func (c *Consumer) handleError(ctx context.Context, err error, msg *Message, attempts int) outcome {
	if c.config.ErrorHandler != nil {
		if handled := c.config.ErrorHandler(ctx, msg, err); handled == nil {
			return outcomeParked // user took ownership
		}
	}
	if c.dlqService != nil {
		c.cbMu.RLock()
		cb := c.circuitBreakers[c.config.DLQ.Topic]
		c.cbMu.RUnlock()

		if cb != nil && cb.IsOpen() {
			c.logger.Warn("circuit breaker open — message unparkable (topic=%s): %v", msg.Topic, err)
			return outcomeBlocked
		}
		if dlqErr := c.dlqService.produceToDLQ(ctx, msg, err, attempts); dlqErr != nil {
			c.logger.Error("dlq produce failed: %v", dlqErr)
			if cb != nil {
				cb.RecordFailure()
			}
			return outcomeBlocked
		}
		if cb != nil {
			cb.RecordSuccess()
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

	// Start tracing span
	var endSpan func(error)
	if c.tracer != nil && len(batch) > 0 {
		ctx, endSpan = c.tracer.StartBatchConsumerSpan(ctx, c.config.GroupID, batch)
	}

	// Process based on configuration
	var processingErr error
	blocked := false
	if c.config.GroupByKey && c.groupedBatchHandler != nil {
		groups := c.groupByKey(batch)
		processingErr = c.groupedBatchHandler(ctx, groups)
	} else if c.batchHandler != nil {
		processingErr = c.batchHandler(ctx, batch)
	} else if c.messageHandler != nil {
		// Fall back to processing messages individually
		for _, msg := range batch {
			if c.processMessage(ctx, msg) == outcomeBlocked {
				blocked = true
			}
		}
	}

	// End span with error if any
	if endSpan != nil {
		endSpan(processingErr)
	}

	// OnMessage batch error
	if processingErr != nil {
		blocked = c.handleBatchError(ctx, processingErr, batch) || blocked
	}

	if blocked {
		c.blockMessages(ctx, batch)
		return outcomeBlocked
	}
	for _, msg := range batch {
		c.clearBlockedOnPark(msg)
	}
	c.storeOffsets(batch)
	return outcomeParked
}

// storeOffsets stores (does not commit) offsets for the given messages.
func (c *Consumer) storeOffsets(msgs []*Message) {
	if len(msgs) == 0 {
		return
	}
	if _, err := c.consumer.StoreOffsets(commitOffsets(msgs)); err != nil {
		c.logger.Warn("store offsets failed: %v", err)
	}
}

// groupByKey groups messages by key
// Optimized with pre-allocation based on estimated group count
func (c *Consumer) groupByKey(msgs []*Message) []GroupedBatch {
	if len(msgs) == 0 {
		return nil
	}

	// Estimate number of unique keys (assume ~10 messages per key on average)
	estimatedGroups := len(msgs) / 10
	if estimatedGroups < 1 {
		estimatedGroups = 1
	}
	if estimatedGroups > len(msgs) {
		estimatedGroups = len(msgs)
	}

	groups := make(map[string][]*Message, estimatedGroups)
	order := make([]string, 0, estimatedGroups)

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

// handleBatchError routes a batch-level error through the per-message
// handleError path (each outcome decides). It reports whether any message
// ended up unparkable. The batch handler runs once — no executeWithRetry —
// so attempts is 1.
func (c *Consumer) handleBatchError(ctx context.Context, err error, batch []*Message) bool {
	blocked := false
	for _, msg := range batch {
		if c.handleError(ctx, err, msg, 1) == outcomeBlocked {
			blocked = true
		}
	}
	return blocked
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

// startDLQRetryConsumer starts the DLQ retry consumer
func (c *Consumer) startDLQRetryConsumer(ctx context.Context) {
	if c.dlqService == nil || c.config.DLQRetry == nil {
		return
	}

	retryConfig := c.config.DLQRetry
	dlqTopic := c.config.DLQ.Topic

	// Create DLQ consumer
	groupID := retryConfig.GroupID
	if groupID == "" {
		groupID = fmt.Sprintf("%s-retry-consumer", dlqTopic)
	}

	cm := c.config.conn().configMap()
	cm["group.id"] = groupID
	cm["auto.offset.reset"] = getOffsetReset(retryConfig.FromBeginning)
	cm["enable.auto.commit"] = false // Task 10 makes commit-after-success explicit

	dlqConsumer, err := ckafka.NewConsumer(&cm)
	if err != nil {
		c.logger.Error("Failed to create DLQ retry consumer: %v", err)
		return
	}
	defer dlqConsumer.Close()

	if err := dlqConsumer.Subscribe(dlqTopic, nil); err != nil {
		c.logger.Error("Failed to subscribe to DLQ topic: %v", err)
		return
	}

	c.logger.Info("DLQ retry consumer started for topic: %s", dlqTopic)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := dlqConsumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if kafkaErr, ok := err.(ckafka.Error); ok && kafkaErr.Code() == ckafka.ErrTimedOut {
					continue
				}
				c.logger.Warn("DLQ consumer error: %v", err)
				continue
			}

			// Convert and process
			message := c.convertMessage(msg)
			c.processDLQRetry(ctx, message, retryConfig)
		}
	}
}

// processDLQRetry processes a DLQ retry message
func (c *Consumer) processDLQRetry(ctx context.Context, msg *Message, config *DLQRetryConfig) {
	c.metrics.IncrementReprocessAttempts(msg.Topic)

	// Get retry count from headers - use strconv for better performance
	retryCount := 0
	if countBytes, ok := msg.Headers["x-dlq-reprocess-count"]; ok {
		retryCount, _ = strconv.Atoi(string(countBytes))
	}

	// Check if max retries exceeded
	if retryCount >= config.MaxRetries {
		c.logger.Warn("DLQ max retries exceeded for message, sending to final DLQ")
		if config.FinalDLQTopic != "" {
			c.sendToFinalDLQ(ctx, msg, config.FinalDLQTopic)
		}
		return
	}

	// Calculate delay with backoff
	delay := config.Delay
	for i := 0; i < retryCount; i++ {
		delay = time.Duration(float64(delay) * config.BackoffMultiplier)
	}

	c.logger.Debug("DLQ retry: waiting %v before reprocessing (attempt %d/%d)", delay, retryCount+1, config.MaxRetries)

	// Wait before reprocessing
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}

	// Update retry count - use strconv for better performance
	msg.Headers["x-dlq-reprocess-count"] = []byte(strconv.Itoa(retryCount + 1))
	msg.Headers["x-dlq-reprocess-timestamp"] = appendTime(nil, time.Now())

	// Reprocess message
	err := c.messageHandler(ctx, msg)
	if err != nil {
		c.metrics.IncrementReprocessFailures()
		c.logger.Warn("DLQ reprocess failed: %v", err)
		// Will be picked up again from DLQ
	} else {
		c.metrics.IncrementReprocessSuccesses()
		c.logger.Info("DLQ message reprocessed successfully")
	}
}

// sendToFinalDLQ sends message to final DLQ
func (c *Consumer) sendToFinalDLQ(ctx context.Context, msg *Message, finalTopic string) {
	msg.Headers["x-final-dlq-reason"] = []byte("max retries exceeded")
	msg.Headers["x-final-dlq-timestamp"] = appendTime(nil, time.Now())

	if c.dlqService != nil {
		if err := c.dlqService.produceToTopic(ctx, finalTopic, msg); err != nil {
			c.logger.Error("Failed to send to final DLQ: %v", err)
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
			c.logger.Info("Partitions assigned: %d", len(e.Partitions))
			atomic.StoreInt32(&c.pauseApplied, 0) // rebalance resets librdkafka pause state

			// Call user's callback
			if c.config.RebalanceCallback != nil {
				if err := c.config.RebalanceCallback(RebalanceEvent{Type: "assigned", Partitions: convertPartitions(e.Partitions)}); err != nil {
					c.logger.Error("Rebalance callback error on assign: %v", err)
					return err
				}
			}

			if cooperative {
				return consumer.IncrementalAssign(e.Partitions) // #32
			}
			return consumer.Assign(e.Partitions)

		case ckafka.RevokedPartitions:
			c.logger.Info("Partitions revoked: %d", len(e.Partitions))

			// Call user's callback
			if c.config.RebalanceCallback != nil {
				if err := c.config.RebalanceCallback(RebalanceEvent{Type: "revoked", Partitions: convertPartitions(e.Partitions)}); err != nil {
					c.logger.Error("Rebalance callback error on revoke: %v", err)
					return err
				}
			}

			// Commit any pending offsets before unassigning (if auto-commit is disabled)
			if !c.config.AutoCommit {
				if _, err := consumer.Commit(); err != nil {
					// Ignore "no offset stored" errors
					if ke, ok := err.(ckafka.Error); !ok || ke.Code() != ckafka.ErrNoOffset {
						c.logger.Warn("Failed to commit offsets during rebalance: %v", err)
					}
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

// ErrSkippedOnMaxRetries wraps the handler error when SkipOnMaxRetries gave up
// on a message; processMessage parks (advances past) such messages after a
// best-effort DLQ produce and ErrorHandler notify.
var ErrSkippedOnMaxRetries = errors.New("message skipped after max retries")

// Commit stores and commits offsets for the given messages. With no
// arguments it commits everything stored so far. Offsets are committed at
// max(msg.Offset)+1 per (topic, partition).
func (c *Consumer) Commit(msgs ...*Message) error {
	if len(msgs) == 0 {
		_, err := c.consumer.Commit()
		return err
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
func (c *Consumer) blockMessages(ctx context.Context, msgs []*Message) {
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
		bs.retryAt = now.Add(blockBackoff(bs.blocks, c.config.Retry))
		c.blocked[tp] = bs
		topic := tp.Topic
		paused = append(paused, ckafka.TopicPartition{Topic: &topic, Partition: tp.Partition, Offset: ckafka.Offset(bs.offset)})
		c.logger.Warn("partition blocked — message unparkable (topic=%s partition=%d offset=%d blocks=%d retryIn=%v)",
			tp.Topic, tp.Partition, bs.offset, bs.blocks, blockBackoff(bs.blocks, c.config.Retry))
	}
	c.blockMu.Unlock()
	c.metrics.SetBlocked(c.blockedSnapshot())
	if err := c.consumer.Pause(paused); err != nil {
		c.logger.Error("pause failed for blocked partition (next pass re-attempts): %v", err)
	}
	for _, tp := range paused {
		if err := c.consumer.Seek(tp, 5000); err != nil {
			t := ""
			if tp.Topic != nil {
				t = *tp.Topic
			}
			c.logger.Error("seek failed for blocked partition (topic=%s partition=%d offset=%d): %v",
				t, tp.Partition, tp.Offset, err)
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
			c.logger.Error("resume failed for blocked partition (next pass re-attempts): %v", err)
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
		c.metrics.SetBlocked(c.blockedSnapshot())
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
	c.metrics.SetBlocked(c.blockedSnapshot())
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
	max := make(map[TopicPartition]int64, len(msgs))
	for _, m := range msgs {
		tp := TopicPartition{Topic: m.Topic, Partition: m.Partition}
		// The ok-check matters: Kafka offsets start at 0, and 0 > 0 is false.
		if cur, ok := max[tp]; !ok || m.Offset > cur {
			max[tp] = m.Offset
		}
	}
	out := make([]ckafka.TopicPartition, 0, len(max))
	for tp, off := range max {
		topic := tp.Topic
		out = append(out, ckafka.TopicPartition{
			Topic: &topic, Partition: tp.Partition, Offset: ckafka.Offset(off + 1),
		})
	}
	return out
}

// retryBudget returns the worst-case wall time of one executeWithRetry cycle
// (all sleeps, no handler time). Feeds the max.poll.interval.ms floor.
func retryBudget(r *RetryConfig) time.Duration {
	maxRetries, initial, multiplier, maxInterval := DefaultRetryMaxRetries, DefaultRetryInitialInterval, DefaultRetryMultiplier, DefaultRetryMaxInterval
	if r != nil {
		if r.MaxRetries > 0 {
			maxRetries = r.MaxRetries
		}
		if r.InitialInterval > 0 {
			initial = r.InitialInterval
		}
		if r.Multiplier > 0 {
			multiplier = r.Multiplier
		}
		if r.MaxInterval > 0 {
			maxInterval = r.MaxInterval
		}
	}
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
func blockBackoff(blocks int, r *RetryConfig) time.Duration {
	initial, maxInterval := DefaultRetryInitialInterval, DefaultRetryMaxInterval
	if r != nil {
		if r.InitialInterval > 0 {
			initial = r.InitialInterval
		}
		if r.MaxInterval > 0 {
			maxInterval = r.MaxInterval
		}
	}
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
