package kafka

import (
	"context"
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
	done    chan struct{} // closed by Close; stops helper goroutines
	flushCh chan struct{} // cap 1: batch-flush signal consumed in the main loop (#18)

	// Batch processing
	batchMu sync.Mutex
	batch   []*Message

	// Idempotency
	idempotencyStore *IdempotencyStore

	// DLQ
	dlqService *DLQService
	dlqMetrics *DLQMetricsCollector

	// Circuit breaker for DLQ
	circuitBreakers map[string]*CircuitBreaker
	cbMu            sync.RWMutex
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
	configMap := buildConsumerConfig(config)

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
		dlqMetrics:      NewDLQMetricsCollector(),
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
		kc.dlqService, err = newDLQService(config.conn(), config.DLQ, kc.dlqMetrics, logger)
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
	// Use atomic CAS to ensure only one Start can succeed
	if !atomic.CompareAndSwapInt32(&c.running, 0, 1) {
		return fmt.Errorf("consumer is already running")
	}
	defer atomic.StoreInt32(&c.running, 0) // #8: every exit path releases

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
		case <-c.flushCh:
			c.processBatch(ctx)
		default:
			c.reconcilePause() // #12: real pause, reconciled every pass

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
				c.processMessage(ctx, message) // Task 9 renames to deliver
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close(ctx context.Context) error {
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	atomic.StoreInt32(&c.running, 0)

	// Stop helper goroutines (batch flush ticker, DLQ retry consumer)
	close(c.done)

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
	return c.dlqMetrics.GetMetrics()
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

// processMessage processes a single message
func (c *Consumer) processMessage(ctx context.Context, msg *Message) {
	// Check idempotency BEFORE processing
	var idempotencyKey string
	if c.idempotencyStore != nil && c.config.IdempotencyKey != nil {
		idempotencyKey = c.config.IdempotencyKey(msg)
		if idempotencyKey != "" && c.idempotencyStore.IsDuplicate(idempotencyKey) {
			c.logger.Debug("Skipping duplicate message with key: %s", idempotencyKey)
			return // Skip duplicate
		}
	}

	// Start tracing span
	var endSpan func(error)
	if c.tracer != nil {
		ctx, endSpan = c.tracer.StartConsumerSpan(ctx, c.config.GroupID, msg)
	}

	// Execute handler with retry
	err := c.executeWithRetry(ctx, msg)

	// OnMessage result
	if err != nil {
		if endSpan != nil {
			endSpan(err)
		}
		c.handleError(ctx, err, msg)
		// DON'T mark as processed if error - allow reprocessing from DLQ
		return
	}

	// End span successfully
	if endSpan != nil {
		endSpan(nil)
	}

	// Only mark as processed on SUCCESS
	if c.idempotencyStore != nil && idempotencyKey != "" {
		c.idempotencyStore.Add(idempotencyKey)
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

// executeWithRetry executes the handler with retry logic
func (c *Consumer) executeWithRetry(ctx context.Context, msg *Message) error {
	maxRetries := DefaultRetryMaxRetries
	initialInterval := DefaultRetryInitialInterval
	multiplier := DefaultRetryMultiplier
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
		skipOnMaxRetries = c.config.Retry.SkipOnMaxRetries
	}

	var lastErr error
	delay := initialInterval

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := c.invokeHandler(ctx, msg)
		if err == nil {
			return nil
		}

		lastErr = err
		c.dlqMetrics.IncrementHandlerRetries(msg.Topic)

		if attempt < maxRetries {
			c.logger.Debug("Retrying message (attempt %d/%d): %v", attempt+1, maxRetries, err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				delay = time.Duration(float64(delay) * multiplier)
			}
		}
	}

	if skipOnMaxRetries {
		c.logger.Warn("Max retries exceeded for message, skipping: %v", lastErr)
		return nil
	}

	return lastErr
}

// handleError handles processing errors
func (c *Consumer) handleError(ctx context.Context, err error, msg *Message) {
	// Call error handler if set
	if c.config.ErrorHandler != nil {
		c.config.ErrorHandler(err, msg)
	}

	// Produce to DLQ if configured
	if c.dlqService != nil {
		// Check circuit breaker
		c.cbMu.RLock()
		cb := c.circuitBreakers[c.config.DLQ.Topic]
		c.cbMu.RUnlock()

		if cb != nil && cb.IsOpen() {
			c.logger.Warn("Circuit breaker open, cannot send to DLQ: %s", c.config.DLQ.Topic)
			return
		}

		dlqErr := c.dlqService.produceToDLQ(ctx, msg, err)
		if dlqErr != nil {
			c.logger.Error("Failed to send to DLQ: %v", dlqErr)
			if cb != nil {
				cb.RecordFailure()
			}
		} else {
			c.logger.Debug("Message sent to DLQ: %s", c.config.DLQ.Topic)
			if cb != nil {
				cb.RecordSuccess()
			}
		}
	}
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

// processBatch processes the current batch
func (c *Consumer) processBatch(ctx context.Context) {
	c.batchMu.Lock()
	if len(c.batch) == 0 {
		c.batchMu.Unlock()
		return
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
	if c.config.GroupByKey && c.groupedBatchHandler != nil {
		groups := c.groupByKey(batch)
		processingErr = c.groupedBatchHandler(ctx, groups)
	} else if c.batchHandler != nil {
		processingErr = c.batchHandler(ctx, batch)
	} else if c.messageHandler != nil {
		// Fall back to processing messages individually
		for _, msg := range batch {
			c.processMessage(ctx, msg)
		}
	}

	// End span with error if any
	if endSpan != nil {
		endSpan(processingErr)
	}

	// OnMessage batch error
	if processingErr != nil {
		c.handleBatchError(ctx, processingErr, batch)
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

// handleBatchError handles batch processing errors
func (c *Consumer) handleBatchError(ctx context.Context, err error, batch []*Message) {
	for _, msg := range batch {
		c.handleError(ctx, err, msg)
	}
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
	c.dlqMetrics.IncrementReprocessAttempts(msg.Topic)

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
		c.dlqMetrics.IncrementReprocessFailures()
		c.logger.Warn("DLQ reprocess failed: %v", err)
		// Will be picked up again from DLQ
	} else {
		c.dlqMetrics.IncrementReprocessSuccesses()
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

// dropBlockedFor drops blocked-partition bookkeeping for the given partitions.
// No-op stub; Task 9 implements it.
func (c *Consumer) dropBlockedFor(ps []ckafka.TopicPartition) {}

// Helper functions

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
