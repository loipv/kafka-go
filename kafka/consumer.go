package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Verify KafkaConsumer implements Consumer interface
var _ Consumer = (*KafkaConsumer)(nil)

// KafkaConsumer implements the Consumer interface
type KafkaConsumer struct {
	consumer *kafka.Consumer
	config   *ConsumerConfig
	tracer   *TracingService
	logger   Logger

	// Handlers
	messageHandler      MessageHandler
	batchHandler        BatchHandler
	groupedBatchHandler GroupedBatchHandler

	// State - using atomic for hot path operations
	mu      sync.RWMutex
	running int32 // atomic: 0=stopped, 1=running
	paused  int32 // atomic: 0=running, 1=paused
	closed  int32 // atomic: 0=open, 1=closed

	// Batch processing
	batchMu    sync.Mutex
	batch      []*Message
	batchTimer *time.Timer

	// Back pressure - using atomic to avoid lock contention
	queueSize    int64 // atomic
	backPressure int32 // atomic: 0=normal, 1=pressured
	bpThreshold  int64 // pre-calculated threshold

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
func NewConsumer(opts ...ConsumerOption) (*KafkaConsumer, error) {
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

	// Build kafka config map
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(config.Brokers, ","),
		"group.id":           config.GroupID,
		"auto.offset.reset":  getOffsetReset(config.FromBeginning),
		"enable.auto.commit": config.AutoCommit,
	}

	if config.SessionTimeout > 0 {
		configMap.SetKey("session.timeout.ms", int(config.SessionTimeout.Milliseconds()))
	}

	if config.HeartbeatInterval > 0 {
		configMap.SetKey("heartbeat.interval.ms", int(config.HeartbeatInterval.Milliseconds()))
	}

	if config.AutoCommitInterval > 0 {
		configMap.SetKey("auto.commit.interval.ms", int(config.AutoCommitInterval.Milliseconds()))
	}

	if config.PartitionAssignor != "" {
		configMap.SetKey("partition.assignment.strategy", string(config.PartitionAssignor))
	}

	// SSL/SASL configuration
	if config.SSL {
		configMap.SetKey("security.protocol", "ssl")
	}

	if config.SASL != nil {
		if config.SSL {
			configMap.SetKey("security.protocol", "sasl_ssl")
		} else {
			configMap.SetKey("security.protocol", "sasl_plaintext")
		}
		configMap.SetKey("sasl.mechanism", config.SASL.Mechanism)
		configMap.SetKey("sasl.username", config.SASL.Username)
		configMap.SetKey("sasl.password", config.SASL.Password)
	}

	// Set log level
	configMap.SetKey("log_level", int(config.LogLevel))

	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = NewDefaultLogger(config.LogLevel)
	}

	kc := &KafkaConsumer{
		consumer:        consumer,
		config:          config,
		logger:          logger,
		batch:           make([]*Message, 0, config.BatchSize),
		circuitBreakers: make(map[string]*CircuitBreaker),
		dlqMetrics:      NewDLQMetricsCollector(),
		// Pre-calculate back pressure threshold to avoid repeated calculation
		bpThreshold: int64(config.MaxQueueSize * config.BackPressureThreshold / 100),
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
		kc.dlqService, err = NewDLQService(config.Brokers, config.DLQ, kc.dlqMetrics, logger)
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

// Handle registers a handler for single messages
func (c *KafkaConsumer) Handle(handler MessageHandler) {
	c.messageHandler = handler
}

// HandleBatch registers a handler for batch messages
func (c *KafkaConsumer) HandleBatch(handler BatchHandler) {
	c.batchHandler = handler
}

// HandleGroupedBatch registers a handler for key-grouped batches
func (c *KafkaConsumer) HandleGroupedBatch(handler GroupedBatchHandler) {
	c.groupedBatchHandler = handler
}

// Start starts consuming messages (blocking)
func (c *KafkaConsumer) Start(ctx context.Context) error {
	// Use atomic CAS to ensure only one Start can succeed
	if !atomic.CompareAndSwapInt32(&c.running, 0, 1) {
		return fmt.Errorf("consumer is already running")
	}

	// Subscribe to topics with optional rebalance callback
	rebalanceCb := c.createRebalanceCallback()
	if err := c.consumer.SubscribeTopics(c.config.Topics, rebalanceCb); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	// Start batch timer if batch processing is enabled
	if c.config.BatchProcessing {
		c.batchMu.Lock()
		c.batchTimer = time.NewTimer(c.config.BatchTimeout)
		c.batchMu.Unlock()
		go c.batchTimeoutHandler(ctx)
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
		default:
			// Fast path: check paused state with atomic load (no lock)
			if atomic.LoadInt32(&c.paused) == 1 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Timeout is normal, continue
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				// Log other errors but continue
				c.logger.Warn("Error reading message: %v", err)
				continue
			}

			// Convert to our Message type
			message := c.convertMessage(msg)

			// Check back pressure
			c.checkBackPressure()

			// Process message
			if c.config.BatchProcessing {
				c.addToBatch(ctx, message)
			} else {
				c.processMessage(ctx, message)
			}
		}
	}
}

// Close closes the consumer
func (c *KafkaConsumer) Close(ctx context.Context) error {
	// Use atomic CAS to ensure only one Close can succeed
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	atomic.StoreInt32(&c.running, 0)

	// Process remaining batch
	if c.config.BatchProcessing {
		c.processBatch(ctx)
	}

	// Stop batch timer
	c.batchMu.Lock()
	if c.batchTimer != nil {
		c.batchTimer.Stop()
	}
	c.batchMu.Unlock()

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
func (c *KafkaConsumer) Pause() {
	atomic.StoreInt32(&c.paused, 1)
}

// Resume resumes consumption
func (c *KafkaConsumer) Resume() {
	atomic.StoreInt32(&c.paused, 0)
}

// GetDLQMetrics returns DLQ metrics
func (c *KafkaConsumer) GetDLQMetrics() *DLQMetrics {
	return c.dlqMetrics.GetMetrics()
}

// GetCircuitState returns circuit breaker state
func (c *KafkaConsumer) GetCircuitState(dlqTopic string) CircuitState {
	c.cbMu.RLock()
	defer c.cbMu.RUnlock()
	if cb, ok := c.circuitBreakers[dlqTopic]; ok {
		return cb.State()
	}
	return CircuitClosed
}

// ResetCircuit resets the circuit breaker
func (c *KafkaConsumer) ResetCircuit(dlqTopic string) {
	c.cbMu.Lock()
	defer c.cbMu.Unlock()
	if cb, ok := c.circuitBreakers[dlqTopic]; ok {
		cb.Reset()
	}
}

// convertMessage converts kafka.Message to Message
// Optimized to avoid allocation when there are no headers
func (c *KafkaConsumer) convertMessage(msg *kafka.Message) *Message {
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
func (c *KafkaConsumer) processMessage(ctx context.Context, msg *Message) {
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

	// Handle result
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

// executeWithRetry executes the handler with retry logic
func (c *KafkaConsumer) executeWithRetry(ctx context.Context, msg *Message) error {
	if c.messageHandler == nil {
		return nil
	}

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
		err := c.messageHandler(ctx, msg)
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
func (c *KafkaConsumer) handleError(ctx context.Context, err error, msg *Message) {
	// Call error handler if set
	if c.config.ErrorHandler != nil {
		c.config.ErrorHandler(err, msg)
	}

	// Send to DLQ if configured
	if c.dlqService != nil {
		// Check circuit breaker
		c.cbMu.RLock()
		cb := c.circuitBreakers[c.config.DLQ.Topic]
		c.cbMu.RUnlock()

		if cb != nil && cb.IsOpen() {
			c.logger.Warn("Circuit breaker open, cannot send to DLQ: %s", c.config.DLQ.Topic)
			return
		}

		dlqErr := c.dlqService.SendToDLQ(ctx, msg, err)
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
func (c *KafkaConsumer) addToBatch(ctx context.Context, msg *Message) {
	c.batchMu.Lock()
	c.batch = append(c.batch, msg)
	batchSize := len(c.batch)
	c.batchMu.Unlock()

	atomic.AddInt64(&c.queueSize, 1)

	// Process batch if full
	if batchSize >= c.config.BatchSize {
		c.processBatch(ctx)
	}
}

// processBatch processes the current batch
func (c *KafkaConsumer) processBatch(ctx context.Context) {
	c.batchMu.Lock()
	if len(c.batch) == 0 {
		c.batchMu.Unlock()
		return
	}
	batch := c.batch
	c.batch = make([]*Message, 0, c.config.BatchSize)

	// Reset timer safely while holding the lock
	if c.batchTimer != nil {
		// Stop and drain the timer
		if !c.batchTimer.Stop() {
			select {
			case <-c.batchTimer.C:
			default:
			}
		}
		c.batchTimer.Reset(c.config.BatchTimeout)
	}
	c.batchMu.Unlock()

	atomic.StoreInt64(&c.queueSize, 0)

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

	// Handle batch error
	if processingErr != nil {
		c.handleBatchError(ctx, processingErr, batch)
	}
}

// groupByKey groups messages by key
// Optimized with pre-allocation based on estimated group count
func (c *KafkaConsumer) groupByKey(msgs []*Message) []GroupedBatch {
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
func (c *KafkaConsumer) handleBatchError(ctx context.Context, err error, batch []*Message) {
	for _, msg := range batch {
		c.handleError(ctx, err, msg)
	}
}

// batchTimeoutHandler handles batch timeout
func (c *KafkaConsumer) batchTimeoutHandler(ctx context.Context) {
	for {
		c.batchMu.Lock()
		timer := c.batchTimer
		c.batchMu.Unlock()

		if timer == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			c.processBatch(ctx)
		}
	}
}

// checkBackPressure checks and handles back pressure
// Uses atomic CAS to avoid race conditions and lock contention
func (c *KafkaConsumer) checkBackPressure() {
	queueSize := atomic.LoadInt64(&c.queueSize)

	// Use pre-calculated threshold (set in NewConsumer)
	if queueSize >= c.bpThreshold && atomic.CompareAndSwapInt32(&c.backPressure, 0, 1) {
		c.Pause()
		c.logger.Warn("Back pressure threshold reached (%d/%d), pausing consumer", queueSize, c.bpThreshold)
	} else if queueSize < c.bpThreshold/2 && atomic.CompareAndSwapInt32(&c.backPressure, 1, 0) {
		c.Resume()
		c.logger.Info("Back pressure relieved, resuming consumer")
	}
}

// startDLQRetryConsumer starts the DLQ retry consumer
func (c *KafkaConsumer) startDLQRetryConsumer(ctx context.Context) {
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

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(c.config.Brokers, ","),
		"group.id":           groupID,
		"auto.offset.reset":  getOffsetReset(retryConfig.FromBeginning),
		"enable.auto.commit": true,
	}

	dlqConsumer, err := kafka.NewConsumer(configMap)
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
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
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
func (c *KafkaConsumer) processDLQRetry(ctx context.Context, msg *Message, config *DLQRetryConfig) {
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
func (c *KafkaConsumer) sendToFinalDLQ(ctx context.Context, msg *Message, finalTopic string) {
	msg.Headers["x-final-dlq-reason"] = []byte("max retries exceeded")
	msg.Headers["x-final-dlq-timestamp"] = appendTime(nil, time.Now())

	if c.dlqService != nil {
		if err := c.dlqService.SendToTopic(ctx, finalTopic, msg); err != nil {
			c.logger.Error("Failed to send to final DLQ: %v", err)
		}
	}
}

// createRebalanceCallback creates a kafka.RebalanceCb from the user's RebalanceCallback
func (c *KafkaConsumer) createRebalanceCallback() kafka.RebalanceCb {
	if c.config.RebalanceCallback == nil {
		return nil
	}

	return func(consumer *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			c.logger.Info("Partitions assigned: %v", e.Partitions)

			// Convert to our TopicPartition type
			partitions := make([]TopicPartition, len(e.Partitions))
			for i, tp := range e.Partitions {
				partitions[i] = TopicPartition{
					Topic:     *tp.Topic,
					Partition: tp.Partition,
					Offset:    int64(tp.Offset),
				}
			}

			// Call user's callback
			if err := c.config.RebalanceCallback(RebalanceEvent{
				Type:       "assigned",
				Partitions: partitions,
			}); err != nil {
				c.logger.Error("Rebalance callback error on assign: %v", err)
				return err
			}

			// Assign partitions to consumer
			return consumer.Assign(e.Partitions)

		case kafka.RevokedPartitions:
			c.logger.Info("Partitions revoked: %v", e.Partitions)

			// Convert to our TopicPartition type
			partitions := make([]TopicPartition, len(e.Partitions))
			for i, tp := range e.Partitions {
				partitions[i] = TopicPartition{
					Topic:     *tp.Topic,
					Partition: tp.Partition,
					Offset:    int64(tp.Offset),
				}
			}

			// Call user's callback
			if err := c.config.RebalanceCallback(RebalanceEvent{
				Type:       "revoked",
				Partitions: partitions,
			}); err != nil {
				c.logger.Error("Rebalance callback error on revoke: %v", err)
				return err
			}

			// Commit any pending offsets before unassigning (if auto-commit is disabled)
			if !c.config.AutoCommit {
				if _, err := consumer.Commit(); err != nil {
					// Ignore "no offset stored" errors
					if kafkaErr, ok := err.(kafka.Error); !ok || kafkaErr.Code() != kafka.ErrNoOffset {
						c.logger.Warn("Failed to commit offsets during rebalance: %v", err)
					}
				}
			}

			// Unassign partitions
			return consumer.Unassign()
		}

		return nil
	}
}

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

