package kafka

import "errors"

// Sentinel errors returned by the library. Assert on them with errors.Is.
var (
	// ErrProducerClosed is returned by Produce, ProduceBatch, ProduceAsync and
	// Flush after the Producer has been closed.
	ErrProducerClosed = errors.New("producer is closed")
	// ErrConsumerClosed is returned by Consumer.Start after Close, and by
	// Consumer.Commit once the consumer has been closed.
	ErrConsumerClosed = errors.New("consumer is closed")
	// ErrDLQClosed is the error of a DLQ produce attempted after the consumer's
	// DLQ producer was closed (by Close). The consumer logs it and does not park
	// the message; it is not returned from Start.
	ErrDLQClosed = errors.New("dlq service is closed")
	// ErrBrokersRequired is returned by NewProducer, NewConsumer and
	// NewHealthChecker when no broker address is configured.
	ErrBrokersRequired = errors.New("brokers are required")
	// ErrGroupIDRequired is returned by NewConsumer when the group id is empty.
	ErrGroupIDRequired = errors.New("group id is required")
	// ErrTopicsRequired is returned by NewConsumer when no topic is configured.
	ErrTopicsRequired = errors.New("at least one topic is required")
	// ErrNoHandler is returned by Consumer.Start when no handler was registered
	// with OnMessage, OnBatch or OnGroupedBatch.
	ErrNoHandler = errors.New("no handler registered; call OnMessage/OnBatch/OnGroupedBatch before Start")
	// ErrConsumerRunning is returned by Consumer.Start when the consumer is
	// already running.
	ErrConsumerRunning = errors.New("consumer is already running")
	// ErrSkippedOnMaxRetries wraps the last handler error that is passed to the
	// error handler and the DLQ when RetryConfig.SkipOnMaxRetries drops a message
	// after its retries. Test for it with errors.Is; the library does not return
	// it from Start.
	ErrSkippedOnMaxRetries = errors.New("message skipped after max retries")

	// ErrDLQTopicRequired is returned by NewConsumer when DLQ is set without a topic.
	ErrDLQTopicRequired = errors.New("dlq topic is required")
	// ErrInvalidBatchSize is returned by NewConsumer when the batch size is not positive.
	ErrInvalidBatchSize = errors.New("batch size must be greater than zero")
	// ErrInvalidBatchTimeout is returned by NewConsumer when the batch timeout is not positive.
	ErrInvalidBatchTimeout = errors.New("batch timeout must be greater than zero")
)
