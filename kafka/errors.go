package kafka

import "errors"

// Sentinel errors returned by the library. Assert on them with errors.Is.
var (
	ErrProducerClosed      = errors.New("producer is closed")
	ErrConsumerClosed      = errors.New("consumer is closed")
	ErrDLQClosed           = errors.New("dlq service is closed")
	ErrBrokersRequired     = errors.New("brokers are required")
	ErrGroupIDRequired     = errors.New("group id is required")
	ErrTopicsRequired      = errors.New("at least one topic is required")
	ErrNoHandler           = errors.New("no handler registered; call OnMessage/OnBatch/OnGroupedBatch before Start")
	ErrConsumerRunning     = errors.New("consumer is already running")
	ErrSkippedOnMaxRetries = errors.New("message skipped after max retries")

	// ErrDLQTopicRequired is returned by NewConsumer when DLQ is set without a topic.
	ErrDLQTopicRequired = errors.New("dlq topic is required")
	// ErrInvalidBatchSize is returned by NewConsumer when the batch size is not positive.
	ErrInvalidBatchSize = errors.New("batch size must be greater than zero")
	// ErrInvalidBatchTimeout is returned by NewConsumer when the batch timeout is not positive.
	ErrInvalidBatchTimeout = errors.New("batch timeout must be greater than zero")
)
