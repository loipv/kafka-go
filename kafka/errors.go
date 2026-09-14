package kafka

import "errors"

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
)
