package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// maxDLQErrorBytes caps the x-dlq-error-message header value.
const maxDLQErrorBytes = 1024

// truncateUTF8 returns the longest prefix of s that is at most limit bytes and
// ends on a rune boundary. It returns s unchanged when it already fits.
func truncateUTF8(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// dlqService handles Dead Letter Queue operations
type dlqService struct {
	producer *ckafka.Producer
	config   *DLQConfig
	metrics  *metricsCollector
	closed   atomic.Bool   // true once closed
	done     chan struct{} // closed at the start of Close: releases waiting produces
}

// dlqDeliveryTimeout bounds how long the poll loop waits for a DLQ delivery
// report unless the DLQ connection's Raw config sets message.timeout.ms
// (or its alias delivery.timeout.ms).
const dlqDeliveryTimeout = 30 * time.Second

// dlqProducerConfig builds the DLQ producer's librdkafka config. The DLQ send
// runs inside the consumer's poll loop, so message.timeout.ms is capped at
// dlqDeliveryTimeout (rather than librdkafka's 5 minutes) unless Raw sets
// message.timeout.ms or its alias delivery.timeout.ms.
func dlqProducerConfig(cc connConfig) ckafka.ConfigMap {
	cm := cc.configMap()
	cm["acks"] = -1 // All replicas
	_, hasMessage := cm["message.timeout.ms"]
	_, hasAlias := cm["delivery.timeout.ms"] // librdkafka alias of message.timeout.ms
	if !hasMessage && !hasAlias {
		cm["message.timeout.ms"] = int(dlqDeliveryTimeout.Milliseconds())
	}
	return cm
}

// newDLQService creates a new DLQ service. It builds its producer from the
// same connConfig as the parent consumer, so SSL/SASL is never dropped.
func newDLQService(cc connConfig, config *DLQConfig, metrics *metricsCollector) (*dlqService, error) {
	cm := dlqProducerConfig(cc)

	producer, err := ckafka.NewProducer(&cm)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLQ producer: %w", err)
	}

	return &dlqService{
		producer: producer,
		config:   config,
		metrics:  metrics,
		done:     make(chan struct{}),
	}, nil
}

// produceToDLQ sends one failed message to the DLQ topic; see produceAll.
func (s *dlqService) produceToDLQ(ctx context.Context, msg *Message, err error, attempts int) error {
	return s.produceAll(ctx, []*Message{msg}, err, attempts)
}

// buildDLQMessage copies msg into a DLQ record. The caller's Message is never
// mutated — the x-dlq-* headers go on a clone. The delivery report echoes the
// source topic back through Opaque so metrics can be keyed by it.
func (s *dlqService) buildDLQMessage(msg *Message, cause error, attempts int) *ckafka.Message {
	out := *msg
	out.Headers = maps.Clone(msg.Headers) // nil-safe

	out.SetHeader("x-dlq-original-topic", []byte(msg.Topic))
	out.SetHeader("x-dlq-timestamp", appendTime(nil, time.Now()))
	out.SetHeader("x-dlq-handler-retry-count", []byte(strconv.Itoa(attempts))) // the real count
	if s.config.IncludeErrorInfo && cause != nil {
		out.SetHeader("x-dlq-error-message", []byte(truncateUTF8(cause.Error(), maxDLQErrorBytes)))
	}

	dlqTopic := s.config.Topic
	kafkaMsg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &dlqTopic,
			Partition: ckafka.PartitionAny,
		},
		Key:    out.Key,
		Value:  out.Value,
		Opaque: msg.Topic,
	}
	for k, v := range out.Headers {
		kafkaMsg.Headers = append(kafkaMsg.Headers, ckafka.Header{Key: k, Value: v})
	}
	return kafkaMsg
}

// produceAll sends msgs to the DLQ topic: it queues every message first, then
// awaits one delivery report per queued message, so a batch costs one round
// trip instead of len(msgs). Each message is a single produce attempt confirmed
// by its delivery report; the sent-to-DLQ metric counts confirmed messages only,
// keyed by source topic. It returns the errors.Join of every failure and nil
// only when all messages were confirmed; the caller decides what a failure
// means (the partition blocks). Closing the service ends the wait with an
// error wrapping ErrDLQClosed: no report can arrive once the producer is gone.
func (s *dlqService) produceAll(ctx context.Context, msgs []*Message, cause error, attempts int) error {
	if s.closed.Load() {
		return ErrDLQClosed
	}
	deliveryChan := make(chan ckafka.Event, len(msgs))
	var errs []error
	queued := 0
	for _, msg := range msgs {
		if perr := s.producer.Produce(s.buildDLQMessage(msg, cause, attempts), deliveryChan); perr != nil {
			errs = append(errs, fmt.Errorf("failed to produce to DLQ: %w", perr))
			continue
		}
		queued++
	}

	for range queued {
		select {
		case e := <-deliveryChan:
			source, err := confirmDLQEvent(e)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			s.metrics.incSentToDLQ(source)
		case <-ctx.Done():
			// Unconfirmed messages are not counted; their reports land in the
			// buffered channel and are dropped with it.
			errs = append(errs, fmt.Errorf("DLQ delivery interrupted: %w", ctx.Err()))
			return errors.Join(errs...)
		case <-s.done:
			errs = append(errs, fmt.Errorf("DLQ delivery interrupted: %w", ErrDLQClosed))
			return errors.Join(errs...)
		}
	}
	return errors.Join(errs...)
}

// confirmDLQEvent turns one delivery event into the confirmed message's source
// topic (metrics are keyed by it, not by the DLQ topic) or an error.
func confirmDLQEvent(e ckafka.Event) (string, error) {
	m, ok := e.(*ckafka.Message)
	if !ok {
		// Fail safe: an unconfirmed message must block its partition and retry.
		return "", fmt.Errorf("DLQ delivery: unexpected event %T: %v", e, e)
	}
	if m.TopicPartition.Error != nil {
		return "", fmt.Errorf("DLQ delivery failed: %w", m.TopicPartition.Error)
	}
	source, _ := m.Opaque.(string)
	return source, nil
}

// dlqCloseFlushTimeout bounds the flush in dlqService.Close. A variable so
// tests can shorten it.
var dlqCloseFlushTimeout = 10 * time.Second

// Close closes the DLQ service
func (s *dlqService) Close() error {
	// Use atomic CAS to ensure only one Close can succeed
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.done)

	s.producer.Flush(int(dlqCloseFlushTimeout.Milliseconds()))
	s.producer.Close()
	return nil
}

// appendTime formats time in RFC3339 format without allocating a string
// Uses time.AppendFormat for zero-allocation formatting
func appendTime(buf []byte, t time.Time) []byte {
	return t.AppendFormat(buf, time.RFC3339)
}
