package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/loipv/kafka-go/kafka"
)

// ============================================================
// Domain Types
// ============================================================

type Order struct {
	OrderID   string  `json:"orderId"`
	Customer  string  `json:"customer"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	CreatedAt string  `json:"createdAt"`
}

type Payment struct {
	PaymentID string  `json:"paymentId"`
	OrderID   string  `json:"orderId"`
	Amount    float64 `json:"amount"`
	Method    string  `json:"method"`
}

// ============================================================
// Generic Decoder Helpers
// ============================================================

// DecodeFunc decodes bytes to a typed value
type DecodeFunc[T any] func([]byte) (T, error)

// JSONDecode creates a JSON decoder for type T
func JSONDecode[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}

// WithDecoder wraps a typed handler with a decoder
func WithDecoder[T any](
	decode DecodeFunc[T],
	handler func(context.Context, T, *kafka.Message) error,
) kafka.MessageHandler {
	return func(ctx context.Context, msg *kafka.Message) error {
		value, err := decode(msg.Value)
		if err != nil {
			return fmt.Errorf("decode error for topic %s: %w", msg.Topic, err)
		}
		return handler(ctx, value, msg)
	}
}

// WithJSONDecoder is a convenience wrapper for JSON decoding
func WithJSONDecoder[T any](
	handler func(context.Context, T, *kafka.Message) error,
) kafka.MessageHandler {
	return WithDecoder(JSONDecode[T], handler)
}

// ============================================================
// Batch Decoder Helpers
// ============================================================

// WithBatchDecoder wraps a typed batch handler with a decoder
func WithBatchDecoder[T any](
	decode DecodeFunc[T],
	handler func(context.Context, []T, []*kafka.Message) error,
) kafka.BatchHandler {
	return func(ctx context.Context, msgs []*kafka.Message) error {
		values := make([]T, 0, len(msgs))
		validMsgs := make([]*kafka.Message, 0, len(msgs))

		for _, msg := range msgs {
			value, err := decode(msg.Value)
			if err != nil {
				// Log and skip invalid messages
				log.Printf("Skipping message due to decode error: %v", err)
				continue
			}
			values = append(values, value)
			validMsgs = append(validMsgs, msg)
		}

		if len(values) == 0 {
			return nil
		}

		return handler(ctx, values, validMsgs)
	}
}

// WithStrictBatchDecoder fails the entire batch if any message fails to decode
func WithStrictBatchDecoder[T any](
	decode DecodeFunc[T],
	handler func(context.Context, []T, []*kafka.Message) error,
) kafka.BatchHandler {
	return func(ctx context.Context, msgs []*kafka.Message) error {
		values := make([]T, len(msgs))

		for i, msg := range msgs {
			value, err := decode(msg.Value)
			if err != nil {
				return fmt.Errorf("decode error at index %d: %w", i, err)
			}
			values[i] = value
		}

		return handler(ctx, values, msgs)
	}
}

// ============================================================
// Grouped Batch Decoder Helpers
// ============================================================

// TypedGroupedBatch represents a key-grouped batch with typed values
type TypedGroupedBatch[T any] struct {
	Key      string
	Values   []T
	Messages []*kafka.Message
}

// WithGroupedBatchDecoder wraps a typed grouped batch handler
func WithGroupedBatchDecoder[T any](
	decode DecodeFunc[T],
	handler func(context.Context, []TypedGroupedBatch[T]) error,
) kafka.GroupedBatchHandler {
	return func(ctx context.Context, groups []kafka.GroupedBatch) error {
		typedGroups := make([]TypedGroupedBatch[T], 0, len(groups))

		for _, group := range groups {
			values := make([]T, 0, len(group.Messages))
			validMsgs := make([]*kafka.Message, 0, len(group.Messages))

			for _, msg := range group.Messages {
				value, err := decode(msg.Value)
				if err != nil {
					log.Printf("Skipping message in group %s: %v", group.Key, err)
					continue
				}
				values = append(values, value)
				validMsgs = append(validMsgs, msg)
			}

			if len(values) > 0 {
				typedGroups = append(typedGroups, TypedGroupedBatch[T]{
					Key:      group.Key,
					Values:   values,
					Messages: validMsgs,
				})
			}
		}

		if len(typedGroups) == 0 {
			return nil
		}

		return handler(ctx, typedGroups)
	}
}

// ============================================================
// Example Handlers
// ============================================================

func processOrder(ctx context.Context, order Order, msg *kafka.Message) error {
	log.Printf("Processing order: %s, customer: %s, amount: %.2f",
		order.OrderID, order.Customer, order.Amount)
	return nil
}

func processOrders(ctx context.Context, orders []Order, msgs []*kafka.Message) error {
	log.Printf("Processing %d orders", len(orders))
	for _, order := range orders {
		log.Printf("  - Order %s: %.2f", order.OrderID, order.Amount)
	}
	// Batch insert to database
	return nil
}

func processOrdersByCustomer(ctx context.Context, groups []TypedGroupedBatch[Order]) error {
	for _, group := range groups {
		total := 0.0
		for _, order := range group.Values {
			total += order.Amount
		}
		log.Printf("Customer %s: %d orders, total: %.2f",
			group.Key, len(group.Values), total)
	}
	return nil
}

// ============================================================
// Main - Examples
// ============================================================

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	// Example 1: Single message with typed handler
	example1SingleMessage(brokers)

	// Example 2: Batch processing with typed handler
	// example2BatchProcessing(brokers)

	// Example 3: Grouped batch with typed handler
	// example3GroupedBatch(brokers)
}

func example1SingleMessage(brokers string) {
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers(brokers),
		kafka.WithGroupID("typed-consumer-example"),
		kafka.WithTopics("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Register typed handler
	consumer.Handle(WithJSONDecoder(processOrder))

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	log.Println("Starting typed consumer...")
	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}

	consumer.Close(context.Background())
}

func example2BatchProcessing(brokers string) {
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers(brokers),
		kafka.WithGroupID("typed-batch-consumer"),
		kafka.WithTopics("orders"),
		kafka.WithBatchProcessing(true),
		kafka.WithBatchSize(100),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Register typed batch handler (skips invalid messages)
	consumer.HandleBatch(WithBatchDecoder(JSONDecode[Order], processOrders))

	// Or use strict mode (fails entire batch on decode error)
	// consumer.HandleBatch(WithStrictBatchDecoder(JSONDecode[Order], processOrders))

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	log.Println("Starting typed batch consumer...")
	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}

	consumer.Close(context.Background())
}

func example3GroupedBatch(brokers string) {
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers(brokers),
		kafka.WithGroupID("typed-grouped-consumer"),
		kafka.WithTopics("orders"),
		kafka.WithBatchProcessing(true),
		kafka.WithBatchSize(100),
		kafka.WithGroupByKey(true), // Group by message key (customer ID)
	)
	if err != nil {
		log.Fatal(err)
	}

	// Register typed grouped batch handler
	consumer.HandleGroupedBatch(WithGroupedBatchDecoder(JSONDecode[Order], processOrdersByCustomer))

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	log.Println("Starting typed grouped consumer...")
	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}

	consumer.Close(context.Background())
}
