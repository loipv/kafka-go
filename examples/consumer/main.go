package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/loipv/kafka-go/kafka"
)

// Order represents an order
type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	CreatedAt  time.Time `json:"created_at"`
}

func main() {
	// Create Kafka consumer with batch processing and DLQ
	consumer, err := kafka.NewConsumer(
		// Connection
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.WithGroupID("order-processors"),
		kafka.WithTopics("orders"),

		// Batch processing
		kafka.WithBatchProcessing(true),
		kafka.WithBatchSize(10),
		kafka.WithBatchTimeout(3*time.Second),

		// DLQ configuration
		kafka.WithDLQ(&kafka.DLQConfig{
			Topic:                  "orders-dlq",
			MaxRetries:             3,
			RetryDelay:             1 * time.Second,
			RetryBackoffMultiplier: 2.0,
			IncludeErrorInfo:       true,
			CircuitBreaker: &kafka.CircuitBreakerConfig{
				FailureThreshold: 5,
				SuccessThreshold: 2,
				Timeout:          30 * time.Second,
			},
		}),

		// DLQ auto-retry
		kafka.WithDLQRetry(&kafka.DLQRetryConfig{
			Enabled:           true,
			MaxRetries:        5,
			Delay:             1 * time.Minute,
			BackoffMultiplier: 2.0,
			FinalDLQTopic:     "orders-dlq-final",
		}),

		// Idempotency
		kafka.WithIdempotencyKey(func(msg *kafka.Message) string {
			if eventID, ok := msg.Headers["event-id"]; ok {
				return string(eventID)
			}
			return ""
		}),
		kafka.WithIdempotencyTTL(1*time.Hour),

		// Retry configuration
		kafka.WithConsumerRetry(&kafka.RetryConfig{
			MaxRetries:      3,
			InitialInterval: 1 * time.Second,
			Multiplier:      2.0,
		}),

		// Tracing
		kafka.ConsumerWithTracing(&kafka.TracingConfig{
			Enabled:    true,
			TracerName: "order-consumer",
		}),

		// Logging
		kafka.ConsumerWithLogLevel(kafka.LogLevelInfo),

		// Error handler
		kafka.WithErrorHandler(func(err error, msg *kafka.Message) {
			log.Printf("Error processing message (key=%s): %v", string(msg.Key), err)
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Register batch handler
	consumer.HandleBatch(func(ctx context.Context, msgs []*kafka.Message) error {
		log.Printf("Processing batch of %d messages", len(msgs))

		for _, msg := range msgs {
			var order Order
			if err := json.Unmarshal(msg.Value, &order); err != nil {
				log.Printf("Failed to unmarshal order: %v", err)
				continue
			}

			log.Printf("Order %s: Customer=%s, Amount=$%.2f, Partition=%d, Offset=%d",
				order.ID, order.CustomerID, order.Amount, msg.Partition, msg.Offset)
		}

		return nil
	})

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Start consuming
	log.Println("Starting consumer...")
	log.Println("Press Ctrl+C to stop")

	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Consumer error: %v", err)
	}

	// Close consumer with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Print DLQ metrics before closing
	metrics := consumer.GetDLQMetrics()
	log.Printf("DLQ Metrics: %+v", metrics.Global)

	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	log.Println("Consumer stopped")
}
