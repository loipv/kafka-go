// Command consumer demonstrates batch consumption with a DLQ, idempotency
// and tracing.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// The handler registered below (OnBatch) puts the consumer in batch mode.
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("order-processors"),
		kafka.ConsumerWithTopics("orders"),

		// A batch is flushed at 10 messages or after 3 seconds.
		kafka.ConsumerWithBatchSize(10),
		kafka.ConsumerWithBatchTimeout(3*time.Second),

		// A message the handler fails on is produced to the DLQ topic; its
		// offset advances only once the DLQ produce is confirmed. If the DLQ
		// is unreachable the partition blocks instead of dropping messages.
		kafka.ConsumerWithDLQ(&kafka.DLQConfig{
			Topic:            "orders-dlq",
			IncludeErrorInfo: true,
		}),

		// Best-effort de-duplication by the event-id header (in-process only).
		kafka.ConsumerWithIdempotencyKey(func(msg *kafka.Message) string {
			return string(msg.Headers["event-id"])
		}),
		kafka.ConsumerWithIdempotencyTTL(time.Hour),

		kafka.ConsumerWithTracing(&kafka.TracingConfig{
			Enabled:    true,
			TracerName: "order-consumer",
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// A returned error fails the whole batch: its messages go to the DLQ.
	// Handlers must tolerate redelivery (at-least-once).
	consumer.OnBatch(func(_ context.Context, msgs []*kafka.Message) error {
		log.Printf("Processing batch of %d messages", len(msgs))

		for _, msg := range msgs {
			var order Order
			if err := json.Unmarshal(msg.Value, &order); err != nil {
				return fmt.Errorf("unmarshal order at offset %d: %w", msg.Offset, err)
			}

			log.Printf("Order %s: Customer=%s, Amount=$%.2f, Partition=%d, Offset=%d",
				order.ID, order.CustomerID, order.Amount, msg.Partition, msg.Offset)
		}

		return nil
	})

	// Cancel the context on SIGINT/SIGTERM so Start returns.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	log.Println("Starting consumer... press Ctrl+C to stop")

	if err := consumer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Consumer error: %v", err)
	}

	// Shut down in order: the Start context is already cancelled, now Close
	// flushes the partial batch and releases the client (also after a Start error).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	m := consumer.Metrics()
	log.Printf("Metrics: handler failures=%d, sent to DLQ=%d, blocked partitions=%d",
		m.HandlerFailures, m.SentToDLQ, len(m.BlockedPartitions))
	for topic, tm := range m.ByTopic {
		log.Printf("  %s: handler failures=%d, sent to DLQ=%d", topic, tm.HandlerFailures, tm.SentToDLQ)
	}

	log.Println("Consumer stopped")
}
