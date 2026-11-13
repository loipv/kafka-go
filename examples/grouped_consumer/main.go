// Command grouped_consumer demonstrates key-grouped batch consumption.
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	// The handler registered below (OnGroupedBatch) puts the consumer in
	// batch mode and groups each batch by message key.
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("order-group-processors"),
		kafka.ConsumerWithTopics("orders"),

		// A batch is flushed at 50 messages or after 5 seconds.
		kafka.ConsumerWithBatchSize(50),
		kafka.ConsumerWithBatchTimeout(5*time.Second),

		// Cooperative sticky only revokes the partitions that move.
		kafka.ConsumerWithPartitionAssignor(kafka.AssignorCooperativeSticky),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Groups come in first-seen key order; messages keep their order within a
	// group. A returned error fails the whole batch, so tolerate redelivery.
	consumer.OnGroupedBatch(func(_ context.Context, groups []kafka.GroupedBatch) error {
		log.Printf("Processing %d customer groups", len(groups))

		for _, group := range groups {
			var totalAmount float64
			orders := make([]Order, 0, len(group.Messages))

			for _, msg := range group.Messages {
				var order Order
				if err := json.Unmarshal(msg.Value, &order); err != nil {
					log.Printf("Failed to unmarshal order: %v", err)
					// A skipped message is acknowledged with the batch and so lost;
					// returning an error instead fails (and redelivers) the whole batch.
					continue
				}
				orders = append(orders, order)
				totalAmount += order.Amount
			}

			// Process all orders for this customer together
			// This is useful for aggregations, maintaining order, etc.
			log.Printf("Customer %s: %d orders, total=$%.2f",
				group.Key, len(orders), totalAmount)

			// Example: Could batch insert to database here
			// db.BatchInsertOrders(group.Key, orders)
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

	log.Println("Starting grouped consumer... press Ctrl+C to stop")

	if err := consumer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Consumer error: %v", err)
	}

	// Close after Start returns (also after an error): it flushes the partial batch.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	log.Println("Consumer stopped")
}
