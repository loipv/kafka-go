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
	// Create Kafka consumer with key-based grouping
	// Messages with the same key are grouped together for batch processing
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.WithGroupID("order-group-processors"),
		kafka.WithTopics("orders"),

		// Enable batch processing with key grouping
		kafka.WithBatchProcessing(true),
		kafka.WithBatchSize(50),
		kafka.WithBatchTimeout(5*time.Second),
		kafka.WithGroupByKey(true), // Enable key grouping

		// Back pressure management
		kafka.WithBackPressureThreshold(80), // Pause at 80% capacity
		kafka.WithMaxQueueSize(1000),

		// Use cooperative sticky assignor for better rebalancing
		kafka.WithPartitionAssignor(kafka.AssignorCooperativeSticky),

		// Logging
		kafka.ConsumerWithLogLevel(kafka.LogLevelInfo),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Register grouped batch handler
	// Messages are grouped by key (customer ID in this case)
	consumer.HandleGroupedBatch(func(ctx context.Context, groups []kafka.GroupedBatch) error {
		log.Printf("Processing %d customer groups", len(groups))

		for _, group := range groups {
			var totalAmount float64
			orders := make([]Order, 0, len(group.Messages))

			for _, msg := range group.Messages {
				var order Order
				if err := json.Unmarshal(msg.Value, &order); err != nil {
					log.Printf("Failed to unmarshal order: %v", err)
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
	log.Println("Starting grouped consumer...")
	log.Println("Messages with the same key will be grouped together")
	log.Println("Press Ctrl+C to stop")

	if err := consumer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Consumer error: %v", err)
	}

	// Close consumer
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := consumer.Close(shutdownCtx); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	log.Println("Consumer stopped")
}
