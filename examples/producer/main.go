package main

import (
	"context"
	"encoding/json"
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
	// Create Kafka client with all common options
	client, err := kafka.NewClient(
		kafka.WithBrokers("localhost:9092"),
		kafka.WithClientID("order-producer"),
		kafka.WithAcks(kafka.AcksAll),
		kafka.WithCompression(kafka.CompressionGZIP),
		kafka.WithIdempotent(true),
		kafka.WithLogLevel(kafka.LogLevelInfo),
		kafka.WithTracing(&kafka.TracingConfig{
			Enabled:    true,
			TracerName: "order-producer",
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Example 1: Send single messages
	log.Println("Sending single messages...")
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			log.Println("Producer stopped")
			return
		default:
		}

		order := Order{
			ID:         fmt.Sprintf("order-%d", i),
			CustomerID: fmt.Sprintf("customer-%d", i%5),
			Amount:     float64(i+1) * 10.50,
			CreatedAt:  time.Now(),
		}

		data, _ := json.Marshal(order)

		err := client.Send(ctx, "orders", &kafka.Message{
			Key:   []byte(order.CustomerID),
			Value: data,
			Headers: kafka.Headers{
				"event-id":   []byte(order.ID),
				"event-type": []byte("order.created"),
			},
		})

		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}

		log.Printf("Sent order %s for customer %s", order.ID, order.CustomerID)
		time.Sleep(100 * time.Millisecond)
	}

	// Example 2: Send batch messages
	log.Println("\nSending batch messages...")
	batchOrders := make([]*kafka.Message, 5)
	for i := 0; i < 5; i++ {
		order := Order{
			ID:         fmt.Sprintf("batch-order-%d", i),
			CustomerID: "batch-customer",
			Amount:     float64(i+1) * 25.0,
			CreatedAt:  time.Now(),
		}
		data, _ := json.Marshal(order)
		batchOrders[i] = &kafka.Message{
			Key:   []byte(order.CustomerID),
			Value: data,
			Headers: kafka.Headers{
				"event-id": []byte(order.ID),
			},
		}
	}

	if err := client.SendBatch(ctx, "orders", batchOrders); err != nil {
		log.Printf("Failed to send batch: %v", err)
	} else {
		log.Println("Batch sent successfully!")
	}

	// Example 3: Send to multiple topics
	log.Println("\nSending to multiple topics...")
	multiTopicBatch := []kafka.TopicMessages{
		{
			Topic: "orders",
			Messages: []*kafka.Message{
				{Key: []byte("multi-1"), Value: []byte(`{"type":"order"}`)},
			},
		},
		{
			Topic: "notifications",
			Messages: []*kafka.Message{
				{Key: []byte("multi-1"), Value: []byte(`{"type":"notification"}`)},
			},
		},
	}

	if err := client.SendMultiTopicBatch(ctx, multiTopicBatch); err != nil {
		log.Printf("Failed to send multi-topic batch: %v", err)
	} else {
		log.Println("Multi-topic batch sent successfully!")
	}

	// Flush remaining messages
	if err := client.Flush(10 * time.Second); err != nil {
		log.Printf("Flush error: %v", err)
	}

	log.Println("Producer finished")
}
