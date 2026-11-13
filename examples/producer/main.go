// Command producer demonstrates synchronous, batch and asynchronous
// producing with graceful shutdown.
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

// newMessage encodes order as a message keyed by customer.
func newMessage(order Order) (*kafka.Message, error) {
	data, err := json.Marshal(order)
	if err != nil {
		return nil, err
	}
	return &kafka.Message{
		Key:   []byte(order.CustomerID),
		Value: data,
		Headers: kafka.Headers{
			"event-id":   []byte(order.ID),
			"event-type": []byte("order.created"),
		},
	}, nil
}

func main() {
	producer, err := kafka.NewProducer(
		kafka.ProducerWithBrokers("localhost:9092"),
		kafka.ProducerWithClientID("order-producer"),
		kafka.ProducerWithAcks(kafka.AcksAll),
		kafka.ProducerWithCompression(kafka.CompressionGZIP),
		kafka.ProducerWithIdempotent(true),
		kafka.ProducerWithTracing(&kafka.TracingConfig{
			Enabled:    true,
			TracerName: "order-producer",
		}),

		// ProduceAsync reports delivery failures here, after it has returned.
		// Without a handler they are only logged.
		kafka.ProducerWithDeliveryErrorHandler(func(msg *kafka.Message, err error) {
			log.Printf("Async delivery failed (topic=%s, event-id=%s): %v",
				msg.Topic, msg.Headers["event-id"], err)
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	// Close flushes what is still queued, then releases the producer.
	defer func() { _ = producer.Close() }()

	// Cancel the context on SIGINT/SIGTERM to stop producing.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Example 1: Produce waits for each delivery report
	log.Println("Sending single messages...")
	for i := 0; i < 10; i++ {
		if ctx.Err() != nil {
			log.Println("Producer stopped")
			return
		}

		order := Order{
			ID:         fmt.Sprintf("order-%d", i),
			CustomerID: fmt.Sprintf("customer-%d", i%5),
			Amount:     float64(i+1) * 10.50,
			CreatedAt:  time.Now(),
		}
		msg, err := newMessage(order)
		if err != nil {
			log.Printf("Failed to encode order: %v", err)
			continue
		}

		if err := producer.Produce(ctx, "orders", msg); err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}

		log.Printf("Sent order %s for customer %s", order.ID, order.CustomerID)
	}

	// Example 2: ProduceBatch waits for the whole batch; the returned error
	// joins the failures of the messages that were not delivered.
	log.Println("Sending batch messages...")
	batch := make([]*kafka.Message, 0, 5)
	for i := 0; i < 5; i++ {
		msg, err := newMessage(Order{
			ID:         fmt.Sprintf("batch-order-%d", i),
			CustomerID: "batch-customer",
			Amount:     float64(i+1) * 25.0,
			CreatedAt:  time.Now(),
		})
		if err != nil {
			log.Printf("Failed to encode order: %v", err)
			continue
		}
		batch = append(batch, msg)
	}

	if err := producer.ProduceBatch(ctx, "orders", batch); err != nil {
		log.Printf("Failed to send batch: %v", err)
	} else {
		log.Println("Batch sent successfully!")
	}

	// Example 3: ProduceAsync only queues the message and returns; a nil
	// error does not mean it was delivered.
	log.Println("Sending async messages...")
	for i := 0; i < 5; i++ {
		msg, err := newMessage(Order{
			ID:         fmt.Sprintf("async-order-%d", i),
			CustomerID: "async-customer",
			Amount:     float64(i+1) * 5.0,
			CreatedAt:  time.Now(),
		})
		if err != nil {
			log.Printf("Failed to encode order: %v", err)
			continue
		}
		if err := producer.ProduceAsync("orders", msg); err != nil {
			log.Printf("Failed to queue message: %v", err)
		}
	}

	// Wait for the async messages to be delivered (or fail into the handler).
	if err := producer.Flush(10 * time.Second); err != nil {
		log.Printf("Flush error: %v", err)
	}

	log.Println("Producer finished")
}
