package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/loipv/kafka-go/kafka"
)

// ExampleNewProducer constructs a producer and sends one confirmed message
// (Produce waits for the delivery report). The example has no // Output:
// comment on purpose: the produce result depends on a live broker, so godoc
// treats it as compile-checked only.
func ExampleNewProducer() {
	p, err := kafka.NewProducer(
		kafka.ProducerWithBrokers("localhost:9092"),
		kafka.ProducerWithAcks(kafka.AcksAll),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer p.Close()

	err = p.Produce(context.Background(), "orders", &kafka.Message{
		Key:   []byte("order-1"),
		Value: []byte(`{"id":"order-1"}`),
	})
	fmt.Println(err)
}

// ExampleProducer_ProduceAsync queues messages without waiting for delivery.
// A nil return only means the message was queued: delivery failures arrive
// later in the handler set with ProducerWithDeliveryErrorHandler. Flush waits
// for the queue to drain.
func ExampleProducer_ProduceAsync() {
	p, err := kafka.NewProducer(
		kafka.ProducerWithBrokers("localhost:9092"),
		kafka.ProducerWithDeliveryErrorHandler(func(msg *kafka.Message, err error) {
			log.Printf("delivery to %s failed: %v", msg.Topic, err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 3; i++ {
		err := p.ProduceAsync("orders", &kafka.Message{
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: []byte(`{"status":"created"}`),
		})
		if err != nil {
			// The local queue is full: back off and retry, or drop.
			log.Printf("queue message: %v", err)
		}
	}

	if err := p.Flush(10 * time.Second); err != nil {
		log.Printf("flush: %v", err)
	}
}

// ExampleNewConsumer consumes single messages until SIGINT or SIGTERM. The
// handler's mode follows what is registered (OnMessage here); register it
// before Start. Close runs after Start returns, whatever the reason.
func ExampleNewConsumer() {
	c, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("my-group"),
		kafka.ConsumerWithTopics("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	c.OnMessage(func(_ context.Context, msg *kafka.Message) error {
		log.Printf("%s[%d]@%d: %s", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		return nil // nil parks the message; an error starts retry, DLQ or block
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("start: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		log.Printf("close: %v", err)
	}
}

// ExampleConsumer_OnBatch processes messages in batches, flushed at the batch
// size or after the batch timeout. The handler is not retried in place: an
// error fails the whole batch, which is redelivered (or sent to the DLQ), so
// make the handler tolerate duplicates.
func ExampleConsumer_OnBatch() {
	c, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("my-group"),
		kafka.ConsumerWithTopics("orders"),
		kafka.ConsumerWithBatchSize(100),
		kafka.ConsumerWithBatchTimeout(2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	c.OnBatch(func(_ context.Context, msgs []*kafka.Message) error {
		log.Printf("inserting %d orders", len(msgs))
		return nil // e.g. one bulk insert; return its error to fail the batch
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("start: %v", err)
	}
	// Close flushes the batch still in memory.
	if err := c.Close(context.Background()); err != nil {
		log.Printf("close: %v", err)
	}
}

// ExampleConsumer_OnGroupedBatch processes a batch grouped by message key:
// one GroupedBatch per key, in first-seen order, with the messages of each key
// in consumption order. Success and failure apply to the whole batch.
func ExampleConsumer_OnGroupedBatch() {
	c, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("my-group"),
		kafka.ConsumerWithTopics("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	c.OnGroupedBatch(func(_ context.Context, groups []kafka.GroupedBatch) error {
		for _, g := range groups {
			log.Printf("customer %s: %d orders", g.Key, len(g.Messages))
		}
		return nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("start: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		log.Printf("close: %v", err)
	}
}

// ExampleConsumer_Commit commits offsets manually. With
// ConsumerWithAutoCommit(false) nothing is committed in the background, so the
// handler calls Commit once the message is durably handled. Commit stores and
// commits the offset after the message, synchronously.
func ExampleConsumer_Commit() {
	c, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("my-group"),
		kafka.ConsumerWithTopics("orders"),
		kafka.ConsumerWithAutoCommit(false),
	)
	if err != nil {
		log.Fatal(err)
	}

	c.OnMessage(func(_ context.Context, msg *kafka.Message) error {
		log.Printf("handling offset %d", msg.Offset) // e.g. write to the database
		return c.Commit(msg)
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("start: %v", err)
	}
	if err := c.Close(context.Background()); err != nil {
		log.Printf("close: %v", err)
	}
}

// ExampleNewHealthChecker checks the cluster, a topic and a consumer group's
// lag. Each check is bounded by its context deadline and always returns a
// result: failures are reported as a DOWN status, never as an error.
func ExampleNewHealthChecker() {
	h, err := kafka.NewHealthChecker(kafka.ProducerWithBrokers("localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res := h.Check(ctx)
	fmt.Println("cluster:", res.Status, res.Error)

	res = h.CheckTopic(ctx, "orders")
	fmt.Println("topic:", res.Status, res.Error)

	// DOWN when the group's total lag exceeds 1000 messages.
	res = h.CheckConsumerLag(ctx, "my-group", 1000)
	fmt.Println("lag:", res.Status, res.Error)
}
