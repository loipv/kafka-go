// Package kafka provides a Kafka producer and consumer built on
// confluent-kafka-go (librdkafka).
//
// Features:
//   - Producer with synchronous Produce and ProduceBatch (back-pressure when
//     the client queue is full) and fire-and-forget ProduceAsync
//   - Handler-based consumer: one active handler, OnMessage, OnBatch or
//     OnGroupedBatch, and its mode follows it
//   - At-least-once delivery: an offset is stored only once a message is
//     parked (processed, confirmed on the DLQ, claimed by the error handler,
//     skipped as an idempotency duplicate, or dropped under the explicit
//     RetryConfig.SkipOnMaxRetries opt-in); any other message blocks its
//     partition with escalating backoff instead of being skipped
//   - Dead Letter Queue: a single confirmed produce per message, optionally to
//     a separate cluster
//   - In-memory idempotency filter (per process, lost on restart)
//   - OpenTelemetry tracing with W3C trace-context propagation
//   - Health checks for brokers, topics and consumer lag
//
// Quick Start:
//
//	producer, err := kafka.NewProducer(kafka.ProducerWithBrokers("localhost:9092"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer producer.Close()
//
//	err = producer.Produce(ctx, "orders", &kafka.Message{
//	    Key:   []byte("order-1"),
//	    Value: []byte(`{"id":"order-1"}`),
//	})
//
//	consumer, err := kafka.NewConsumer(
//	    kafka.ConsumerWithBrokers("localhost:9092"),
//	    kafka.ConsumerWithGroupID("my-group"),
//	    kafka.ConsumerWithTopics("orders"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Register one handler before Start; nil parks the message.
//	consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
//	    return nil
//	})
//
//	err = consumer.Start(ctx) // blocks until ctx is cancelled
//	_ = consumer.Close(context.Background())
//
// See the README for configuration, delivery semantics and examples.
package kafka

// Version is the library version.
const Version = "1.2.0"
