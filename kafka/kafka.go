// Package kafka provides a production-ready Kafka client and consumer library
// built on top of confluent-kafka-go.
//
// Features:
//   - High-performance producer with Produce(), ProduceBatch(), ProduceAsync() methods
//   - Handler-based consumer with auto-discovery and registration
//   - Intelligent batch processing with configurable size and timeout
//   - Key-based grouping within batches for ordered processing
//   - In-memory idempotency with TTL
//   - Dead Letter Queue (DLQ) with automatic retry and exponential backoff
//   - OpenTelemetry distributed tracing
//   - Built-in health checks
//   - Graceful shutdown support
//
// Quick Start:
//
//	// Create producer
//	producer, err := kafka.NewProducer(
//	    kafka.ProducerWithBrokers("localhost:9092"),
//	    kafka.ProducerWithClientID("my-app"),
//	)
//
//	// Produce message
//	err = producer.Produce(ctx, "topic", &kafka.Message{
//	    Key:   []byte("key"),
//	    Value: []byte("value"),
//	})
//
//	// Create consumer
//	consumer, err := kafka.NewConsumer(
//	    kafka.ConsumerWithBrokers("localhost:9092"),
//	    kafka.ConsumerWithGroupID("my-group"),
//	    kafka.ConsumerWithTopics("topic"),
//	)
//
//	// Register handler
//	consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
//	    // Process message
//	    return nil
//	})
//
//	// Start consuming
//	err = consumer.Start(ctx)
package kafka

// Version of the library
const Version = "1.0.0"
