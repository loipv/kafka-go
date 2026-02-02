# kafka-go

A production-ready Go library for Kafka client and consumer functionality built on top of [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go). This library provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

## Features

- **Producer (Client)**: High-performance Kafka producer with `Send()`, `SendBatch()`, `SendQueued()` methods
- **Consumer**: Handler-based consumer with auto-discovery and registration
- **Batch Processing**: Intelligent batching with configurable size and timeout
- **Key-Based Grouping**: Group messages by key within batches for ordered processing
- **Rebalance Callback**: Custom handling for partition assignment/revocation events
- **Back Pressure**: Automatic pause/resume when consumers are overwhelmed
- **Idempotency**: In-memory duplicate prevention with TTL
- **Dead Letter Queue (DLQ)**: Automatic retry with exponential backoff
- **Circuit Breaker**: Prevent DLQ flooding when downstream is unhealthy
- **OpenTelemetry Tracing**: Distributed tracing across produce → consume with same trace ID
- **Health Checks**: Built-in health indicators
- **Graceful Shutdown**: Proper cleanup on application shutdown
- **Custom Logger**: Pluggable logging interface

## Installation

```bash
go get github.com/loipv/kafka-go
```

### Requirements

- Go 1.21+
- librdkafka (required by confluent-kafka-go)

### Platform Support

confluent-kafka-go is built on librdkafka (C library). Supported platforms:
- **Linux**: x64, arm64
- **macOS**: arm64 (Apple Silicon), x64 (Intel)
- **Windows**: x64

### Optional: OpenTelemetry Tracing

For distributed tracing support:

```bash
go get go.opentelemetry.io/otel
go get go.opentelemetry.io/otel/trace
go get go.opentelemetry.io/otel/sdk/trace
```

## Project Structure

```
kafka-go/
├── kafka/                     # Library code
│   ├── kafka.go              # Package documentation
│   ├── types.go              # Core types and interfaces
│   ├── options.go            # Client (Producer) configuration options
│   ├── consumer_options.go   # Consumer configuration options
│   ├── client.go             # Producer implementation
│   ├── consumer.go           # Consumer implementation
│   ├── dlq.go                # DLQ, Circuit Breaker, Idempotency
│   ├── health.go             # Health checks
│   ├── tracing.go            # OpenTelemetry tracing
│   └── logger.go             # Logger interface
├── examples/                 # Usage examples
│   ├── producer/             # Producer example
│   ├── consumer/             # Batch consumer with DLQ
│   ├── grouped_consumer/     # Key-based grouping
│   ├── typed_consumer/       # Generic typed handlers with JSON decoding
│   ├── health/               # Health check server
│   └── rebalance/            # Rebalance callback example
├── go.mod
├── go.sum
└── README.md
```

## Quick Start

### 1. Create Kafka Client (Producer)

```go
package main

import (
    "context"
    "log"

    "github.com/loipv/kafka-go/kafka"
)

func main() {
    // Create client with options
    client, err := kafka.NewClient(
        kafka.WithBrokers("localhost:9092"),
        kafka.WithClientID("my-app"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Send a message
    err = client.Send(context.Background(), "orders", &kafka.Message{
        Key:   []byte("customer-123"),
        Value: []byte(`{"orderId": "123", "amount": 100}`),
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### 2. Create a Consumer

```go
package main

import (
    "context"
    "log"

    "github.com/loipv/kafka-go/kafka"
)

func main() {
    // Create consumer
    consumer, err := kafka.NewConsumer(
        kafka.ConsumerWithBrokers("localhost:9092"),
        kafka.WithGroupID("order-processors"),
        kafka.WithTopics("orders"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close(context.Background())

    // Register handler
    consumer.Handle(func(ctx context.Context, msg *kafka.Message) error {
        log.Printf("Processing order: %s", string(msg.Value))
        return nil
    })

    // Start consuming (blocking)
    if err := consumer.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

### 3. Batch Consumer

```go
consumer, err := kafka.NewConsumer(
    kafka.ConsumerWithBrokers("localhost:9092"),
    kafka.WithGroupID("order-processors"),
    kafka.WithTopics("orders"),
    kafka.WithBatchProcessing(true),
    kafka.WithBatchSize(100),
    kafka.WithBatchTimeout(5*time.Second),
)

// Handle batch of messages
consumer.HandleBatch(func(ctx context.Context, msgs []*kafka.Message) error {
    log.Printf("Processing %d orders", len(msgs))
    for _, msg := range msgs {
        // Process each message
    }
    return nil
})
```

## Configuration

### Client Options

```go
client, err := kafka.NewClient(
    // Required
    kafka.WithBrokers("localhost:9092", "localhost:9093"),
    kafka.WithClientID("my-app"),

    // Optional - SSL/SASL
    kafka.WithSSL(true),
    kafka.WithSASL(&kafka.SASLConfig{
        Mechanism: "SCRAM-SHA-256",
        Username:  os.Getenv("KAFKA_USERNAME"),
        Password:  os.Getenv("KAFKA_PASSWORD"),
    }),

    // Optional - Connection settings
    kafka.WithConnectionTimeout(3*time.Second),
    kafka.WithRequestTimeout(30*time.Second),

    // Optional - Producer settings
    kafka.WithAcks(kafka.AcksAll),           // -1 (all), 0 (none), 1 (leader only)
    kafka.WithCompression(kafka.CompressionGZIP),
    kafka.WithIdempotent(true),

    // Optional - Retry configuration
    kafka.WithRetry(&kafka.RetryConfig{
        MaxRetries:       8,
        InitialInterval:  100*time.Millisecond,
        MaxInterval:      30*time.Second,
        Multiplier:       2.0,
    }),

    // Optional - Logging
    kafka.WithLogLevel(kafka.LogLevelInfo),
    kafka.WithLogger(customLogger), // Custom logger implementation

    // Optional - Tracing
    kafka.WithTracing(&kafka.TracingConfig{
        Enabled:       true,
        TracerName:    "my-kafka-service",
        TracerVersion: "1.0.0",
    }),
)
```

### Consumer Options

```go
consumer, err := kafka.NewConsumer(
    // Required
    kafka.ConsumerWithBrokers("localhost:9092"),
    kafka.WithGroupID("my-consumer-group"),
    kafka.WithTopics("topic1", "topic2"),

    // Optional - SSL/SASL authentication
    kafka.ConsumerWithSSL(true),
    kafka.ConsumerWithSASL(&kafka.SASLConfig{
        Mechanism: "SCRAM-SHA-256",
        Username:  os.Getenv("KAFKA_USERNAME"),
        Password:  os.Getenv("KAFKA_PASSWORD"),
    }),

    // Optional - Session settings
    kafka.WithSessionTimeout(30*time.Second),
    kafka.WithHeartbeatInterval(3*time.Second),
    kafka.WithRebalanceTimeout(60*time.Second),

    // Optional - Batch processing
    kafka.WithBatchProcessing(true),
    kafka.WithBatchSize(100),              // Max messages per batch
    kafka.WithBatchTimeout(5*time.Second), // Max wait time
    kafka.WithGroupByKey(true),            // Group messages by key

    // Optional - Pressure management
    kafka.WithBackPressureThreshold(80),   // Pause at 80% capacity
    kafka.WithMaxQueueSize(1000),

    // Optional - Idempotency
    kafka.WithIdempotencyKey(func(msg *kafka.Message) string {
        return string(msg.Headers["event-id"])
    }),
    kafka.WithIdempotencyTTL(1*time.Hour),

    // Optional - Dead Letter Queue
    kafka.WithDLQ(&kafka.DLQConfig{
        Topic:                 "orders-dlq",
        MaxRetries:            3,
        RetryDelay:            1*time.Second,
        RetryBackoffMultiplier: 2.0,
        IncludeErrorInfo:      true,
    }),

    // Optional - DLQ Auto-Retry
    kafka.WithDLQRetry(&kafka.DLQRetryConfig{
        Enabled:           true,
        MaxRetries:        5,
        Delay:             1*time.Minute,
        BackoffMultiplier: 2.0,
        FinalDLQTopic:     "orders-dlq-final",
    }),

    // Optional - Commit settings
    kafka.WithAutoCommit(true),
    kafka.WithAutoCommitInterval(5*time.Second),
    kafka.WithFromBeginning(false),

    // Optional - Partition assignment
    kafka.WithPartitionAssignor(kafka.AssignorCooperativeSticky),

    // Optional - Rebalance callback
    kafka.WithRebalanceCallback(func(event kafka.RebalanceEvent) error {
        // Handle partition assignment/revocation
        return nil
    }),

    // Optional - Retry
    kafka.WithConsumerRetry(&kafka.RetryConfig{
        MaxRetries:            3,
        InitialInterval:       1*time.Second,
        Multiplier:            2.0,
        SkipOnMaxRetries:      false,
    }),
)
```

## Consumer Patterns

### Basic Consumer

```go
consumer.Handle(func(ctx context.Context, msg *kafka.Message) error {
    // Process single message
    order := &Order{}
    if err := json.Unmarshal(msg.Value, order); err != nil {
        return err
    }
    return processOrder(order)
})
```

### Batch Consumer

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithBatchProcessing(true),
    kafka.WithBatchSize(100),
    kafka.WithBatchTimeout(5*time.Second),
    // ... other options
)

consumer.HandleBatch(func(ctx context.Context, msgs []*kafka.Message) error {
    // Process batch of messages
    for _, msg := range msgs {
        // ...
    }
    return nil
})
```

### Batch with Key Grouping

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithBatchProcessing(true),
    kafka.WithBatchSize(100),
    kafka.WithGroupByKey(true),
    // ... other options
)

consumer.HandleGroupedBatch(func(ctx context.Context, groups []kafka.GroupedBatch) error {
    // groups = [{Key: "customer-1", Messages: [...]}, ...]
    for _, group := range groups {
        log.Printf("Processing %d orders for %s", len(group.Messages), group.Key)
    }
    return nil
})
```

### Consumer with Rebalance Callback

Use rebalance callbacks to handle partition assignment/revocation events:

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithTopics("orders"),
    kafka.WithAutoCommit(false), // Manual commit for precise control
    kafka.WithPartitionAssignor(kafka.AssignorCooperativeSticky),
    
    kafka.WithRebalanceCallback(func(event kafka.RebalanceEvent) error {
        switch event.Type {
        case "assigned":
            for _, tp := range event.Partitions {
                log.Printf("Assigned: %s [%d] @ offset %d", 
                    tp.Topic, tp.Partition, tp.Offset)
                // Initialize resources for this partition
                // Load checkpoints, prepare buffers, etc.
            }
            
        case "revoked":
            for _, tp := range event.Partitions {
                log.Printf("Revoked: %s [%d]", tp.Topic, tp.Partition)
                // Flush buffers before losing partition
                // Save checkpoints, cleanup resources, etc.
            }
        }
        return nil
    }),
)
```

**Use cases for rebalance callbacks:**

| Use Case | Description |
|----------|-------------|
| **Manual offset commits** | Commit pending offsets before partition revocation |
| **Resource cleanup** | Close connections, flush buffers when losing partitions |
| **State initialization** | Load state from DB when assigned new partitions |
| **Checkpoint management** | Save/restore processing checkpoints |
| **Monitoring** | Log/alert on rebalance events |

### Consumer with DLQ

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithTopics("payments"),
    kafka.WithDLQ(&kafka.DLQConfig{
        Topic:                  "payments-dlq",
        MaxRetries:             3,
        RetryDelay:             1*time.Second,
        RetryBackoffMultiplier: 2.0,
        IncludeErrorInfo:       true,
    }),
    // ... other options
)

consumer.Handle(func(ctx context.Context, msg *kafka.Message) error {
    // If this returns error, message will be retried then sent to DLQ
    return processPayment(msg)
})
```

### Consumer with Idempotency

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithTopics("events"),
    kafka.WithIdempotencyKey(func(msg *kafka.Message) string {
        if eventID, ok := msg.Headers["event-id"]; ok {
            return string(eventID)
        }
        return ""
    }),
    kafka.WithIdempotencyTTL(1*time.Hour),
    // ... other options
)

consumer.Handle(func(ctx context.Context, msg *kafka.Message) error {
    // Duplicate messages (same event-id) will be skipped automatically
    return processEvent(msg)
})
```

### Consumer with Back Pressure

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithTopics("high-volume"),
    kafka.WithBatchProcessing(true),
    kafka.WithBackPressureThreshold(80), // Pause at 80% capacity
    kafka.WithMaxQueueSize(1000),
    // ... other options
)

consumer.HandleBatch(func(ctx context.Context, msgs []*kafka.Message) error {
    // Consumer will auto-pause when overwhelmed
    return processHighVolume(msgs)
})
```

## Producer API

### Client Methods

```go
// Send single message
err := client.Send(ctx, "topic", &kafka.Message{
    Key:   []byte("message-key"),
    Value: []byte(`{"data": "value"}`),
    Headers: kafka.Headers{
        "correlation-id": []byte("123"),
    },
})

// Send batch to single topic
err := client.SendBatch(ctx, "topic", []*kafka.Message{
    {Key: []byte("key1"), Value: []byte("value1")},
    {Key: []byte("key2"), Value: []byte("value2")},
})

// Send to multiple topics
err := client.SendMultiTopicBatch(ctx, []kafka.TopicMessages{
    {Topic: "topic1", Messages: []*kafka.Message{{Value: []byte("msg1")}}},
    {Topic: "topic2", Messages: []*kafka.Message{{Value: []byte("msg2")}}},
})

// Queue message for auto-batching
err := client.SendQueued(ctx, "topic", &kafka.Message{Value: []byte("message")})
```

### Message Options

```go
// Send with specific partition
err := client.Send(ctx, "topic", &kafka.Message{
    Key:       []byte("key"),
    Value:     []byte("value"),
    Partition: kafka.PartitionAny, // or specific partition number
    Timestamp: time.Now(),
    Headers: kafka.Headers{
        "custom-header": []byte("value"),
    },
})
```

## Health Checks

Built-in health check indicators:

```go
// Create health checker
health := kafka.NewHealthCheckerWithBrokers([]string{"localhost:9092"})

// Check if Kafka is healthy
result := health.Check(ctx)
if result.Status == kafka.HealthStatusUp {
    log.Println("Kafka is healthy")
}

// Check brokers
brokersResult := health.CheckBrokers(ctx)
log.Printf("Brokers: %v", brokersResult.Details["brokers"])

// Check consumer lag
lagResult := health.CheckConsumerLag(ctx, "my-consumer-group", 1000)
if lagResult.Status == kafka.HealthStatusDown {
    log.Printf("Consumer lag too high: %v", lagResult.Details["lag"])
}

// Check topic exists
topicResult := health.CheckTopic(ctx, "orders")

// HTTP handler integration
http.HandleFunc("/health/kafka", func(w http.ResponseWriter, r *http.Request) {
    result := health.Check(r.Context())
    if result.Status != kafka.HealthStatusUp {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    json.NewEncoder(w).Encode(result)
})
```

## OpenTelemetry Tracing

### How It Works

1. **Producer**: When sending a message, the library creates a span and injects the trace context (W3C Trace Context format) into Kafka message headers
2. **Consumer**: When receiving a message, the library extracts the trace context from headers and creates a child span linked to the producer's trace
3. **Batch Consumer**: For batch processing, the first message's trace becomes the parent, and all other messages are added as span links

```
┌─────────────────┐                         ┌─────────────────┐
│  HTTP Request   │                         │  Consumer App   │
│  TraceID: abc   │                         │                 │
│  ┌───────────┐  │      Kafka Topic        │  ┌───────────┐  │
│  │  publish  │──┼─────────────────────────┼──│  process  │  │
│  │  span     │  │  Headers:               │  │  span     │  │
│  │           │  │  traceparent: 00-abc... │  │           │  │
│  └───────────┘  │                         │  └───────────┘  │
│                 │                         │  TraceID: abc   │
└─────────────────┘                         └─────────────────┘
```

### Enable Tracing

```go
// Initialize OpenTelemetry (do this before creating Kafka client)
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func initTracer() (*sdktrace.TracerProvider, error) {
    exporter, err := otlptracehttp.New(context.Background(),
        otlptracehttp.WithEndpoint("localhost:4318"),
        otlptracehttp.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceName("my-kafka-service"),
        )),
    )
    otel.SetTracerProvider(tp)
    return tp, nil
}

// Create client with tracing enabled
client, err := kafka.NewClient(
    kafka.WithBrokers("localhost:9092"),
    kafka.WithClientID("my-app"),
    kafka.WithTracing(&kafka.TracingConfig{
        Enabled:       true,
        TracerName:    "my-kafka-service",
        TracerVersion: "1.0.0",
    }),
)
```

### Span Attributes (OpenTelemetry Semantic Conventions v1.24+)

**Producer Span:**

| Attribute | Example | Description |
|-----------|---------|-------------|
| `messaging.system` | `kafka` | Messaging system |
| `messaging.destination.name` | `orders` | Topic name |
| `messaging.operation.name` | `publish` | Operation name |
| `messaging.operation.type` | `publish` | Operation type |
| `messaging.destination.partition.id` | `0` | Partition (if specified) |
| `messaging.kafka.message.key` | `customer-123` | Message key (if present) |

**Consumer Span:**

| Attribute | Example | Description |
|-----------|---------|-------------|
| `messaging.system` | `kafka` | Messaging system |
| `messaging.destination.name` | `orders` | Topic name |
| `messaging.destination.partition.id` | `0` | Partition number |
| `messaging.operation.name` | `process` | Operation name |
| `messaging.operation.type` | `process` | Operation type |
| `messaging.kafka.offset` | `12345` | Message offset |
| `messaging.kafka.consumer.group` | `order-group` | Consumer group ID |
| `messaging.kafka.message.key` | `customer-123` | Message key (if present) |

**Batch Consumer Span (additional):**

| Attribute | Example | Description |
|-----------|---------|-------------|
| `messaging.batch.message_count` | `100` | Number of messages in batch |

## DLQ Features

### Circuit Breaker

The DLQ system includes a circuit breaker to prevent flooding DLQ when the system is unhealthy:

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithDLQ(&kafka.DLQConfig{
        Topic:      "orders-dlq",
        MaxRetries: 3,
        CircuitBreaker: &kafka.CircuitBreakerConfig{
            FailureThreshold: 5,        // Open circuit after 5 failures
            SuccessThreshold: 2,        // Close circuit after 2 successes
            Timeout:          30*time.Second,
        },
    }),
    // ... other options
)

// Get circuit state
state := consumer.GetCircuitState("orders-dlq")
// States: CircuitClosed, CircuitOpen, CircuitHalfOpen

// Reset circuit manually
consumer.ResetCircuit("orders-dlq")
```

### DLQ Metrics

```go
// Get DLQ metrics
metrics := consumer.GetDLQMetrics()
// {
//   Global: {HandlerRetries: 10, MessagesSentToDLQ: 2, ReprocessAttempts: 5},
//   ByTopic: {"orders": {HandlerRetries: 10, SentToDLQ: 2}},
// }
```

### DLQ Headers

| Header | Description |
|--------|-------------|
| `x-dlq-original-topic` | Original topic name |
| `x-dlq-handler-retry-count` | Retries before sent to DLQ |
| `x-dlq-timestamp` | Timestamp when sent to DLQ |
| `x-dlq-error-message` | Error message |
| `x-dlq-reprocess-count` | Reprocess attempts from DLQ |
| `x-dlq-reprocess-timestamp` | Timestamp of reprocess |
| `x-final-dlq-reason` | Reason sent to final DLQ |

## Custom Logger

The library supports custom logging via the `Logger` interface:

```go
// Logger interface
type Logger interface {
    Debug(format string, args ...interface{})
    Info(format string, args ...interface{})
    Warn(format string, args ...interface{})
    Error(format string, args ...interface{})
}

// Use custom logger
type MyLogger struct{}

func (l *MyLogger) Debug(format string, args ...interface{}) {
    // Your implementation
}
func (l *MyLogger) Info(format string, args ...interface{}) {
    // Your implementation
}
func (l *MyLogger) Warn(format string, args ...interface{}) {
    // Your implementation
}
func (l *MyLogger) Error(format string, args ...interface{}) {
    // Your implementation
}

// Use with client
client, _ := kafka.NewClient(
    kafka.WithBrokers("localhost:9092"),
    kafka.WithLogger(&MyLogger{}),
)

// Use with consumer
consumer, _ := kafka.NewConsumer(
    kafka.ConsumerWithBrokers("localhost:9092"),
    kafka.WithGroupID("my-group"),
    kafka.WithTopics("orders"),
    kafka.ConsumerWithLogger(&MyLogger{}),
)

// Or use the default no-op logger to disable logging
client, _ := kafka.NewClient(
    kafka.WithBrokers("localhost:9092"),
    kafka.WithLogger(kafka.NewNoopLogger()),
)
```

## Graceful Shutdown

```go
// Setup graceful shutdown
ctx, cancel := context.WithCancel(context.Background())

// Handle shutdown signals
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

go func() {
    <-sigChan
    log.Println("Shutting down...")
    cancel()
}()

// Start consumer (will stop when context is cancelled)
if err := consumer.Start(ctx); err != nil && err != context.Canceled {
    log.Fatal(err)
}

// Close with timeout
shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
defer shutdownCancel()

if err := consumer.Close(shutdownCtx); err != nil {
    log.Printf("Error during shutdown: %v", err)
}
```

## Error Handling

### Custom Error Handler

```go
consumer, _ := kafka.NewConsumer(
    kafka.WithErrorHandler(func(err error, msg *kafka.Message) {
        log.Printf("Error processing message: %v, key: %s", err, msg.Key)
        // Custom error handling logic
    }),
    // ... other options
)
```

### Retry Behavior

| Scenario | Default Behavior |
|----------|-----------------|
| Handler returns error | Retry with exponential backoff |
| Max retries exceeded (no DLQ) | Error logged, consumer continues |
| Max retries exceeded (with DLQ) | Message sent to DLQ topic |
| DLQ send fails | Circuit breaker may open |

## Examples

See the [examples](./examples) directory for complete working examples:

- **[producer](./examples/producer)** - Producer with batch and multi-topic sending
- **[consumer](./examples/consumer)** - Batch consumer with DLQ and idempotency
- **[grouped_consumer](./examples/grouped_consumer)** - Key-based message grouping
- **[typed_consumer](./examples/typed_consumer)** - Generic typed handlers with JSON decoding
- **[health](./examples/health)** - Health check HTTP server
- **[rebalance](./examples/rebalance)** - Rebalance callback with partition state management

Run examples:

```bash
# Producer
go run examples/producer/main.go

# Consumer
go run examples/consumer/main.go

# Grouped consumer
go run examples/grouped_consumer/main.go

# Health check server
go run examples/health/main.go

# Rebalance-aware consumer
go run examples/rebalance/main.go

# Typed consumer with JSON decoding
go run examples/typed_consumer/main.go
```

## Typed Handlers (Deserialization)

The library keeps `Message.Value` as `[]byte` for flexibility. Use generic helper functions to deserialize messages into typed structs.

### JSON Decoder Helper

```go
// Generic decoder function type
type DecodeFunc[T any] func([]byte) (T, error)

// JSON decoder
func JSONDecode[T any](data []byte) (T, error) {
    var v T
    err := json.Unmarshal(data, &v)
    return v, err
}

// Typed handler wrapper
func WithJSONDecoder[T any](
    handler func(context.Context, T, *kafka.Message) error,
) kafka.MessageHandler {
    return func(ctx context.Context, msg *kafka.Message) error {
        var v T
        if err := json.Unmarshal(msg.Value, &v); err != nil {
            return fmt.Errorf("decode error: %w", err)
        }
        return handler(ctx, v, msg)
    }
}
```

### Single Message

```go
type Order struct {
    OrderID  string  `json:"orderId"`
    Customer string  `json:"customer"`
    Amount   float64 `json:"amount"`
}

consumer.Handle(WithJSONDecoder(func(ctx context.Context, order Order, msg *kafka.Message) error {
    log.Printf("Order %s: %.2f", order.OrderID, order.Amount)
    return nil
}))
```

### Batch Processing

```go
// Skip invalid messages
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
                log.Printf("Skipping invalid message: %v", err)
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

// Usage
consumer.HandleBatch(WithBatchDecoder(JSONDecode[Order], func(ctx context.Context, orders []Order, msgs []*kafka.Message) error {
    return db.InsertOrders(ctx, orders)
}))
```

### Grouped Batch

```go
type TypedGroupedBatch[T any] struct {
    Key      string
    Values   []T
    Messages []*kafka.Message
}

func WithGroupedBatchDecoder[T any](
    decode DecodeFunc[T],
    handler func(context.Context, []TypedGroupedBatch[T]) error,
) kafka.GroupedBatchHandler {
    return func(ctx context.Context, groups []kafka.GroupedBatch) error {
        typedGroups := make([]TypedGroupedBatch[T], 0, len(groups))
        for _, group := range groups {
            values := make([]T, 0, len(group.Messages))
            for _, msg := range group.Messages {
                if value, err := decode(msg.Value); err == nil {
                    values = append(values, value)
                }
            }
            if len(values) > 0 {
                typedGroups = append(typedGroups, TypedGroupedBatch[T]{
                    Key:    group.Key,
                    Values: values,
                })
            }
        }
        return handler(ctx, typedGroups)
    }
}

// Usage: aggregate orders by customer
consumer.HandleGroupedBatch(WithGroupedBatchDecoder(JSONDecode[Order], func(ctx context.Context, groups []TypedGroupedBatch[Order]) error {
    for _, group := range groups {
        total := 0.0
        for _, order := range group.Values {
            total += order.Amount
        }
        log.Printf("Customer %s: %d orders, total %.2f", group.Key, len(group.Values), total)
    }
    return nil
}))
```

### Other Serialization Formats

```go
// Protobuf
import "google.golang.org/protobuf/proto"

func ProtoDecode[T proto.Message](data []byte) (T, error) {
    var v T
    v = reflect.New(reflect.TypeOf(v).Elem()).Interface().(T)
    err := proto.Unmarshal(data, v)
    return v, err
}

// MessagePack
import "github.com/vmihailenco/msgpack/v5"

func MsgpackDecode[T any](data []byte) (T, error) {
    var v T
    err := msgpack.Unmarshal(data, &v)
    return v, err
}
```

See [examples/typed_consumer](./examples/typed_consumer) for complete working code.

## API Reference

### Types

```go
// Message represents a Kafka message
type Message struct {
    Key       []byte
    Value     []byte
    Headers   Headers
    Partition int32
    Offset    int64
    Timestamp time.Time
    Topic     string
}

// Headers is a map of header key-value pairs
type Headers map[string][]byte

// GroupedBatch represents messages grouped by key
type GroupedBatch struct {
    Key      string
    Messages []*Message
}

// TopicMessages represents messages for a specific topic
type TopicMessages struct {
    Topic    string
    Messages []*Message
}

// RebalanceEvent represents a partition rebalance event
type RebalanceEvent struct {
    Type       string           // "assigned" or "revoked"
    Partitions []TopicPartition
}

// TopicPartition represents a topic and partition pair
type TopicPartition struct {
    Topic     string
    Partition int32
    Offset    int64
}

// HealthResult represents health check result
type HealthResult struct {
    Status  HealthStatus
    Details map[string]interface{}
    Error   error
}
```

### Enums

```go
// Acks configuration
const (
    AcksNone   Acks = 0   // No acknowledgment
    AcksLeader Acks = 1   // Leader acknowledgment
    AcksAll    Acks = -1  // All replicas acknowledgment
)

// Compression types
const (
    CompressionNone   Compression = 0
    CompressionGZIP   Compression = 1
    CompressionSnappy Compression = 2
    CompressionLZ4    Compression = 3
    CompressionZSTD   Compression = 4
)

// Partition assignors
const (
    AssignorRange             PartitionAssignor = "range"
    AssignorRoundRobin        PartitionAssignor = "roundrobin"
    AssignorCooperativeSticky PartitionAssignor = "cooperative-sticky"
)

// Health status
const (
    HealthStatusUp   HealthStatus = "UP"
    HealthStatusDown HealthStatus = "DOWN"
)

// Circuit breaker states
const (
    CircuitClosed   CircuitState = "CLOSED"
    CircuitOpen     CircuitState = "OPEN"
    CircuitHalfOpen CircuitState = "HALF_OPEN"
)

// Log levels
const (
    LogLevelNone  LogLevel = 0
    LogLevelError LogLevel = 1
    LogLevelWarn  LogLevel = 2
    LogLevelInfo  LogLevel = 3
    LogLevelDebug LogLevel = 4
)
```

### Client Interface

```go
type Client interface {
    // Send sends a single message to a topic
    Send(ctx context.Context, topic string, msg *Message) error
    
    // SendBatch sends multiple messages to a single topic
    SendBatch(ctx context.Context, topic string, msgs []*Message) error
    
    // SendMultiTopicBatch sends messages to multiple topics
    SendMultiTopicBatch(ctx context.Context, batches []TopicMessages) error
    
    // SendQueued queues a message for automatic batching
    SendQueued(ctx context.Context, topic string, msg *Message) error
    
    // Flush waits for all queued messages to be sent
    Flush(timeout time.Duration) error
    
    // Close closes the client
    Close() error
}
```

### Consumer Interface

```go
type Consumer interface {
    // Handle registers a handler for single messages
    Handle(handler MessageHandler)
    
    // HandleBatch registers a handler for batch messages
    HandleBatch(handler BatchHandler)
    
    // HandleGroupedBatch registers a handler for key-grouped batches
    HandleGroupedBatch(handler GroupedBatchHandler)
    
    // Start starts consuming messages (blocking)
    Start(ctx context.Context) error
    
    // Close closes the consumer
    Close(ctx context.Context) error
    
    // Pause pauses consumption
    Pause()
    
    // Resume resumes consumption
    Resume()
    
    // GetDLQMetrics returns DLQ metrics
    GetDLQMetrics() *DLQMetrics
    
    // GetCircuitState returns circuit breaker state
    GetCircuitState(dlqTopic string) CircuitState
    
    // ResetCircuit resets the circuit breaker
    ResetCircuit(dlqTopic string)
}

// Handler types
type MessageHandler func(ctx context.Context, msg *Message) error
type BatchHandler func(ctx context.Context, msgs []*Message) error
type GroupedBatchHandler func(ctx context.Context, groups []GroupedBatch) error
type RebalanceCallback func(event RebalanceEvent) error
```

## License

MIT
