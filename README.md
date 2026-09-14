# kafka-go

A production-ready Go library for Kafka producer and consumer functionality built on top of [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go). This library provides enterprise-grade features including at-least-once delivery, intelligent batch processing, idempotency guarantees, key-based grouping, and a Dead Letter Queue with circuit breaker.

## Features

- **At-least-once delivery**: Offsets advance only for parked messages; unparkable messages block their partition instead of being dropped
- **Producer**: High-performance Kafka producer with `Produce()`, `ProduceBatch()`, `ProduceAsync()` methods
- **Consumer**: Handler-based consumer with auto-discovery and registration
- **Batch Processing**: Intelligent batching with configurable size and timeout
- **Key-Based Grouping**: Group messages by key within batches for ordered processing
- **Rebalance Callback**: Custom handling for partition assignment/revocation events
- **Idempotency**: In-memory duplicate prevention with TTL
- **Dead Letter Queue (DLQ)**: Automatic retry with exponential backoff
- **Circuit Breaker**: Prevent DLQ flooding when downstream is unhealthy
- **OpenTelemetry Tracing**: Distributed tracing across produce → consume with same trace ID
- **Health Checks**: Built-in health indicators
- **Graceful Shutdown**: Proper cleanup on application shutdown
- **Structured Logging**: `log/slog` integration

## Installation

```bash
go get github.com/loipv/kafka-go
```

### Requirements

- Go 1.24+
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
│   ├── types.go              # Core types and enums
│   ├── options.go            # Producer options (ProducerWith*)
│   ├── consumer_options.go   # Consumer options (ConsumerWith*)
│   ├── config.go             # connConfig seam shared by all internal clients
│   ├── client.go             # Producer implementation
│   ├── consumer.go           # Consumer implementation
│   ├── dlq.go                # DLQ, Circuit Breaker, Idempotency
│   ├── health.go             # Health checks
│   ├── tracing.go            # OpenTelemetry tracing
│   └── errors.go             # Sentinel errors
├── examples/                 # Usage examples
│   ├── producer/             # Producer example
│   ├── consumer/             # Batch consumer with DLQ
│   ├── grouped_consumer/     # Key-based grouping
│   ├── typed_consumer/       # Generic typed handlers with JSON decoding
│   ├── health/               # Health check server
│   └── rebalance/            # Rebalance callback + manual commit
├── .github/workflows/        # CI (test, lint, integration, darwin)
├── go.mod
├── go.sum
├── Makefile
├── CHANGELOG.md
└── README.md
```

## Quick Start

### 1. Create a Producer

```go
package main

import (
    "context"
    "log"

    "github.com/loipv/kafka-go/kafka"
)

func main() {
    // Create producer with options
    producer, err := kafka.NewProducer(
        kafka.ProducerWithBrokers("localhost:9092"),
        kafka.ProducerWithClientID("my-app"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Produce a message
    err = producer.Produce(context.Background(), "orders", &kafka.Message{
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
        kafka.ConsumerWithGroupID("order-processors"),
        kafka.ConsumerWithTopics("orders"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close(context.Background())

    // Register handler
    consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
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
    kafka.ConsumerWithGroupID("order-processors"),
    kafka.ConsumerWithTopics("orders"),
    kafka.ConsumerWithBatchProcessing(true),
    kafka.ConsumerWithBatchSize(100),
    kafka.ConsumerWithBatchTimeout(5*time.Second),
)

// OnBatch: process a batch of messages
consumer.OnBatch(func(ctx context.Context, msgs []*kafka.Message) error {
    log.Printf("Processing %d orders", len(msgs))
    for _, msg := range msgs {
        // Process each message
    }
    return nil
})
```

## Configuration

### Producer Options

```go
producer, err := kafka.NewProducer(
    // Required
    kafka.ProducerWithBrokers("localhost:9092", "localhost:9093"),
    kafka.ProducerWithClientID("my-app"),

    // Optional - SSL/SASL
    kafka.ProducerWithSSL(true),
    kafka.ProducerWithSASL(&kafka.SASLConfig{
        Mechanism: "SCRAM-SHA-256",
        Username:  os.Getenv("KAFKA_USERNAME"),
        Password:  os.Getenv("KAFKA_PASSWORD"),
    }),

    // Optional - Connection settings
    kafka.ProducerWithConnectionTimeout(3*time.Second),
    kafka.ProducerWithRequestTimeout(30*time.Second),

    // Optional - Producer settings
    kafka.ProducerWithAcks(kafka.AcksAll),           // -1 (all), 0 (none), 1 (leader only)
    kafka.ProducerWithCompression(kafka.CompressionGZIP),
    kafka.ProducerWithIdempotent(true),

    // Optional - Retry configuration
    kafka.ProducerWithRetry(&kafka.RetryConfig{
        MaxRetries:       8,
        InitialInterval:  100*time.Millisecond,
        MaxInterval:      30*time.Second,
        Multiplier:       2.0,
    }),

    // Optional - Logging (defaults to slog.Default())
    kafka.ProducerWithLogger(slog.Default()),

    // Optional - Tracing
    kafka.ProducerWithTracing(&kafka.TracingConfig{
        Enabled:       true,
        TracerName:    "my-kafka-service",
        TracerVersion: "1.0.0",
    }),

    // Optional - Raw librdkafka config keys, merged last
    kafka.ProducerWithRawConfig(map[string]any{
        "linger.ms": 5,
    }),
)
```

### Consumer Options

```go
consumer, err := kafka.NewConsumer(
    // Required
    kafka.ConsumerWithBrokers("localhost:9092"),
    kafka.ConsumerWithGroupID("my-consumer-group"),
    kafka.ConsumerWithTopics("topic1", "topic2"),

    // Optional - SSL/SASL authentication
    kafka.ConsumerWithSSL(true),
    kafka.ConsumerWithSASL(&kafka.SASLConfig{
        Mechanism: "SCRAM-SHA-256",
        Username:  os.Getenv("KAFKA_USERNAME"),
        Password:  os.Getenv("KAFKA_PASSWORD"),
    }),

    // Optional - Session settings
    kafka.ConsumerWithSessionTimeout(30*time.Second),
    kafka.ConsumerWithHeartbeatInterval(3*time.Second),
    kafka.ConsumerWithRebalanceTimeout(60*time.Second),

    // Optional - Batch processing
    kafka.ConsumerWithBatchProcessing(true),
    kafka.ConsumerWithBatchSize(100),              // Max messages per batch
    kafka.ConsumerWithBatchTimeout(5*time.Second), // Max wait time
    kafka.ConsumerWithGroupByKey(true),            // Group messages by key

    // Optional - Idempotency
    kafka.ConsumerWithIdempotencyKey(func(msg *kafka.Message) string {
        return string(msg.Headers["event-id"])
    }),
    kafka.ConsumerWithIdempotencyTTL(1*time.Hour),

    // Optional - Dead Letter Queue
    kafka.ConsumerWithDLQ(&kafka.DLQConfig{
        Topic:                 "orders-dlq",
        MaxRetries:            3,
        RetryDelay:            1*time.Second,
        RetryBackoffMultiplier: 2.0,
        IncludeErrorInfo:      true,
    }),

    // Optional - DLQ Auto-Retry
    kafka.ConsumerWithDLQRetry(&kafka.DLQRetryConfig{
        Enabled:           true,
        MaxRetries:        5,
        Delay:             1*time.Minute,
        BackoffMultiplier: 2.0,
        FinalDLQTopic:     "orders-dlq-final",
    }),

    // Optional - Commit settings
    kafka.ConsumerWithAutoCommit(true),
    kafka.ConsumerWithAutoCommitInterval(5*time.Second),
    kafka.ConsumerWithFromBeginning(false),

    // Optional - Partition assignment
    kafka.ConsumerWithPartitionAssignor(kafka.AssignorCooperativeSticky),

    // Optional - Rebalance callback
    kafka.ConsumerWithRebalanceCallback(func(event kafka.RebalanceEvent) error {
        // Handle partition assignment/revocation
        return nil
    }),

    // Optional - Retry
    kafka.ConsumerWithRetry(&kafka.RetryConfig{
        MaxRetries:            3,
        InitialInterval:       1*time.Second,
        MaxInterval:           30*time.Second,
        Multiplier:            2.0,
        SkipOnMaxRetries:      false,
    }),

    // Optional - Raw librdkafka config keys, merged last
    kafka.ConsumerWithRawConfig(map[string]any{
        "queued.max.messages.kbytes": 64,
    }),
)
```

## Consumer Patterns

### Basic Consumer

```go
consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
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
    kafka.ConsumerWithBatchProcessing(true),
    kafka.ConsumerWithBatchSize(100),
    kafka.ConsumerWithBatchTimeout(5*time.Second),
    // ... other options
)

consumer.OnBatch(func(ctx context.Context, msgs []*kafka.Message) error {
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
    kafka.ConsumerWithBatchProcessing(true),
    kafka.ConsumerWithBatchSize(100),
    kafka.ConsumerWithGroupByKey(true),
    // ... other options
)

consumer.OnGroupedBatch(func(ctx context.Context, groups []kafka.GroupedBatch) error {
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
    kafka.ConsumerWithTopics("orders"),
    kafka.ConsumerWithAutoCommit(false), // Manual commit for precise control
    kafka.ConsumerWithPartitionAssignor(kafka.AssignorCooperativeSticky),
    
    kafka.ConsumerWithRebalanceCallback(func(event kafka.RebalanceEvent) error {
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
    kafka.ConsumerWithTopics("payments"),
    kafka.ConsumerWithDLQ(&kafka.DLQConfig{
        Topic:                  "payments-dlq",
        MaxRetries:             3,
        RetryDelay:             1*time.Second,
        RetryBackoffMultiplier: 2.0,
        IncludeErrorInfo:       true,
    }),
    // ... other options
)

consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
    // If this returns error, message will be retried then sent to DLQ
    return processPayment(msg)
})
```

### Consumer with Idempotency

```go
consumer, _ := kafka.NewConsumer(
    kafka.ConsumerWithTopics("events"),
    kafka.ConsumerWithIdempotencyKey(func(msg *kafka.Message) string {
        if eventID, ok := msg.Headers["event-id"]; ok {
            return string(eventID)
        }
        return ""
    }),
    kafka.ConsumerWithIdempotencyTTL(1*time.Hour),
    // ... other options
)

consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
    // Duplicate messages (same event-id) will be skipped automatically
    return processEvent(msg)
})
```

## Delivery Guarantees

Delivery is **at-least-once by default**. The consumer sets
`enable.auto.offset.store=false` and stores an offset only when the message is
genuinely parked — auto-commit then commits what you *finished*, not what you
*read*:

| Outcome | Offset stored? |
|---------|----------------|
| Handler succeeded | Yes |
| Written to DLQ, delivery report confirmed | Yes |
| `ErrorHandler` returned `nil` (user took ownership) | Yes |
| `SkipOnMaxRetries = true` | Yes — the one explicit opt-in to loss |
| No DLQ configured · DLQ send failed · circuit breaker open | **No — partition blocks** |

**Blocked partitions.** A message that cannot be parked (failing handler, no
DLQ, or the DLQ itself is down) does not get dropped: its partition is paused,
seeked back to the message's offset, and retried with escalating backoff
(`RetryConfig.InitialInterval`, doubling per consecutive block, capped at
`MaxInterval`, default 30s). Other partitions keep flowing, the poll loop never
sleeps, and a transient DLQ outage self-heals without operator action. Entering
the blocked state logs a WARN naming topic/partition/offset, and
`DLQMetrics().BlockedPartitions` exposes the currently-blocked set as a gauge.

**Batch mode.** If a batch handler fails, no offset from the batch is stored —
the whole batch is re-delivered. Messages that did succeed may therefore be
processed twice; that is what `ConsumerWithIdempotencyKey` is for.

**Skipping instead of blocking.** Set `RetryConfig.SkipOnMaxRetries: true` to
restore the old lossy behavior: after max retries the error handler fires, the
DLQ (if configured) receives the message, and the offset advances.

**Producer side.** `Produce`/`ProduceBatch`/`ProduceMultiTopicBatch` are
synchronous and return errors. `ProduceAsync` cannot — a failed delivery is
reported through the handler registered with `ProducerWithDeliveryErrorHandler`:

```go
producer, err := kafka.NewProducer(
    kafka.ProducerWithBrokers("localhost:9092"),
    kafka.ProducerWithDeliveryErrorHandler(func(msg *kafka.Message, err error) {
        // Persist or re-queue — without this, async delivery failures are only logged
        log.Printf("delivery failed for topic=%s key=%s: %v", msg.Topic, msg.Key, err)
    }),
)
```

## Producer API

### Producer Methods

```go
// Produce single message
err := producer.Produce(ctx, "topic", &kafka.Message{
    Key:   []byte("message-key"),
    Value: []byte(`{"data": "value"}`),
    Headers: kafka.Headers{
        "correlation-id": []byte("123"),
    },
})

// Produce batch to single topic
err := producer.ProduceBatch(ctx, "topic", []*kafka.Message{
    {Key: []byte("key1"), Value: []byte("value1")},
    {Key: []byte("key2"), Value: []byte("value2")},
})

// Produce to multiple topics
err := producer.ProduceMultiTopicBatch(ctx, []kafka.TopicBatch{
    {Topic: "topic1", Messages: []*kafka.Message{{Value: []byte("msg1")}}},
    {Topic: "topic2", Messages: []*kafka.Message{{Value: []byte("msg2")}}},
})

// Produce without waiting for the delivery report (librdkafka batches internally)
err := producer.ProduceAsync("topic", &kafka.Message{Value: []byte("message")})
```

### Message Options

```go
// Partitioning is by Key — the producer ignores Message.Partition
err := producer.Produce(ctx, "topic", &kafka.Message{
    Key:       []byte("key"),
    Value:     []byte("value"),
    Timestamp: time.Now(),
    Headers: kafka.Headers{
        "custom-header": []byte("value"),
    },
})
```

## Health Checks

Built-in health check indicators:

```go
// Create health checker (owns a producer; Close it when done)
health, err := kafka.NewHealthChecker(kafka.ProducerWithBrokers("localhost:9092"))
if err != nil {
    log.Fatal(err)
}
defer health.Close()

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
// Initialize OpenTelemetry (optional — W3C trace-context propagation works
// without any global setup; this wires an exporter)
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

// Create producer with tracing enabled
producer, err := kafka.NewProducer(
    kafka.ProducerWithBrokers("localhost:9092"),
    kafka.ProducerWithClientID("my-app"),
    kafka.ProducerWithTracing(&kafka.TracingConfig{
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
    kafka.ConsumerWithDLQ(&kafka.DLQConfig{
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
state := consumer.CircuitState("orders-dlq")
// States: CircuitClosed, CircuitOpen, CircuitHalfOpen

// Reset circuit manually
consumer.ResetCircuit("orders-dlq")
```

### DLQ Metrics

```go
// Get DLQ metrics
metrics := consumer.DLQMetrics()
// {
//   Global: {HandlerRetries: 10, MessagesSentToDLQ: 2, ReprocessAttempts: 5},
//   ByTopic: {"orders": {HandlerRetries: 10, SentToDLQ: 2}},
//   BlockedPartitions: [{Topic: "orders", Partition: 2}], // currently blocked
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

## Logging

The library logs through `log/slog` (`*slog.Logger`). The default is
`slog.Default()`; pass any `*slog.Logger` to customize:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
producer, err := kafka.NewProducer(
    kafka.ProducerWithBrokers("localhost:9092"),
    kafka.ProducerWithLogger(logger),
)
```

```go
// Use with consumer
consumer, _ := kafka.NewConsumer(
    kafka.ConsumerWithBrokers("localhost:9092"),
    kafka.ConsumerWithGroupID("my-group"),
    kafka.ConsumerWithTopics("orders"),
    kafka.ConsumerWithLogger(logger),
)

// Silence library logging entirely
producer, _ := kafka.NewProducer(
    kafka.ProducerWithBrokers("localhost:9092"),
    kafka.ProducerWithLogger(slog.New(slog.DiscardHandler)),
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

The error handler runs after retries are exhausted, before the DLQ. Its return
value decides ownership: `nil` means you persisted the message yourself (the
offset advances); any error defers to the DLQ / blocked-partition machinery.

```go
consumer, _ := kafka.NewConsumer(
    kafka.ConsumerWithErrorHandler(func(ctx context.Context, msg *kafka.Message, err error) error {
        log.Printf("Error processing message: %v, key: %s", err, msg.Key)
        // Custom error handling logic
        return err // or nil to claim ownership and advance the offset
    }),
    // ... other options
)
```

### Retry Behavior

| Scenario | Default Behavior |
|----------|-----------------|
| Handler returns error | Retried with exponential backoff (`RetryConfig`) |
| Max retries exceeded, DLQ configured | Message sent to DLQ (produce confirmed), offset stored |
| Max retries exceeded, no DLQ | Partition blocks; message retried with escalating backoff |
| `SkipOnMaxRetries: true` | Error handler fires, DLQ (if any) receives message, offset advances |
| DLQ send fails / circuit breaker open | Partition blocks; retried until the DLQ recovers |

## Testing

The test suite runs in three tiers:

| Tier | Covers | Needs Docker? |
|------|--------|---------------|
| Unit | Pure logic: config maps, retry math, tracing carriers, circuit breaker | No |
| Mock broker | Send/consume round-trips, delivery reports, commit/resume, DLQ, blocked partitions, broker-down | No — `kafka.NewMockCluster` runs in-process |
| Integration | SASL/TLS auth regression, cooperative rebalance with two consumers, real consumer lag | Yes — testcontainers + Redpanda |

```bash
make test              # unit + mock-broker tests
make test-short        # unit only (skips the mock-broker tier)
make test-race         # race detector
make test-integration  # Redpanda container suite (requires Docker)
```

The integration tests live behind the `//go:build integration` build tag, so
they are excluded from the default build graph — the Docker client is only
pulled in when explicitly requested. CI runs the first two tiers on every PR
and the integration tier on main.

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

consumer.OnMessage(WithJSONDecoder(func(ctx context.Context, order Order, msg *kafka.Message) error {
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
consumer.OnBatch(WithBatchDecoder(JSONDecode[Order], func(ctx context.Context, orders []Order, msgs []*kafka.Message) error {
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
consumer.OnGroupedBatch(WithGroupedBatchDecoder(JSONDecode[Order], func(ctx context.Context, groups []TypedGroupedBatch[Order]) error {
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

// TopicBatch represents messages for a specific topic
type TopicBatch struct {
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
    Details map[string]any
    Error   string // error message, empty when healthy
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
```

## License

MIT
