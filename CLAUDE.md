# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A production-ready Go library for Kafka client and consumer functionality built on top of [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go). Provides enterprise-grade features including intelligent batch processing, idempotency guarantees, key-based grouping, and automatic pressure management.

## Development Commands

```bash
# Build the library
go build ./...

# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run a specific test
go test -v -run TestName ./kafka

# Run examples
go run examples/producer/main.go
go run examples/consumer/main.go
go run examples/grouped_consumer/main.go
go run examples/health/main.go
go run examples/rebalance/main.go
go run examples/typed_consumer/main.go

# Check for race conditions
go test -race ./...

# Get dependencies
go mod download

# Make targets (mirror CI)
# make build / make test / make lint / make cover run the same checks as the CI workflow
```

## Requirements

- Go 1.24+
- librdkafka (C library required by confluent-kafka-go)

## Architecture

### Core Components

All library code lives in the `kafka/` package:

- **Client (Producer)**: `client.go` - Implements `Client` interface with `Send()`, `SendBatch()`, `SendMultiTopicBatch()`, `SendQueued()` methods. Uses internal message queue with automatic batching.
- **Consumer**: `consumer.go` - Implements `Consumer` interface with handler-based message processing. Supports single message, batch, and key-grouped batch handlers.
- **Types**: `types.go` - Core types (`Message`, `Headers`, `GroupedBatch`, `TopicPartition`), interfaces (`Client`, `Consumer`), and enums.
- **Options**: `options.go` (client), `consumer_options.go` (consumer) - Functional options pattern for configuration.

### Supporting Components

- **DLQ Service**: `dlq.go` - Dead Letter Queue with circuit breaker pattern and idempotency store
- **Health Checks**: `health.go` - Broker, topic, and consumer lag health indicators
- **Tracing**: `tracing.go` - OpenTelemetry distributed tracing with W3C Trace Context propagation
- **Logger**: `logger.go` - Pluggable logging interface

### Key Patterns

**Functional Options**: Both client and consumer use the functional options pattern:
```go
client, err := kafka.NewClient(
    kafka.WithBrokers("localhost:9092"),
    kafka.WithClientID("my-app"),
)
```

**Handler Registration**: Consumer uses handler functions registered before `Start()`:
- `Handle(MessageHandler)` - single messages
- `HandleBatch(BatchHandler)` - batches
- `HandleGroupedBatch(GroupedBatchHandler)` - key-grouped batches

**Interface Verification**: Both `KafkaClient` and `KafkaConsumer` use compile-time interface verification:
```go
var _ Client = (*KafkaClient)(nil)
var _ Consumer = (*KafkaConsumer)(nil)
```

### Configuration Naming Conventions

Client options use `With*` prefix (e.g., `WithBrokers`, `WithClientID`).
Consumer options use:
- `ConsumerWith*` for connection/auth (e.g., `ConsumerWithBrokers`, `ConsumerWithSSL`)
- `With*` for behavior settings (e.g., `WithGroupID`, `WithTopics`, `WithBatchProcessing`)

### Message Flow

1. **Producer**: Message -> `buildKafkaMessage()` -> confluent producer -> delivery channel -> confirmation
2. **Consumer**: confluent consumer -> `convertMessage()` -> idempotency check -> handler (with retry) -> error handling (optional DLQ)

### Concurrency

- `KafkaClient` uses `sync.RWMutex` for producer state and separate mutex for queue operations
- `KafkaConsumer` uses multiple mutexes: state (`mu`), batch (`batchMu`), circuit breakers (`cbMu`)
- Atomic operations for queue size tracking in back pressure management

### Typed Handlers Pattern

The library keeps `Message.Value` as `[]byte`. For typed deserialization, use generic wrapper functions (see `examples/typed_consumer/`):

```go
// Generic pattern for typed handlers
func WithJSONDecoder[T any](handler func(context.Context, T, *kafka.Message) error) kafka.MessageHandler {
    return func(ctx context.Context, msg *kafka.Message) error {
        var v T
        if err := json.Unmarshal(msg.Value, &v); err != nil {
            return err
        }
        return handler(ctx, v, msg)
    }
}
```
