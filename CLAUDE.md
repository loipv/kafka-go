# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A production-ready Go library for Kafka producer and consumer functionality built on top of [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go). Provides enterprise-grade features including at-least-once delivery, intelligent batch processing, idempotency guarantees, key-based grouping, and a Dead Letter Queue with circuit breaker.

## Development Commands

Make targets mirror CI:

```bash
make build             # go build ./...
make test              # go test ./... (unit + mock-broker tiers)
make test-short        # go test -short ./... (unit only)
make test-race         # go test -race ./...
make test-integration  # go test -tags=integration ./kafka/... (needs Docker)
make vet               # go vet ./... && go vet -tags=integration ./kafka/...
make lint              # golangci-lint run
make cover             # race + coverage report
```

```bash
# Run a specific test
go test -v -run TestName ./kafka

# Run examples (need a local broker on localhost:9092)
go run examples/producer/main.go
go run examples/consumer/main.go
go run examples/grouped_consumer/main.go
go run examples/health/main.go
go run examples/rebalance/main.go
go run examples/typed_consumer/main.go
```

## Requirements

- Go 1.24+ (go.mod: `go 1.24.0`, `toolchain go1.24.10`)
- librdkafka (statically vendored by confluent-kafka-go for common platforms; builds need `CGO_ENABLED=1`)

## Architecture

### Core Components

All library code lives in the `kafka/` package. There are **no interfaces for the producer/consumer** — `NewProducer`/`NewConsumer` return the concrete `*Producer` (client.go) and `*Consumer` (consumer.go). A user who needs a seam declares their own interface on the consuming side. Because both our package and confluent's are named `kafka`, confluent's package is aliased **`ckafka`** throughout (`ckafka.Message`, `ckafka.ConfigMap`, ...).

- **Producer**: `client.go` - `Produce()`, `ProduceBatch()`, `ProduceMultiTopicBatch()` (synchronous, `errors.Join` for batches) and `ProduceAsync()` (delivery failures go to `ProducerWithDeliveryErrorHandler`, otherwise logged). `Close()` waits on the delivery-report goroutine.
- **Consumer**: `consumer.go` - Handler-based processing via `OnMessage`/`OnBatch`/`OnGroupedBatch`, dispatched through the `invokeHandler` seam. Also `Commit(msgs...)`, `Pause()`/`Resume()` (broker-fetch pause, reconciled in the poll loop), `DLQMetrics()`, `CircuitState(topic)`.
- **Types**: `types.go` - Core types (`Message`, `Headers`, `GroupedBatch`, `TopicBatch`, `TopicPartition`) and enums.
- **Config seam**: `config.go` - `connConfig` with a `configMap()` method; every internal client (producer, consumer, DLQ producer, DLQ retry consumer, health checks) builds its config through it, so SSL/SASL is inherited everywhere.
- **Options**: `options.go` (producer), `consumer_options.go` (consumer) - functional options; `ProducerWithRawConfig`/`ConsumerWithRawConfig` merge raw librdkafka keys last.
- **Errors**: `errors.go` - sentinel errors (`ErrBrokersRequired`, `ErrNoHandler`, ...); tests assert on these.

### Delivery Model (at-least-once)

`enable.auto.offset.store=false` always. An offset is stored (via `StoreOffsets`) only when a message is *parked*: handler succeeded, written to DLQ with the produce confirmed, `ErrorHandler` returned `nil`, or `SkipOnMaxRetries` parked it as unprocessable. Anything unparkable (no DLQ, DLQ send failed, circuit breaker open) **blocks its partition**: pause + seek + escalating backoff (`InitialInterval` doubling to `MaxInterval`, default 30s), reconciled in the same poll-loop pass as pause/resume so the loop never sleeps and `max.poll.interval.ms` is never blown. Batch mode fails the whole batch (re-delivered wholesale; use idempotency for the duplicates). `ConsumerWithAutoCommit(false)` also disables the offset store — the handler must call `Commit()` itself.

### Key Patterns

**Functional Options**: `NewProducer`/`NewConsumer` take variadic options:
```go
producer, err := kafka.NewProducer(
    kafka.ProducerWithBrokers("localhost:9092"),
    kafka.ProducerWithClientID("my-app"),
)
```

**Handler Registration**: Consumer handlers are registered before `Start()` (which validates one is set — `ErrNoHandler` otherwise):
- `OnMessage(MessageHandler)` - single messages
- `OnBatch(BatchHandler)` - batches
- `OnGroupedBatch(GroupedBatchHandler)` - key-grouped batches

### Configuration Naming Conventions

- Producer options: `ProducerWith*` (e.g., `ProducerWithBrokers`, `ProducerWithAcks`)
- Consumer options: `ConsumerWith*` (e.g., `ConsumerWithBrokers`, `ConsumerWithGroupID`, `ConsumerWithRetry`)
- Logging: `ProducerWithLogger`/`ConsumerWithLogger(*slog.Logger)`; default `slog.Default()`, off is `slog.New(slog.DiscardHandler)`

### Message Flow

1. **Producer**: Message -> `produceAndAwait()` -> confluent producer (span closure in `Opaque` for delivery-report correlation) -> delivery report -> confirmation
2. **Consumer**: confluent consumer -> `convertMessage()` -> idempotency check -> handler via `invokeHandler` (with retry) -> parked? store offset : block partition

### Concurrency

- One goroutine ever calls user handlers: the poll loop (batch flush is a ticker + non-blocking send on `flushCh`)
- `Producer` uses a mutex for state plus a `sync.WaitGroup` for the delivery-report goroutine
- `Consumer` uses `mu` for state and `batchMu` for batch assembly; `running` is an atomic CAS with a deferred reset

### Testing

Three tiers: unit (pure logic), mock broker (`kafka.NewMockCluster`, no Docker, skipped under `-short`), and integration (`//go:build integration`, testcontainers + Redpanda, needs Docker). Both `go vet` and lint run with the `integration` tag.

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
