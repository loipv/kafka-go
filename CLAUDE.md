# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Project Overview

A Go library (`github.com/loipv/kafka-go`, package `kafka`) for Kafka producers and consumers, built on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) v2. It provides at-least-once delivery with partition blocking, a Dead Letter Queue (DLQ), an in-memory idempotency filter, OpenTelemetry tracing and health checks. `README.md` is the user documentation; this file is for people changing the code.

## Development Commands

Make targets mirror CI:

```bash
make build             # go build ./...
make test              # go test ./... (unit + mock-broker tiers)
make test-short        # go test -short ./... (unit only; mock-broker tests skip)
make test-race         # go test -race ./...
make test-integration  # go test -tags=integration ./kafka/... (needs Docker)
make vet               # go vet ./... && go vet -tags=integration ./kafka/...
make lint              # golangci-lint run (revive, errorlint, copyloopvar, nilerr; build tag integration)
make cover             # race + coverage report
make cover-check       # race + coverage; fails below 80% (COVER_MIN) over ./kafka/...
make vuln              # govulncheck ./... (needs network)
make tidy              # go mod tidy
```

```bash
# Run a specific test
go test -v -run TestName ./kafka

# Run examples (need a local broker on localhost:9092)
go run examples/producer/main.go     # also: consumer, grouped_consumer, health, rebalance, typed_consumer
```

## Requirements

- Go 1.26.11+ (see `go.mod`)
- librdkafka (statically vendored by confluent-kafka-go for common platforms; builds need `CGO_ENABLED=1`, the Makefile sets it)

## Architecture

All library code lives in `kafka/`. There are **no interfaces for the producer/consumer**: `NewProducer`/`NewConsumer` return the concrete `*Producer` and `*Consumer`; a user who needs a seam declares their own interface. Confluent's package is aliased **`ckafka`** everywhere (both packages are named `kafka`).

### Core Components (one concern per file)

- `doc.go` - package doc and `Version`.
- `types.go` - `Message`, `Headers`, `GroupedBatch`, `TopicPartition`, `Metrics`/`TopicMetrics`, handler function types, `RebalanceEvent`/`RebalanceType`, enums (`Acks`, `Compression`, `PartitionAssignor`, `HealthStatus`).
- `errors.go` - sentinel errors; tests assert on them.
- `config.go` - `connConfig` with `configMap()` (every internal client builds its librdkafka config through it, so SSL/SASL is inherited everywhere), `buildProducerConfig`/`buildConsumerConfig` (derives `max.poll.interval.ms`), `SASLConfig` (password redacted when printed, logged or JSON-encoded), `TracingConfig`, `warnPlaintextSASL`, `warnIgnoredDLQConn`.
- `producer.go` - `Producer`: `Produce`, `ProduceBatch` (synchronous, back-pressure when the client queue is full, `errors.Join` of failures), `ProduceAsync` (failures go to `ProducerWithDeliveryErrorHandler`, else logged), `Flush`, `Close`.
- `producer_options.go` - `ProducerConfig` and `ProducerWith*` options.
- `consumer.go` - `Consumer`: `OnMessage`/`OnBatch`/`OnGroupedBatch`, `Start`, `Close`, `Commit`, `Pause`/`Resume`, `Metrics`, the poll loop, batch assembly, rebalance callback.
- `consumer_options.go` - `ConsumerConfig`, `DLQConfig`, `RetryConfig` and `ConsumerWith*` options.
- `retry.go` - in-place retry of the single-message handler and `retryBudget` (feeds `max.poll.interval.ms`).
- `block.go` - blocked-partition bookkeeping: pause, seek back, escalating backoff, resume.
- `dlq.go` - `dlqService`: one DLQ produce per message, confirmed by its delivery report; `x-dlq-*` headers.
- `idempotency.go` - in-memory key store with TTL.
- `metrics.go` - `metricsCollector` behind `Consumer.Metrics()`.
- `health.go` - `HealthChecker`: `Check`, `CheckTopic`, `CheckConsumerLag`.
- `tracing.go` - OpenTelemetry spans and trace-context propagation through message headers.

### Delivery Model (at-least-once)

`enable.auto.offset.store=false` always. An offset is stored (via `StoreOffsets`) only when a message is *parked*: the handler succeeded, the DLQ produce was confirmed, the `ErrorHandler` returned `nil`, it was an idempotency duplicate, or `SkipOnMaxRetries` dropped it (the explicit opt-in to loss). Anything unparkable (no DLQ, DLQ produce failed) **blocks its partition**: pause, seek back, retry after an escalating wait (`InitialInterval` doubling to `MaxInterval`, default 30s). Other partitions keep flowing, and the wait itself never sleeps the poll loop: it is reconciled each pass of the loop along with pause/resume. Shutdown is a third outcome (`outcomeInterrupted`): when a handler returns an error after the `Start` context ended, or a retry wait is cut short by that context or by `Close` (`errInterrupted`), the message or batch is neither parked nor blocked: nothing is stored, the `ErrorHandler` and DLQ are skipped, the partition is seeked back (`seekBack`), and it is redelivered, also when `Start` is called again on the same `Consumer`. A handler that returns `nil` after the context ended is parked as usual.

Not everything is non-blocking. Single-message handler retries and the DLQ delivery wait run **on the poll loop** and stop consumption of every partition while they last. `max.poll.interval.ms` is derived to cover them: max(explicit `ConsumerWithRebalanceTimeout`, (retry budget + DLQ delivery timeout) x 1.5, 300s); above librdkafka's 24h limit `buildConsumerConfig` returns an error naming the cause rather than clamping. The DLQ delivery timeout is 30s unless the DLQ connection's raw config sets `message.timeout.ms` (or its alias `delivery.timeout.ms`): that is `ConsumerWithRawConfig` by default, and `DLQConfig.Raw` only when `DLQConfig.Brokers` points at a separate cluster (`dlqConn()` in `config.go`).

The consumer's mode follows the registered handler (one active handler; there are no batch flags). In batch mode a batch handler is not retried in place. A failed batch is offered to the `ErrorHandler` message by message, its unclaimed messages go to the DLQ in one pass, and otherwise the whole batch blocks and is redelivered. Idempotency keys are recorded only for successful batches, so a redelivered failed batch is filtered only for keys that earlier successful handling recorded; handlers must tolerate duplicates. On revoke, `dropPendingFor` removes the revoked partitions' messages from the pending batch (unstored, so the next owner gets them) before `dropBlockedFor` clears their block state.

`ConsumerWithAutoCommit(false)` turns off only the automatic commit: parked offsets are still stored, the handler must call `Commit()` itself, and the library still commits stored offsets when partitions are revoked.

### Message Flow

1. **Producer**: `Message` -> `prepareMessage` (span closure in `Opaque` for delivery-report correlation) -> `produceAndAwait` -> delivery report -> result.
2. **Consumer**: `ReadMessage` -> `convertMessage` -> idempotency check -> handler (with retry in single-message mode) -> parked: store offset / unparkable: block partition / interrupted by shutdown: seek back, store nothing.

### Concurrency

- User handlers (message, batch, grouped batch, `ErrorHandler`) run on the goroutine that called `Start`, the only one that polls. The exception is the final batch flush in `Close`, which runs on the goroutine calling `Close` after the consume loop exited. If the loop is still running after `Close`'s wait (`closeLoopWait`, 5s), `Close` returns `ErrCloseTimeout` and sets `teardownByLoop` under `mu`; the loop then runs `teardown` itself in `loopDone`, without the final flush. The client is never destroyed under a running loop.
- `batchFlushLoop` (batch mode only) is a ticker that does a non-blocking send on `flushCh`; the poll loop does the flush. Rebalance callbacks run on whichever goroutine is polling the client. The `ProducerWithDeliveryErrorHandler` handler runs on the producer's delivery-report goroutine (`handleDeliveryReports`), so it must not block for long.
- `Pause`, `Resume`, `Metrics` and `Commit` are safe from any goroutine; `On*` registration is not synchronised and must precede `Start`.
- `Producer` runs one delivery-report goroutine (`handleDeliveryReports`), waited on in `Close`. `Producer.mu` guards the client's lifetime: every use of the client outside `Close` goes through `acquire()` (read lock plus the `closed` check) and `Close` flips `closed` under the write lock, so produce methods may race `Close`. The lock is never held while waiting for a delivery report. The idempotency store runs one cleanup goroutine, stopped by its `close`.
- State flags are `atomic.Bool` (`running` is a CAS with a deferred reset); `mu` guards `loopExited` and `teardownByLoop` and makes two steps atomic (`Start` claiming a run and publishing its `loopExited`; `Close` flipping `closed` and sampling the run), `batchMu` the batch, `blockMu` the blocked map.

## Key Patterns

**Functional options**: `NewProducer`/`NewConsumer` take variadic options:
```go
producer, err := kafka.NewProducer(
    kafka.ProducerWithBrokers("localhost:9092"),
    kafka.ProducerWithClientID("my-app"),
)
```

**Handler registration** (before `Start`, which returns `ErrNoHandler` otherwise): `OnMessage(MessageHandler)`, `OnBatch(BatchHandler)`, `OnGroupedBatch(GroupedBatchHandler)`. A later registration replaces the earlier one.

**Typed handlers**: the library keeps `Message.Value` as `[]byte`. For typed decoding, wrap handlers (see `examples/typed_consumer/`):

```go
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

## Testing

Three tiers: unit (pure logic), mock broker (`ckafka.NewMockCluster`, no Docker, skipped under `-short`) and integration (`//go:build integration`, testcontainers + Redpanda, needs Docker). Both `go vet` and lint run with the `integration` tag. Shared helpers are in `testhelpers_test.go` (`skipIfShort`, `newMockCluster`, `uniqueTopic`, `waitFor`, `fastAtLeastOnce`, ...). Runnable documentation lives in `example_test.go`.

## Conventions

- One concern per source file. Tests for `x.go` live in `x_test.go`; the only other test files are `testhelpers_test.go`, `integration_test.go` and `example_test.go`. Declaration-only files need no test file.
- Test names are `Test<Object>_<Behaviour>`. Mock-broker tests start with `skipIfShort(t)`.
- Export only what a user calls; internal types are unexported.
- Options are named `ProducerWith*` / `ConsumerWith*` and their doc comment states the default. Every exported identifier has a doc comment (lint enforces it).
- No `get` prefix. Receivers are one letter (`p`, `c`, `h`, `s`).
- Sentinel errors live in `errors.go` and are wrapped with `%w`.
- Logging is structured `slog`. Never log a message's key, value or headers, or anything derived from them.
- Comments explain why and never cite task or ticket numbers.
- State flags are `atomic.Bool`.
- A commit either moves code or changes behaviour, never both. Bug fixes come with a test that failed before the fix.
- Before committing, run `make build vet lint` and `make cover-check`.
