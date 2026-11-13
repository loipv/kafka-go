# kafka-go

kafka-go is a Go library for producing to and consuming from Apache Kafka. It is built on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) (a cgo wrapper around librdkafka) and adds a handler-based consumer with at-least-once delivery: an offset is stored only after a message has been dealt with, and a message that cannot be dealt with blocks its partition instead of being skipped.

Two terms are used throughout. A message is **parked** when it has been dealt with durably, so its offset may be stored (see [Delivery guarantees](#delivery-guarantees)). A partition is **blocked** when its next message cannot be parked: it is paused and retried after a growing wait.

## Contents

- [Features](#features)
- [Installation](#installation)
- [Project structure](#project-structure)
- [Quick start](#quick-start)
- [Producer](#producer)
- [Consumer](#consumer)
- [Delivery guarantees](#delivery-guarantees)
- [Dead Letter Queue](#dead-letter-queue)
- [Idempotency](#idempotency)
- [Configuration reference](#configuration-reference)
- [Security](#security)
- [Observability](#observability)
- [Error handling](#error-handling)
- [Typed handlers](#typed-handlers)
- [Examples](#examples)
- [Testing and development](#testing-and-development)
- [Known limitations](#known-limitations)
- [License](#license)

## Features

- Synchronous (`Produce`, `ProduceBatch`) and asynchronous (`ProduceAsync`) producing.
- Handler-based consumer in three modes: single message, batch, and key-grouped batch.
- At-least-once delivery: offsets are stored only for parked messages, and unparkable messages block their partition.
- Dead Letter Queue, optionally on a separate cluster.
- In-memory idempotency filter (best effort, one process).
- OpenTelemetry tracing.
- Health checks for the cluster, a topic and consumer group lag.
- Structured logging with `log/slog`.
- Raw librdkafka configuration pass-through for settings the library does not model.

## Installation

```bash
go get github.com/loipv/kafka-go
```

Requirements:

- Go 1.24.10 or newer (the `go` directive in `go.mod`).
- cgo: build with `CGO_ENABLED=1` and a C toolchain. confluent-kafka-go bundles a statically linked librdkafka for common platforms, so a separate librdkafka install is normally not needed.

## Project structure

```
kafka-go/
├── kafka/                       # the library (package kafka)
│   ├── doc.go                   # package documentation and Version
│   ├── types.go                 # Message, Headers, GroupedBatch, handler types, enums
│   ├── errors.go                # sentinel errors
│   ├── config.go                # connection/auth config shared by every client; SASLConfig, TracingConfig
│   ├── producer.go              # Producer
│   ├── producer_options.go      # ProducerConfig and ProducerWith* options
│   ├── consumer.go              # Consumer: poll loop, batching, Commit, rebalance
│   ├── consumer_options.go      # ConsumerConfig, DLQConfig, RetryConfig and ConsumerWith* options
│   ├── block.go                 # partition blocking and backoff
│   ├── retry.go                 # handler retries
│   ├── dlq.go                   # Dead Letter Queue producer
│   ├── idempotency.go           # in-memory idempotency store
│   ├── metrics.go               # handler-failure and DLQ counters
│   ├── tracing.go               # OpenTelemetry spans and header propagation
│   ├── health.go                # HealthChecker
│   └── *_test.go                # unit, mock-broker and integration tests
├── examples/                    # runnable programs (see Examples)
├── Makefile
└── .github/workflows/ci.yml
```

## Quick start

Produce a message and wait for the broker to acknowledge it:

```go
producer, err := kafka.NewProducer(
	kafka.ProducerWithBrokers("localhost:9092"),
	kafka.ProducerWithClientID("my-app"),
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

err = producer.Produce(ctx, "orders", &kafka.Message{
	Key:   []byte("order-1"),
	Value: []byte(`{"id":"order-1"}`),
})
if err != nil {
	log.Printf("produce: %v", err)
}
```

Consume single messages until SIGINT or SIGTERM. Register the handler before `Start`, and always call `Close` after `Start` returns:

```go
package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/loipv/kafka-go/kafka"
)

func main() {
	consumer, err := kafka.NewConsumer(
		kafka.ConsumerWithBrokers("localhost:9092"),
		kafka.ConsumerWithGroupID("my-group"),
		kafka.ConsumerWithTopics("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
		log.Printf("%s[%d]@%d: %s", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		return nil // nil parks the message; an error starts retry, DLQ or block
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := consumer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("start: %v", err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := consumer.Close(closeCtx); err != nil {
		log.Printf("close: %v", err)
	}
}
```

Consume in batches instead. The mode follows the registered handler:

```go
consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("localhost:9092"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithBatchSize(100),
	kafka.ConsumerWithBatchTimeout(2*time.Second),
)
if err != nil {
	log.Fatal(err)
}

consumer.OnBatch(func(ctx context.Context, msgs []*kafka.Message) error {
	log.Printf("inserting %d orders", len(msgs))
	return nil // for example one bulk insert; return its error to fail the batch
})

if err := consumer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
	log.Printf("start: %v", err)
}
closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := consumer.Close(closeCtx); err != nil {
	log.Printf("close: %v", err)
}
```

## Producer

`NewProducer` applies the defaults in the [configuration reference](#producer-options), then your options in order. It does not wait for a broker connection: unreachable brokers show up as delivery errors on the first produce.

| Method | Behaviour |
|---|---|
| `Produce(ctx, topic, msg)` | Sends one message and blocks until its delivery report arrives. Returns the delivery error if the broker did not acknowledge it. If `ctx` ends first it returns the context's error, and the message may still be delivered afterwards. |
| `ProduceBatch(ctx, topic, msgs)` | Sends all messages and blocks until every report has arrived. Returns `nil` for an empty batch or when all were acknowledged, otherwise the `errors.Join` of every failure (use `errors.Is` / `errors.As`). The joined errors carry only topic and partition, so an error does not say which messages failed: treat each message's delivery as unknown (some may have been delivered). |
| `ProduceAsync(topic, msg)` | Queues the message and returns at once. A `nil` return only means "queued". Delivery failures go to the handler set with `ProducerWithDeliveryErrorHandler`, or are logged at error level when there is none. |
| `Flush(timeout)` | Waits up to `timeout` for every queued message, `ProduceAsync` ones included. Returns an error stating how many are still queued, or `nil`. After `Close` it returns `ErrProducerClosed`. |
| `Close()` | Flushes for up to 10 seconds, logs a warning with the number of messages still undelivered (they are lost), then closes. Safe to call more than once; always returns `nil`. |

The message's `Partition`, `Offset` and `Topic` fields are ignored when producing: the `topic` argument decides the topic, and librdkafka picks the partition from the key. A zero `Timestamp` lets librdkafka use the current time.

```go
producer, err := kafka.NewProducer(
	kafka.ProducerWithBrokers("localhost:9092"),
	kafka.ProducerWithDeliveryErrorHandler(func(msg *kafka.Message, err error) {
		log.Printf("delivery to %s failed: %v", msg.Topic, err)
	}),
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

err = producer.ProduceAsync("orders", &kafka.Message{Value: []byte(`{"status":"created"}`)})
if err != nil {
	log.Printf("queue message: %v", err) // the local queue is full: retry or drop
}
if err := producer.Flush(10 * time.Second); err != nil {
	log.Printf("flush: %v", err)
}
```

Things to know:

- **Back-pressure.** When librdkafka's local queue is full, `Produce` and `ProduceBatch` wait for in-flight delivery reports to drain it and retry. A message that can never fit (for example one larger than `queue.buffering.max.kbytes`) fails at once with a queue-full error. `ProduceAsync` has no back-pressure: it returns the queue-full error immediately and the caller decides what to do.
- **Close.** Do not call a produce method concurrently with `Close`; stop the producing goroutines first. `Produce`, `ProduceBatch`, `ProduceAsync` and `Flush` are otherwise safe for concurrent use. After `Close`, the produce methods and `Flush` return `ErrProducerClosed`.
- **The delivery error handler** runs on the producer's single delivery-report goroutine. A slow handler delays later reports, and it must not call `Close`.
- **Retries** belong to librdkafka. Tune them, and the delivery timeout, through `ProducerWithRawConfig`:

  ```go
  producer, err := kafka.NewProducer(
  	kafka.ProducerWithBrokers("localhost:9092"),
  	kafka.ProducerWithRawConfig(map[string]any{
  		"retries":            5,
  		"retry.backoff.ms":   200,
  		"message.timeout.ms": 60000,
  	}),
  )
  if err != nil {
  	log.Fatal(err)
  }
  defer producer.Close()
  ```

  `ProducerWithRequestTimeout` is the broker request timeout, not the delivery timeout. `ProducerWithIdempotent(true)` enables librdkafka's idempotent producer, which prevents duplicates and reordering caused by producer-side retries and requires `AcksAll`.

## Consumer

Create a consumer with `NewConsumer`, register one handler, run it with `Start`, and release it with `Close`. `NewConsumer` validates the options and does not connect; connecting and subscribing happen in `Start`.

### Handlers

| Registration | Handler type | Called with |
|---|---|---|
| `OnMessage` | `MessageHandler` | One message at a time. A failure is retried in place (see [Handler retries](#handler-retries)). |
| `OnBatch` | `BatchHandler` | A batch (`[]*Message`) in consumption order, possibly from several partitions. |
| `OnGroupedBatch` | `GroupedBatchHandler` | A batch grouped by `string(Key)` as `[]GroupedBatch`, groups in first-seen order and messages in consumption order within each group. |

- The mode follows the registered handler. Only one handler is active: a later `OnMessage`, `OnBatch` or `OnGroupedBatch` call replaces the earlier one.
- Register before `Start`. Without a handler `Start` returns `ErrNoHandler`. The `On*` methods are not synchronised.
- A batch is flushed when it reaches `ConsumerWithBatchSize` messages, on a ticker of `ConsumerWithBatchTimeout`, and in `Close`. Returning `nil` parks the whole batch; returning an error fails the whole batch.
- All handlers, including the error handler, run on the goroutine that called `Start`. The one exception is the final batch flush inside `Close`, which runs on the goroutine that called `Close`.

```go
consumer.OnGroupedBatch(func(ctx context.Context, groups []kafka.GroupedBatch) error {
	for _, g := range groups {
		log.Printf("customer %s: %d orders", g.Key, len(g.Messages))
	}
	return nil
})
```

### Start and shutdown

`Start` blocks. It returns:

- the context's error when `ctx` is cancelled;
- `nil` when `Close` was called;
- an error wrapping the client error when librdkafka reports a fatal one;
- `ErrNoHandler`, `ErrConsumerClosed` or `ErrConsumerRunning`, or an error when the subscription fails.

`Close` is still required after `Start` returns, whatever the reason: `Start` does not release the client or flush the last partial batch. Shut down in this order:

1. Cancel the context passed to `Start` (the signal handler in the quick start does this) and wait for `Start` to return.
2. Call `Close(ctx)`. It waits up to 5 seconds for the poll loop to leave, flushes the batch still in memory (batch modes), closes the DLQ producer and the idempotency store, and closes the client. `ctx` bounds only that final flush, so give it a deadline (create the context just before calling `Close`, as the quick start does) and do not pass an already-cancelled context.

If `Close` is called while a long retry is still in flight and the `Start` context was not cancelled, `Close` falls back to its 5-second wait and may close the client under that retry. `Close` is safe to call more than once; later calls return `nil`.

Shutdown is not a handler failure. A message or batch whose handler, or retry wait, is in flight when the `Start` context is cancelled (or whose retry wait `Close` interrupts) is neither parked nor blocked: its offset is not stored, the error handler and the DLQ are not invoked, and it is redelivered when the group next consumes the partition. The same holds for the final batch flush in `Close` when its `ctx` ends and the batch handler then fails. Let handlers honour their `ctx` so shutdown is prompt.

### Pause and Resume

`Pause` stops fetching from the assigned partitions. The poll loop keeps polling, so the consumer stays in its group and a partial batch is still flushed on its timeout. The pause survives rebalances, and calling it before `Start` takes effect once partitions are assigned. `Resume` restarts fetching.

A blocked partition keeps waiting out its backoff across a `Resume`: it resumes on its own when the wait elapses, and is not resumed while the consumer is paused. `Pause`, `Resume`, `Metrics` and `Commit` may be called from any goroutine.

### Commit

`Commit(msgs...)` commits synchronously. With messages it stores and commits the offset after the highest-offset message per topic and partition. With no arguments it commits every offset stored so far (the offsets of parked messages) and returns `nil` when there is nothing to commit. It returns `ErrConsumerClosed` once the consumer has been closed. Messages must come from this consumer.

With auto commit on (the default) calling it is optional, for committing sooner than the interval. With `ConsumerWithAutoCommit(false)` nothing is committed in the background and the handler must commit:

```go
consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("localhost:9092"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithAutoCommit(false),
)
if err != nil {
	log.Fatal(err)
}

consumer.OnMessage(func(ctx context.Context, msg *kafka.Message) error {
	// ... write to the database first ...
	return consumer.Commit(msg)
})
```

### Rebalance callback

`ConsumerWithRebalanceCallback` is called with a `RebalanceEvent` whose `Type` is `RebalanceAssigned` or `RebalanceRevoked` and whose `Partitions` are the affected topic-partitions. It runs on whichever goroutine is polling the client (normally the one running `Start`, and the one running `Close` while the client shuts down), before the library applies the assignment, so keep it short. A non-nil error is logged and makes the library skip its own handling of that event. `TopicPartition.Offset` in an event is whatever librdkafka reports, often a logical value such as "unset", not a message position.

```go
consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("localhost:9092"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithRebalanceCallback(func(event kafka.RebalanceEvent) error {
		switch event.Type {
		case kafka.RebalanceAssigned:
			log.Printf("assigned %d partitions", len(event.Partitions))
		case kafka.RebalanceRevoked:
			log.Printf("revoked %d partitions", len(event.Partitions)) // flush per-partition state here
		}
		return nil
	}),
)
if err != nil {
	log.Fatal(err)
}
// Register a handler, then Start and Close as in the quick start.
_ = consumer
```

`ConsumerWithPartitionAssignor` selects the assignment strategy: `AssignorRange` (default), `AssignorRoundRobin` or `AssignorCooperativeSticky`. All members of a group should use the same one.

## Delivery guarantees

The consumer is at-least-once: a message may be delivered more than once, and is not lost unless you opt in to that. `enable.auto.offset.store` is always `false`, so librdkafka never stores an offset on its own.

### When an offset is stored

An offset is stored only when a message is **parked**. A message is parked when:

1. the handler returned `nil`;
2. it was written to the DLQ and the produce was confirmed;
3. the error handler claimed it by returning `nil`;
4. it was skipped as an idempotency duplicate;
5. it was dropped under `RetryConfig.SkipOnMaxRetries` (explicit opt-in, see below).

Stored offsets are committed by auto commit (the default) or by `Commit`. With auto commit off, the library also commits them when partitions are revoked.

A message whose handling is interrupted by shutdown is neither parked nor blocked: its offset is not stored and it is redelivered (see [Start and shutdown](#start-and-shutdown)).

### What blocks a partition

When a failed message is not claimed by the error handler and is not confirmed on the DLQ (there is no DLQ, or the DLQ produce failed), it cannot be parked, and its partition is blocked: the library pauses the partition, seeks back to the failed message, and retries after a wait. The first wait is `RetryConfig.InitialInterval`; it doubles on each consecutive block of the same message up to `RetryConfig.MaxInterval` (defaults 1s and 30s), and resets when the message finally parks. The wait between redeliveries of a blocked partition does not hold up the poll loop: other partitions keep flowing while it runs, and `max.poll.interval.ms` is not exceeded by it. (Handler retries and the DLQ delivery wait are different; see [Handler retries](#handler-retries).) A revoked partition drops its blocked state. `Consumer.Metrics().BlockedPartitions` lists the blocked partitions.

In batch modes the whole batch fails together: if any message of the batch cannot be parked, nothing is stored and the batch is redelivered wholesale. Use [idempotency](#idempotency) or an idempotent handler for the duplicates this causes.

### Handler retries

`ConsumerWithRetry` takes a `RetryConfig`. A zero or negative field uses its default, so retries cannot be disabled by setting a field to 0. Without `ConsumerWithRetry` the same defaults apply.

| Field | Default | Meaning |
|---|---|---|
| `MaxRetries` | 3 | Retries after the first attempt: the handler is called at most `MaxRetries+1` times. |
| `InitialInterval` | 1s | Wait before the first retry, and the first partition-block wait. |
| `MaxInterval` | 30s | Cap on the retry wait and on the partition-block wait. |
| `Multiplier` | 2.0 | Growth factor of the retry wait. The partition-block wait always doubles. |
| `SkipOnMaxRetries` | false | Drop a message after its retries are exhausted instead of blocking. |

Retries apply to single-message handlers only. A batch handler is not retried in place: a failed batch goes straight to the error handler and the DLQ, or blocks and is redelivered whole. For a batch handler only `InitialInterval` and `MaxInterval` are used, as the partition-block wait.

Retry waits run on the poll loop. While a message is being retried, no partition of this consumer is consumed, and the same holds while a message waits for its DLQ confirmation. With the defaults the retry waits add up to about 7 seconds (1s + 2s + 4s), and the DLQ wait is bounded by 30 seconds, so one failing message can hold up the whole consumer for up to about 37 seconds. With `MaxRetries: 10` and `MaxInterval: 30s` the retry waits alone add up to about 3 minutes (1s, 2s, 4s, 8s, 16s, then 30s for the remaining five) per message. This is why `max.poll.interval.ms` is derived from these waits (see [below](#maxpollintervalms)). Keep the retry budget small, and prefer the DLQ over long retries.

When a single-message handler has failed all its attempts, the message goes to the error handler (if set; `nil` claims it), then to the DLQ (if set; a confirmed produce parks it), and otherwise blocks its partition. A message whose attempts shutdown cut short goes to neither; it is redelivered (see [Start and shutdown](#start-and-shutdown)).

```go
consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("localhost:9092"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithRetry(&kafka.RetryConfig{
		MaxRetries:      5,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
	}),
	kafka.ConsumerWithErrorHandler(func(ctx context.Context, msg *kafka.Message, err error) error {
		log.Printf("giving up on %s[%d]@%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
		return err // nil would claim the message; an error defers to the DLQ or blocks
	}),
)
if err != nil {
	log.Fatal(err)
}
// Register a handler, then Start and Close as in the quick start.
_ = consumer
```

The error handler is called once per message that failed: in batch modes, once for each message of a failed batch. It is not called for a message or batch whose handling shutdown interrupted. Note that `err` can carry text derived from message content; see [Security](#security).

### SkipOnMaxRetries

`SkipOnMaxRetries` is the explicit opt-in to losing messages. For a single-message handler whose retries are exhausted, the message is offered to the error handler and then, best effort, to the DLQ, and is parked whether or not either took it. If the DLQ produce also fails, the message is lost. The error passed on wraps `ErrSkippedOnMaxRetries`. It has no effect on batch handlers.

### Auto commit off

With `ConsumerWithAutoCommit(false)` the handler must call `Commit`. The library still stores the offsets of parked messages and commits them when partitions are revoked in a rebalance.

### max.poll.interval.ms

The consumer must poll within `max.poll.interval.ms` or the group considers it failed. The library sets it to the largest of:

- `ConsumerWithRebalanceTimeout` (default 0, meaning unset);
- 300 seconds;
- 1.5 × (the sum of the handler retry waits + the DLQ delivery timeout), because those waits run on the poll loop.

With the defaults the 300-second floor applies. The estimate counts the waits only, not the time your handler runs, so raise `ConsumerWithRebalanceTimeout` if a handler or a batch handler can run longer. `NewConsumer` returns an error when `ConsumerWithSessionTimeout` is not below the resulting value. librdkafka caps `max.poll.interval.ms` at 24 hours (86400000 ms): when `ConsumerWithRebalanceTimeout` or the value derived from the retry waits and the DLQ timeout exceeds it, `NewConsumer` returns an error naming the cause instead of clamping.

## Dead Letter Queue

`ConsumerWithDLQ(&kafka.DLQConfig{...})` sends messages the consumer cannot process to another topic. Without a DLQ such a message blocks its partition. The library does not read the DLQ topic back; reprocessing is up to you.

| `DLQConfig` field | Meaning |
|---|---|
| `Topic` | Dead letter topic. Required: an empty value makes `NewConsumer` return `ErrDLQTopicRequired`. |
| `IncludeErrorInfo` | Adds the handler error text as the `x-dlq-error-message` header. Default false. |
| `Brokers`, `SSL`, `SASL`, `Raw` | Select a separate DLQ cluster (see below). |

How a message reaches the DLQ:

- The record keeps the original key, value and headers. The partition is chosen by librdkafka from the key.
- It gets these headers: `x-dlq-original-topic`, `x-dlq-timestamp` (RFC 3339), `x-dlq-handler-retry-count` (the number of times the handler was called: 1 for a batch handler) and, with `IncludeErrorInfo`, `x-dlq-error-message`.
- The DLQ produce is a single attempt with `acks=all`, confirmed by its delivery report and bounded by the DLQ producer's `message.timeout.ms`: 30 seconds unless the DLQ connection's raw config sets `message.timeout.ms` (`delivery.timeout.ms` is accepted as an alias). That is `ConsumerWithRawConfig` by default, and `DLQConfig.Raw` only when `DLQConfig.Brokers` points at a separate cluster.
- When the produce fails or times out, the message is not parked: its partition blocks and the message is retried later. A broken DLQ therefore blocks the partitions of the messages it could not take, and nothing is dropped. Each attempt holds up the poll loop for up to the delivery timeout. The one exception is `SkipOnMaxRetries`.
- When a batch fails, the messages the error handler did not claim are sent in one pass. If any of them fails, the whole batch blocks.
- Confirmed sends are counted in `Metrics.SentToDLQ`, keyed by the source topic.

`IncludeErrorInfo` caveats: the error text can contain sensitive data from the failed message, and it is truncated to 1024 bytes. Leave it off if your handler errors can echo payloads.

**Separate cluster.** With empty `Brokers` (the default) the DLQ producer inherits the consumer's connection: brokers, SSL, SASL and `Raw`, and the `SSL`, `SASL` and `Raw` fields of `DLQConfig` are ignored (`NewConsumer` logs a warning naming the ones that are set). With `Brokers` set, the DLQ producer uses only the `SSL`, `SASL` and `Raw` of `DLQConfig`; nothing is inherited, so the consumer's credentials never reach another cluster.

```go
consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("localhost:9092"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithDLQ(&kafka.DLQConfig{
		Topic:   "orders-dlq",
		Brokers: []string{"dlq-broker:9093"},
		SSL:     true,
		SASL:    &kafka.SASLConfig{Mechanism: "SCRAM-SHA-512", Username: "dlq-writer", Password: "secret"},
	}),
)
if err != nil {
	log.Fatal(err)
}
// Register a handler, then Start and Close as in the quick start.
_ = consumer
```

**Error handler semantics.** `ConsumerWithErrorHandler` runs before the DLQ. Returning `nil` means the handler took ownership: the message is parked and the DLQ is not used. Returning an error defers to the DLQ or, without one, blocks the partition. With `SkipOnMaxRetries` the message is parked whatever it returns.

## Idempotency

`ConsumerWithIdempotencyKey` takes a function that returns a key for a message. A message whose key was already processed successfully is skipped without calling the handler, and its offset advances. An empty key is never filtered. `ConsumerWithIdempotencyTTL` sets how long a key is remembered (default 1 hour; zero or less also selects 1 hour).

```go
consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("localhost:9092"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithIdempotencyKey(func(msg *kafka.Message) string {
		return string(msg.Headers["event-id"])
	}),
	kafka.ConsumerWithIdempotencyTTL(time.Hour),
)
if err != nil {
	log.Fatal(err)
}
// Register a handler, then Start and Close as in the quick start.
_ = consumer
```

This is a best-effort filter, not exactly-once processing. Its limits:

- The memory is in-process only. It is lost on restart and when a partition moves to another instance.
- It holds one entry per distinct key seen within one TTL, so memory grows with key volume times TTL.
- A key is recorded only after the handler succeeded. In batch modes duplicates are removed before the handler is called (including repeats inside the same batch), but the keys of a failed batch are not recorded. If that batch is redelivered, only messages whose keys earlier successful handling recorded are filtered; with a DLQ or an error handler that claims the messages, the batch is not redelivered at all.

Real duplicate protection belongs in the handler, for example a unique key in the database that makes a repeated write a no-op.

## Configuration reference

Options are applied in order, so a later option overrides an earlier one for the same setting. Each option's full behaviour is in its doc comment (`go doc github.com/loipv/kafka-go/kafka ConsumerWithRetry`).

### Producer options

| Option | Default | Description |
|---|---|---|
| `ProducerWithBrokers(brokers ...string)` | none (required) | Broker addresses (`bootstrap.servers`). `ErrBrokersRequired` when empty. |
| `ProducerWithClientID(id)` | empty | `client.id`. Empty means librdkafka's own default. |
| `ProducerWithConnectionTimeout(d)` | 10s | `socket.connection.setup.timeout.ms`. Zero or less leaves librdkafka's default. |
| `ProducerWithRequestTimeout(d)` | 30s | `request.timeout.ms`, how long the broker waits for the acknowledgments. Not the delivery timeout. Zero or less leaves librdkafka's default. |
| `ProducerWithSSL(enabled)` | false | TLS to the brokers. |
| `ProducerWithSASL(*SASLConfig)` | nil | SASL authentication. |
| `ProducerWithAcks(acks)` | `AcksAll` | `AcksNone`, `AcksLeader` or `AcksAll`. |
| `ProducerWithCompression(c)` | `CompressionNone` | `CompressionGZIP`, `CompressionSnappy`, `CompressionLZ4`, `CompressionZSTD`. An unsupported value is rejected by `NewProducer`. |
| `ProducerWithIdempotent(enabled)` | false | librdkafka's idempotent producer (`enable.idempotence`); requires `AcksAll`. Off leaves the key unset. |
| `ProducerWithDeliveryErrorHandler(func(*Message, error))` | nil | Sink for `ProduceAsync` delivery failures. Without one they are logged. |
| `ProducerWithTracing(*TracingConfig)` | nil (off) | OpenTelemetry tracing; needs `Enabled: true`. |
| `ProducerWithLogger(*slog.Logger)` | `slog.Default()` | Logger for the producer's diagnostics. |
| `ProducerWithRawConfig(map[string]any)` | nil | Extra librdkafka keys. |

`NewHealthChecker` takes the same `ProducerOption` type; see [Health checks](#health-checks).

### Consumer options

| Option | Default | Description |
|---|---|---|
| `ConsumerWithBrokers(brokers ...string)` | none (required) | Broker addresses. `ErrBrokersRequired` when empty. |
| `ConsumerWithGroupID(id)` | none (required) | `group.id`. `ErrGroupIDRequired` when empty. |
| `ConsumerWithTopics(topics ...string)` | none (required) | Topics to subscribe to. `ErrTopicsRequired` when empty. |
| `ConsumerWithSSL(enabled)` | false | TLS to the brokers. |
| `ConsumerWithSASL(*SASLConfig)` | nil | SASL authentication. |
| `ConsumerWithSessionTimeout(d)` | 30s | `session.timeout.ms`. Must be below the computed `max.poll.interval.ms`. Zero or less leaves librdkafka's default. |
| `ConsumerWithHeartbeatInterval(d)` | 3s | `heartbeat.interval.ms`. Zero or less leaves librdkafka's default. |
| `ConsumerWithRebalanceTimeout(d)` | 0 (computed) | Lower bound for `max.poll.interval.ms`, at most 24h; see [above](#maxpollintervalms). |
| `ConsumerWithBatchSize(n)` | 100 | Messages that trigger a batch flush. Must be above zero (`ErrInvalidBatchSize`). Batch handlers only. |
| `ConsumerWithBatchTimeout(d)` | 5s | Interval at which a partial batch is flushed. Must be above zero (`ErrInvalidBatchTimeout`). Batch handlers only. |
| `ConsumerWithIdempotencyKey(fn)` | nil (off) | Key extractor for de-duplication. |
| `ConsumerWithIdempotencyTTL(d)` | 1h | How long a processed key is remembered. |
| `ConsumerWithDLQ(*DLQConfig)` | nil (no DLQ) | Dead Letter Queue. |
| `ConsumerWithRetry(*RetryConfig)` | nil (3 retries, 1s to 30s, ×2) | Handler retry policy. |
| `ConsumerWithErrorHandler(fn)` | nil | Called for a message the consumer could not process. |
| `ConsumerWithAutoCommit(enabled)` | true | Background commits (`enable.auto.commit`). |
| `ConsumerWithAutoCommitInterval(d)` | 5s | `auto.commit.interval.ms`. Zero or less leaves librdkafka's default. |
| `ConsumerWithFromBeginning(enabled)` | false | Where a group with no committed offset starts: `true` earliest, `false` latest. |
| `ConsumerWithPartitionAssignor(a)` | `AssignorRange` | `AssignorRange`, `AssignorRoundRobin` or `AssignorCooperativeSticky`. |
| `ConsumerWithRebalanceCallback(fn)` | nil | Called on partition assignment and revocation. |
| `ConsumerWithTracing(*TracingConfig)` | nil (off) | OpenTelemetry tracing; needs `Enabled: true`. |
| `ConsumerWithLogger(*slog.Logger)` | `slog.Default()` | Logger for the consumer's diagnostics. |
| `ConsumerWithRawConfig(map[string]any)` | nil | Extra librdkafka keys. |

### Raw librdkafka config

`ProducerWithRawConfig` and `ConsumerWithRawConfig` merge extra librdkafka keys (for example `linger.ms`, `fetch.min.bytes`). Raw keys are merged into the connection and authentication keys (`bootstrap.servers`, `client.id` (producer only), `security.protocol`, `sasl.*`), which they may therefore override. The library then sets its own keys after the merge, and Raw cannot override these:

- Producer: `acks`; `request.timeout.ms` and `socket.connection.setup.timeout.ms` while their options are above zero; `compression.type` unless the compression is none; `enable.idempotence` when `ProducerWithIdempotent(true)`.
- Consumer: `group.id`, `auto.offset.reset`, `enable.auto.commit`, `enable.auto.offset.store` (always false) and `max.poll.interval.ms`; `session.timeout.ms`, `heartbeat.interval.ms`, `auto.commit.interval.ms` and `partition.assignment.strategy` while their options are above zero or non-empty (the default). Setting such an option to zero leaves the key to Raw.

## Security

### TLS / mTLS

`ProducerWithSSL(true)` and `ConsumerWithSSL(true)` switch the connection to TLS (`security.protocol=ssl`, or `sasl_ssl` together with SASL). The certificates are configured through the raw librdkafka keys:

| Key | Purpose |
|---|---|
| `ssl.ca.location` | CA certificate used to verify the brokers. |
| `ssl.certificate.location` | Client certificate (for mTLS). |
| `ssl.key.location` | Client private key (for mTLS). |
| `ssl.key.password` | Password of the private key, if it is encrypted. |

```go
sslKeys := map[string]any{
	"ssl.ca.location":          "/etc/kafka/ca.pem",
	"ssl.certificate.location": "/etc/kafka/client.pem",
	"ssl.key.location":         "/etc/kafka/client.key",
	"ssl.key.password":         os.Getenv("KAFKA_KEY_PASSWORD"),
}

consumer, err := kafka.NewConsumer(
	kafka.ConsumerWithBrokers("broker:9093"),
	kafka.ConsumerWithGroupID("my-group"),
	kafka.ConsumerWithTopics("orders"),
	kafka.ConsumerWithSSL(true),
	kafka.ConsumerWithRawConfig(sslKeys),
)
if err != nil {
	log.Fatal(err)
}
// Register a handler, then Start and Close as in the quick start.
_ = consumer
```

The DLQ producer on the same cluster inherits the consumer's SSL, SASL and Raw settings, and `NewHealthChecker` accepts the same `ProducerWith*` connection options. A DLQ on a separate cluster takes its own settings from `DLQConfig`.

### SASL

```go
producer, err := kafka.NewProducer(
	kafka.ProducerWithBrokers("broker:9093"),
	kafka.ProducerWithSSL(true),
	kafka.ProducerWithSASL(&kafka.SASLConfig{
		Mechanism: "SCRAM-SHA-512", // or "PLAIN", "SCRAM-SHA-256"
		Username:  "app",
		Password:  os.Getenv("KAFKA_PASSWORD"),
	}),
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

- Use SASL together with SSL. Without SSL the credentials travel unencrypted (`sasl_plaintext`), and `NewProducer`, `NewConsumer` and `NewHealthChecker` log a warning. The warning never contains the username or password.
- `SASLConfig` replaces the password with `[REDACTED]` for `%v`, `%+v` and `%#v`, in `slog`'s text and JSON handlers, and in `encoding/json` output, also when the `SASLConfig` (or a pointer to it) is nested in another struct such as `DLQConfig`. A config therefore cannot be round-tripped through JSON with its password; decoding JSON still reads it. `fmt` cannot call methods on unexported fields, so hold a `*SASLConfig`, not a value, in an unexported field of your own struct.

### What the library logs

The library never logs message keys, values or headers. Three places can still carry message-derived data: the `x-dlq-error-message` header (`IncludeErrorInfo`) and the `messaging.kafka.message.key` span attribute (`IncludeMessageKey`), both off by default; and, with tracing on, the text of handler errors, which is recorded on spans. Error values returned by your own handlers are also logged as they are, so avoid putting payloads in them.

## Observability

### Tracing

Pass a `TracingConfig` with `Enabled: true` to `ProducerWithTracing` or `ConsumerWithTracing`. Spans come from the global OpenTelemetry tracer provider, so configure the OpenTelemetry SDK in your application. The trace context travels in the message headers through the global propagator, with W3C Trace Context used when no propagator is set.

```go
producer, err := kafka.NewProducer(
	kafka.ProducerWithBrokers("localhost:9092"),
	kafka.ProducerWithTracing(&kafka.TracingConfig{Enabled: true, TracerName: "order-producer"}),
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

| `TracingConfig` field | Default | Meaning |
|---|---|---|
| `Enabled` | false | Tracing is off even when a config is passed. |
| `TracerName` | `github.com/loipv/kafka-go` | Instrumentation name. |
| `TracerVersion` | `kafka.Version` | Instrumentation version. |
| `IncludeMessageKey` | false | Records the key as `messaging.kafka.message.key`. Off because keys may hold personal data such as user ids or emails. |

Spans:

| Where | Span name (kind) | Attributes |
|---|---|---|
| `Produce`, `ProduceBatch` (one span per message; trace context injected into the headers) | `<topic> publish` (producer) | `messaging.system`, `messaging.destination.name`, `messaging.operation.name`, `messaging.operation.type` (`publish`) |
| Single-message consumer (one span per message, retries included) | `<group> <topic> process` (consumer) | `messaging.system`, `messaging.destination.name`, `messaging.destination.partition.id`, `messaging.operation.name`, `messaging.operation.type` (`process`), `messaging.kafka.offset`, `messaging.kafka.consumer.group` |
| Batch consumer (one span per batch) | `<group> <topic of the first message> process batch` (consumer) | `messaging.system`, `messaging.destination.name`, `messaging.operation.name`, `messaging.operation.type`, `messaging.kafka.consumer.group`, `messaging.batch.message_count` |

A consumer span is parented to the trace context found in the message headers; a batch span is parented to the first message's context and links to the others (link attribute `message.index`). Idempotency duplicates removed before the handler are not part of the span. A handler error is recorded on the span. `ProduceAsync` is not traced: it neither starts a span nor injects trace context.

### Metrics

`Consumer.Metrics()` returns a copy of the counters; call it again for fresh numbers. The library exposes no metrics endpoint, so publish the values to your own system.

| Field | Meaning |
|---|---|
| `HandlerFailures` | Failed handler invocations. A single-message handler counts every failed attempt, including the first; a failed batch counts once per message handed to the handler. Equals the sum of `ByTopic`. |
| `SentToDLQ` | Messages whose DLQ produce was confirmed. |
| `ByTopic` | Both counters per source topic (the topic the message was consumed from, not the DLQ topic). Topics with nothing to report may be absent. |
| `BlockedPartitions` | A snapshot, not a counter: the partitions blocked at the time of the call. |

```go
m := consumer.Metrics()
log.Printf("handler failures=%d, sent to DLQ=%d, blocked partitions=%d",
	m.HandlerFailures, m.SentToDLQ, len(m.BlockedPartitions))
for topic, tm := range m.ByTopic {
	log.Printf("%s: handler failures=%d, sent to DLQ=%d", topic, tm.HandlerFailures, tm.SentToDLQ)
}
```

### Logging

Both clients log through `log/slog` (default `slog.Default()`): rebalances, retries, blocked partitions, DLQ failures, async delivery failures. Pass your own logger, or silence the output:

```go
quiet := slog.New(slog.DiscardHandler)

producer, err := kafka.NewProducer(
	kafka.ProducerWithBrokers("localhost:9092"),
	kafka.ProducerWithLogger(quiet), // ConsumerWithLogger(quiet) for a consumer
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close()
```

### Health checks

`NewHealthChecker` takes `ProducerOption`s that describe the connection (`ProducerWithBrokers` is required) and owns one long-lived connection; release it with `Close`, and do not call `Close` while a check is running. Options that only shape produced messages have no effect on it.

| Method | UP when |
|---|---|
| `Check(ctx)` | The cluster answered a metadata request. Details: `brokers`, `topics`, `originatingId`. |
| `CheckTopic(ctx, topic)` | The topic's metadata is present without an error. Details: `topic`, `partitionCount`, `partitions`. The check never creates the topic, even when the broker auto-creates topics: a missing topic is DOWN. |
| `CheckConsumerLag(ctx, groupID, maxLag)` | The group exists and its total lag (high watermark minus committed offset, summed over partitions) is not above `maxLag`. Details: `groupId`, `state`, `memberCount`, `lag`, `maxLag`, `partitions`. |

Each check returns a `*HealthResult` (never `nil`) with `Status` (`HealthStatusUp` or `HealthStatusDown`), `Details` and `Error`; every failure is reported as DOWN, not as a Go error. The context's deadline bounds each check; without a deadline the metadata and watermark queries get a 10-second default each, while the group describe and offset listing in `CheckConsumerLag` use the admin client's own default timeouts. `HealthResult` marshals to JSON.

```go
h, err := kafka.NewHealthChecker(kafka.ProducerWithBrokers("localhost:9092"))
if err != nil {
	log.Fatal(err)
}
defer h.Close()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

res := h.Check(ctx)
fmt.Println("cluster:", res.Status, res.Error)

// DOWN when the group's total lag exceeds 1000 messages.
res = h.CheckConsumerLag(ctx, "my-group", 1000)
fmt.Println("lag:", res.Status, res.Error)
```

## Error handling

Sentinel errors are package variables; test for them with `errors.Is`.

| Error | Returned |
|---|---|
| `ErrBrokersRequired` | By `NewProducer`, `NewConsumer` and `NewHealthChecker` when no broker is configured. |
| `ErrGroupIDRequired` | By `NewConsumer` when the group id is empty. |
| `ErrTopicsRequired` | By `NewConsumer` when no topic is configured. |
| `ErrDLQTopicRequired` | By `NewConsumer` when a `DLQConfig` has an empty `Topic`. |
| `ErrInvalidBatchSize` | By `NewConsumer` when the batch size is not positive. |
| `ErrInvalidBatchTimeout` | By `NewConsumer` when the batch timeout is not positive. |
| `ErrNoHandler` | By `Consumer.Start` when no handler was registered. |
| `ErrConsumerRunning` | By `Consumer.Start` when the consumer is already running. |
| `ErrConsumerClosed` | By `Consumer.Start` after `Close`, and by `Consumer.Commit` once the consumer has been closed. |
| `ErrProducerClosed` | By `Produce`, `ProduceBatch`, `ProduceAsync` and `Flush` after the producer was closed. |
| `ErrSkippedOnMaxRetries` | Wraps the last handler error passed to the error handler and the DLQ when `SkipOnMaxRetries` drops a message. `Start` does not return it. |

`NewProducer` and `NewConsumer` also return an error when librdkafka rejects the resulting configuration (for example an unsupported compression codec). `NewConsumer` itself rejects a session timeout that is not below the computed `max.poll.interval.ms`, and a `max.poll.interval.ms` above librdkafka's 24-hour limit.

## Typed handlers

The library keeps `Message.Value` as `[]byte`. To decode into a type, wrap your handler in a generic function (a longer version with decoders for batch and grouped handlers is in `examples/typed_consumer`):

```go
type Order struct {
	ID string `json:"id"`
}

func jsonHandler[T any](h func(context.Context, T, *kafka.Message) error) kafka.MessageHandler {
	return func(ctx context.Context, msg *kafka.Message) error {
		var v T
		if err := json.Unmarshal(msg.Value, &v); err != nil {
			return fmt.Errorf("decode %s: %w", msg.Topic, err)
		}
		return h(ctx, v, msg)
	}
}

func register(consumer *kafka.Consumer) {
	consumer.OnMessage(jsonHandler(func(ctx context.Context, o Order, msg *kafka.Message) error {
		log.Printf("order %s", o.ID)
		return nil
	}))
}
```

A message that cannot be decoded fails like any other handler error: it is retried, then goes to the error handler or the DLQ, or blocks its partition.

## Examples

Each example needs a broker on `localhost:9092`.

| Command | Shows |
|---|---|
| `go run examples/producer/main.go` | Synchronous, batch and asynchronous producing with graceful shutdown. |
| `go run examples/consumer/main.go` | Batch consumption with a DLQ, idempotency and tracing. |
| `go run examples/grouped_consumer/main.go` | Key-grouped batch consumption. |
| `go run examples/typed_consumer/main.go` | Generic typed handlers with JSON decoding (`EXAMPLE=batch` or `EXAMPLE=grouped` for the other variants). |
| `go run examples/health/main.go` | HTTP endpoints for cluster, topic and consumer-lag health. |
| `go run examples/rebalance/main.go` | Per-partition state across rebalances with manual commits. |

`kafka/example_test.go` holds compiled `Example` functions that appear in the package documentation.

## Testing and development

There are three test tiers:

- **Unit**: pure logic, no broker.
- **Mock broker**: runs against librdkafka's built-in mock cluster (`NewMockCluster` in confluent-kafka-go), no Docker. Skipped under `-short`.
- **Integration**: build tag `integration`, uses testcontainers with Redpanda and needs Docker.

Make targets (they mirror CI):

| Target | Runs |
|---|---|
| `make build` | `go build ./...` |
| `make test` | `go test ./...` (unit and mock broker) |
| `make test-short` | `go test -short ./...` (unit only) |
| `make test-race` | `go test -race ./...` |
| `make test-integration` | `go test -tags=integration -timeout=15m ./kafka/...` (needs Docker) |
| `make vet` | `go vet ./...` and `go vet -tags=integration ./kafka/...` |
| `make lint` | `golangci-lint run` |
| `make cover` | Race tests with a coverage report |
| `make cover-check` | Race tests over `./kafka/...`; fails below 80% statement coverage (`COVER_MIN` overrides the floor) |
| `make vuln` | `govulncheck ./...` |
| `make tidy` | `go mod tidy` |

## Known limitations

- Idempotency is in-process only: it is lost on restart or when a partition moves to another instance.
- A batch handler is not retried in place; a failed batch is redelivered whole (or sent to the DLQ).
- Retry waits and the DLQ delivery wait run on the poll loop, so a message being retried or awaiting DLQ confirmation stops consumption of every partition of that consumer.
- `ProduceAsync` has no back-pressure and no trace span.
- Produce methods must not run concurrently with `Close`.
- `CheckConsumerLag` needs the brokers' consumer group admin APIs (describe group, list group offsets) and permission to use them.

## License

MIT
