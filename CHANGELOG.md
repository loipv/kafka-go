# Changelog

## v1.1.0 (unreleased)

v1.1.0 fixes all 33 confirmed defects from a full code review, deletes dead
features, renames the API to Kafka/Go convention, and makes delivery
**at-least-once by default**. It is breaking — the migration is one mechanical
pass, see [Migration](#migration).

### Removed

- The `Client` and `Consumer` **interfaces** — constructors now return the
  concrete `*Producer` / `*Consumer`; declare your own three-line interface if
  you need a seam.
- `MessagePool` family (`NewMessagePool`, `AcquireMessage`, `ReleaseMessage`).
- `WithBackPressureThreshold` / `WithMaxQueueSize` and their config fields and
  defaults. Replacement: `ConsumerWithRawConfig(map[string]any{...})` with
  librdkafka's own `queued.max.messages.kbytes` / `queued.min.messages` keys —
  back pressure is the broker client's job, and the deleted options never
  wired those keys anyway.
- `SendQueued` and its background flusher (→ `ProduceAsync`).
- `Logger` / `DefaultLogger` / `NoopLogger`, `LogLevel` and its five constants,
  `WithLogLevel` / `ConsumerWithLogLevel`
  (→ `ProducerWithLogger` / `ConsumerWithLogger(*slog.Logger)`).
- Deprecated aliases `WithConsumerBrokers`, `WithConsumerTracing`.
- `NewDLQService` (now unexported).
- `NewHealthCheckerWithBrokers`
  (→ `NewHealthChecker(opts ...ProducerOption)` / `NewHealthCheckerFromProducer`,
  now returning an error and requiring `Close()`).

### Renamed

Every entry is a pure identifier substitution — the Migration section below
ports all of them in one pass.

| Kind | Before | After |
|---|---|---|
| type | `KafkaClient` | `Producer` |
| type | `KafkaConsumer` | `Consumer` |
| type | `ClientConfig`, `ClientOption` | `ProducerConfig`, `ProducerOption` |
| type | `TopicMessages` | `TopicBatch` |
| ctor | `NewClient` | `NewProducer() (*Producer, error)` |
| producer | `Send`, `SendBatch`, `SendMultiTopicBatch` | `Produce`, `ProduceBatch`, `ProduceMultiTopicBatch` |
| producer | `SendQueued` | `ProduceAsync` (rewritten; see Removed) |
| consumer | `Handle`, `HandleBatch`, `HandleGroupedBatch` | `OnMessage`, `OnBatch`, `OnGroupedBatch` |
| consumer | `GetDLQMetrics`, `GetCircuitState` | `DLQMetrics()`, `CircuitState(topic)` |
| health | `NewHealthCheckerFromClient` | `NewHealthCheckerFromProducer` |
| tracing | `MessagingKafkaConsumerGroupKey` | `MessagingConsumerGroupKey` |
| options | every producer option | `ProducerWith*` — `ProducerWithBrokers`, `ProducerWithAcks`, … |
| options | every consumer option | `ConsumerWith*` — `ConsumerWithGroupID`, `ConsumerWithTopics`, `ConsumerWithRetry`, … |

### Changed signatures

- `Consumer` gains `Commit(msgs ...*Message) error` — explicit offset commit;
  zero args commits everything stored (offsets are stored only when messages
  are parked).
- `WithLogger` / `ConsumerWithLogger` take `*slog.Logger`.
- `ErrorHandler` becomes `func(ctx context.Context, msg *Message, err error) error`
  (was `func(err error, msg *Message)`) — `ctx` first, `error` last, and the
  return value lets the handler claim ownership of the message.
- `HealthResult.Error` is `string` (`json:"error,omitzero"`).
- `Default*` values are `const`.
- Minimum Go 1.24.10.

### Changed behavior (same signature)

The ones that will surprise people:

- **Delivery is now at-least-once.** Offsets are stored only for parked
  messages. A handler that keeps failing with no DLQ configured now **blocks
  its partition** instead of silently dropping the message and moving on. Set
  `SkipOnMaxRetries: true` to restore the old lossy behavior. Expect
  previously-invisible failures to become visible as blocked partitions and
  growing lag.
- **`SkipOnMaxRetries: true` no longer reports success.** The error handler
  now fires and the DLQ receives the message before the offset advances;
  previously both were bypassed entirely.
- **`Message.Partition` is ignored by the producer.** Partitioning is by `Key`.
- **`WithAutoCommit(false)` now also disables auto offset store; you must call
  `Commit()`.** Previously it committed only on revoke, and only if you had
  registered a rebalance callback.
- `Pause()`/`Resume()` pause the broker fetch instead of spin-sleeping; no
  more eviction while paused.
- `Start()` returns `ErrNoHandler` instead of silently discarding every
  message.
- `log_level` is no longer forwarded; librdkafka returns to its default
  verbosity (6).
- `WithRetry`, `WithRebalanceTimeout`, `RetryConfig.MaxInterval` now take
  effect (were no-ops). `RetryConfig.Multiplier` is consumer-only.
- `DLQConfig.MaxRetries/RetryDelay/RetryBackoffMultiplier` now retry the DLQ
  **produce**.
- DLQ headers are no longer written into the caller's `Message.Headers`.
- The DLQ retry consumer no longer auto-commits; failed reprocessing is
  retried, not lost.
- **All internal clients now inherit SSL/SASL.** On a secured cluster these
  previously failed silently — **expect newly-visible auth errors that were
  previously "DOWN" or dropped messages.**

### Added

- At-least-once offset model: `enable.auto.offset.store=false` always; offsets
  stored via `StoreOffsets` only when a message is parked. Unparkable messages
  pause + seek their partition and retry with escalating backoff (initial
  interval doubling to `MaxInterval`, default 30s); other partitions keep
  flowing. Blocked partitions are logged (WARN) and exposed as a gauge on
  `DLQMetrics().BlockedPartitions`.
- `ProducerWithDeliveryErrorHandler(func(msg *Message, err error))` — async
  delivery failures reach the caller instead of a log line.
- `ProducerWithRawConfig` / `ConsumerWithRawConfig(map[string]any)` — raw
  librdkafka config keys, merged last.
- Sentinel errors in `kafka/errors.go` (`ErrProducerClosed`,
  `ErrBrokersRequired`, `ErrNoHandler`, …) for programmatic assertions.
- `DLQConfig.Brokers` / `SSL` / `SASL` / `Raw` — send the DLQ to a separate
  cluster. Empty `Brokers` keeps today's behavior (DLQ shares the consumer's
  connection); when set, the DLQ producer and DLQ retry consumer use only these
  fields and inherit nothing from the consumer.
- Health checks compute real consumer lag via watermark offsets.
- Test suite: unit + mock-broker tests (`kafka.NewMockCluster`, no Docker) on
  every run, plus a build-tagged Redpanda integration suite; CI workflow
  (test / lint / integration / darwin), Makefile, `.golangci.yml`.

### Fixed

All 33 review findings, highlights:

- SSL/SASL is inherited by the DLQ producer, the DLQ retry consumer, and every
  health-check client (previously bootstrap-servers-only — silent message loss
  and permanent DOWN on secured clusters).
- The DLQ retry path no longer panics when only a batch/grouped handler is
  registered; `SetHeader` lazily allocates `Headers`.
- Manual commit works: offsets are committed after processing (not on read),
  `Commit(msgs...)` exists, the rebalance callback is always installed, and
  cooperative-sticky rebalancing uses `IncrementalAssign`/`IncrementalUnassign`.
- W3C trace-context propagation works without global otel setup.
- Producer delivery reports correlate spans by `Opaque` identity; `Close()`
  waits for the delivery-report goroutine (no close race).
- `NewConsumer`/`NewProducer` surface config errors; `session.timeout.ms` <
  `max.poll.interval.ms` is validated with a clear message.

### Dependencies

- OpenTelemetry bump v1.28 → v1.35. `otel` and `otel/trace` are direct
  requires (only `otel/metric` is indirect), so every user's otel moves to
  v1.35.
- `go 1.24.10` go.mod directive (toolchain line no longer needed).
- testcontainers-go v0.40.0 + `modules/redpanda` v0.40.0 — test-only, behind
  the `integration` build tag, not in the default build graph.

### Migration

Mechanical port, one pass:

```bash
gofmt -r 'NewClient -> NewProducer' -w ./...
gofmt -r 'SendBatch -> ProduceBatch' -w ./...
gofmt -r 'Send -> Produce' -w ./...
gofmt -r 'HandleBatch -> OnBatch' -w ./...
gofmt -r 'HandleGroupedBatch -> OnGroupedBatch' -w ./...
gofmt -r 'Handle -> OnMessage' -w ./...
gofmt -r 'GetDLQMetrics -> DLQMetrics' -w ./...
gofmt -r 'GetCircuitState -> CircuitState' -w ./...
gofmt -r 'KafkaClient -> Producer' -w ./...
gofmt -r 'KafkaConsumer -> Consumer' -w ./...
```

Then: prefix every producer option `ProducerWith*` and consumer option
`ConsumerWith*`; drop `Get`; replace the logger with `*slog.Logger`;
`ErrorHandler` closures gain `(ctx, msg, err) error`. Behavioral review
checklist: (1) consumers with failing handlers and no DLQ now block the
partition instead of dropping — set `SkipOnMaxRetries: true` to keep the old
behavior; (2) `ConsumerWithAutoCommit(false)` now requires calling `Commit`;
(3) internal clients inherit SSL/SASL — previously-silent failures become
visible errors.
