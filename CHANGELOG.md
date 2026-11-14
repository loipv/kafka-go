# Changelog

## v1.2.0 (2026-11-13)

> **v1.2.0 contains breaking API changes despite the minor version number.**
> The library has no known users yet, so the module path stays
> `github.com/loipv/kafka-go` and there is no `/v2`. If you are upgrading from
> v1.0.0, apply the [v1.1.0 migration](#migration) first (v1.1.0 was never
> tagged), then [Migrating from 1.1](#migrating-from-11). One correction to the
> 1.1.0 notes below: `ConsumerWithAutoCommit(false)` does not disable the offset
> store. The library still stores the offsets of parked messages and commits them
> when partitions are revoked; only the periodic automatic commit is off, so the
> application must call `Commit` to commit at other times.

v1.2.0 trims the API to what a user calls, drops the DLQ retry machinery and the
circuit breaker, makes the consumer mode follow the registered handler, and
fixes a set of defects found in a second audit (several of them security
related). Delivery stays at-least-once.

### Removed

- **DLQ auto-retry and the final DLQ**: `ConsumerWithDLQRetry`, `DLQRetryConfig`
  (and `ConsumerConfig.DLQRetry`), and the DLQ retry consumer behind them,
  together with the `Reprocess*` metrics. A failed message is produced to the DLQ
  once; reprocessing DLQ messages is the application's job (consume the DLQ
  topic with a regular `Consumer`).
- **Circuit breaker**: `CircuitBreakerConfig` (and `DLQConfig.CircuitBreaker`),
  `CircuitBreaker` / `NewCircuitBreaker` and its methods, `CircuitState` with
  `CircuitClosed` / `CircuitOpen` / `CircuitHalfOpen`, `Consumer.CircuitState`
  and `Consumer.ResetCircuit`. A DLQ that cannot be written now blocks the
  affected partitions with escalating backoff instead.
- `DLQConfig.MaxRetries`, `DLQConfig.RetryDelay`,
  `DLQConfig.RetryBackoffMultiplier`, `DLQConfig.CircuitBreaker` and the `DefaultDLQMaxRetries`,
  `DefaultDLQRetryDelay`, `DefaultDLQBackoffMultiplier` constants: a DLQ produce
  is a single attempt.
- `ConsumerWithBatchProcessing` and `ConsumerWithGroupByKey` (and the
  `ConsumerConfig.BatchProcessing` / `GroupByKey` fields): the registered
  handler decides the mode.
- `Producer.ProduceMultiTopicBatch` and `TopicBatch`: call `ProduceBatch` once
  per topic.
- `ProducerWithRetry` and `ProducerConfig.Retry`. `RetryConfig` is now
  consumer-only.
- `HealthChecker.CheckBrokers` (use `Check`), `HealthChecker.SetTimeout` (use the
  context deadline) and `NewHealthCheckerFromProducer` (use `NewHealthChecker`).
- `PartitionAny`: the producer always partitions by key and ignores
  `Message.Partition`.
- `ErrDLQClosed`: it could never reach a caller (the consumer only logs it), so
  it is unexported.
- Exported internals, which are now unexported:
  - types `DLQService`, `DLQMetricsCollector` (with `NewDLQMetricsCollector` and
    its methods), `IdempotencyStore` (with `NewIdempotencyStore`, `Add`,
    `IsDuplicate`, `Remove`, `Size`, `Close`) and `TracingService` (with
    `NewTracingService`, `StartProducerSpan`, `StartConsumerSpan`,
    `StartBatchConsumerSpan`, `InjectTraceContext`, `ExtractTraceContext`);
  - constants `DefaultConnectionTimeout`, `DefaultRequestTimeout`,
    `DefaultSessionTimeout`, `DefaultHeartbeatInterval`, `DefaultRebalanceTimeout`,
    `DefaultBatchSize`, `DefaultBatchTimeout`, `DefaultIdempotencyTTL`,
    `DefaultAutoCommitInterval`, `DefaultRetryMaxRetries`,
    `DefaultRetryInitialInterval`, `DefaultRetryMultiplier`,
    `DefaultRetryMaxInterval` (the defaults are documented on each option) and the
    span attribute keys `MessagingSystemKey`, `MessagingDestinationNameKey`,
    `MessagingDestinationPartitionID`, `MessagingOperationNameKey`,
    `MessagingOperationTypeKey`, `MessagingKafkaOffsetKey`,
    `MessagingConsumerGroupKey`, `MessagingKafkaMessageKeyKey`,
    `MessagingBatchMessageCountKey`.

### Changed

- `Produce`, `ProduceBatch`, `ProduceAsync` and `Flush` may now be called
  concurrently with `Producer.Close` (it was forbidden: a call could reach the
  client while `Close` destroyed it). A call that loses the race returns
  `ErrProducerClosed`, and a `Produce` or `ProduceBatch` still waiting for a
  delivery report when `Close` ends returns an error wrapping
  `ErrProducerClosed` instead of waiting for its context.
- **The consumer mode follows the registered handler.** `OnMessage`, `OnBatch`
  and `OnGroupedBatch` select single-message, batch and key-grouped delivery; one
  handler is active and a later registration replaces the earlier one.
  `ConsumerWithBatchSize` and `ConsumerWithBatchTimeout` still size the batches.
- **`Consumer.DLQMetrics()` is now `Consumer.Metrics()`** and returns a flat
  `Metrics` (`HandlerFailures`, `SentToDLQ`, `ByTopic map[string]TopicMetrics`,
  `BlockedPartitions`) instead of `DLQMetrics{Global, ByTopic, BlockedPartitions}`.
  `Global.HandlerRetries` became `HandlerFailures` (same counting for
  single-message handlers: every failed attempt, including the first; the batch
  counting is described under Fixed), `Global.MessagesSentToDLQ` became
  `SentToDLQ`, and the per-topic `HandlerRetries` became `HandlerFailures`; the
  JSON keys changed with them (see the migration table).
- `SentToDLQ` per-topic metrics are keyed by the **source topic** (the topic the
  message was consumed from), not the DLQ topic.
- `Compression` is a string type (`"none"`, `"gzip"`, `"snappy"`, `"lz4"`,
  `"zstd"`) instead of an `int`; the constant names are unchanged.
- `RebalanceEvent.Type` is a `RebalanceType`, with the constants
  `RebalanceAssigned` and `RebalanceRevoked`.
- Health checks are bounded by the context deadline: a deadline longer than 10s is
  no longer capped at 10s, and without a deadline each broker query gets a 10s
  default. `CheckConsumerLag` describes only the requested group instead of
  listing every group on the cluster.
- A DLQ produce is a **single attempt**, confirmed by its delivery report and
  bounded by a 30s delivery timeout. The timeout comes from the DLQ connection's
  raw config under `message.timeout.ms` (alias `delivery.timeout.ms`): the
  consumer's `ConsumerWithRawConfig` by default, `DLQConfig.Raw` when
  `DLQConfig.Brokers` points at a separate cluster. The wait
  runs on the poll loop, so the derived `max.poll.interval.ms` now covers it:
  max(`ConsumerWithRebalanceTimeout`, (retry budget + DLQ delivery timeout) x 1.5,
  300s).
- A failed batch is sent to the DLQ in one pass (all messages queued, then one
  delivery report awaited per message) instead of one round trip per message.
- `Commit()` with nothing to commit returns `nil` instead of librdkafka's "no
  offset stored" error.
- `Start` returns `ErrConsumerClosed` after `Close`, and `Commit` returns it once
  the consumer has been closed.
- `NewConsumer` validates more: it returns the new sentinel errors
  `ErrDLQTopicRequired` (a `DLQConfig` without a topic), `ErrInvalidBatchSize`
  and `ErrInvalidBatchTimeout` (non-positive batch settings).
- Tracing no longer records the message key as a span attribute unless
  `TracingConfig.IncludeMessageKey` is set (see Security).
- `NewConsumer` returns an error naming the cause (the retry budget, the DLQ
  delivery timeout or `ConsumerWithRebalanceTimeout`) when `max.poll.interval.ms`
  would exceed librdkafka's 24-hour limit, instead of librdkafka's bare range
  error. The value is not clamped.
- `NewConsumer` logs a warning when `DLQConfig.SSL`, `SASL` or `Raw` is set
  without `DLQConfig.Brokers`: those fields are ignored then, and the DLQ uses
  the consumer's connection. The warning names the fields, never their values.
- Contributors only: source and test files were reorganised (one concern per
  file, `x_test.go` per `x.go`), CI enforces an 80% coverage floor and doc
  comments on every export, and `CLAUDE.md` documents the conventions.

### Fixed

- The DLQ producer closed silently with messages still queued, and an unread
  error event from an earlier broker outage made that close wait its whole
  10-second flush timeout with nothing to deliver. `Consumer.Close` now logs a
  warning with the number of DLQ messages left undelivered, and stale events no
  longer delay it.
- `Consumer.Close` destroyed the client under a consume loop that was still
  running after its 5-second wait (a handler that had not returned, with the
  `Start` context not cancelled), which could crash the process. `Close` now
  returns `ErrCloseTimeout` in that case and the loop releases the DLQ
  producer, the idempotency store and the client when `Start` returns; the
  final batch flush is skipped and those messages are redelivered.
- `Producer.Close` could stall for the full flush timeout and lose async delivery
  failures, because it stopped the delivery-report reader before flushing. It
  now flushes first and logs a warning with the number of messages it gave up on
  if the flush times out.
- `ProduceBatch` failed messages with "Queue full" when a batch was larger than
  the client queue. `Produce` and `ProduceBatch` now apply back-pressure: they
  wait for in-flight deliveries to drain a full queue. A message that can never
  fit still fails with the queue-full error.
- `ConsumerWithIdempotencyKey` was ignored in batch mode. Duplicates (across
  batches and within one batch) are now removed before the batch handler runs
  and are parked with the batch.
- A user `Resume()` bypassed the backoff of a blocked partition and resumed it
  early. A blocked partition now stays paused until its own wait elapses.
- `Start` spun forever, logging, after a fatal client error; it now returns an
  error wrapping the client error. Call `Close` afterwards.
- Failed batch handler invocations were missing from `HandlerFailures`; they are
  now counted, one per message of the failed batch.
- `CheckConsumerLag` ignored context cancellation between partitions and kept
  querying; it now stops with a DOWN result.
- A DLQ delivery event that was not a message report was counted as a success;
  the message is now treated as unconfirmed (its partition blocks and retries).
  `Produce` and `ProduceBatch` likewise report such an event as an error.
- A graceful shutdown was treated as a handler failure. When the `Start` context
  was cancelled while a handler or a retry wait was in flight (or `Close`
  interrupted a retry wait), the error handler received `context canceled` and,
  by returning `nil`, parked a message that was never processed (in batch mode
  the whole batch), so its offset was committed and the message lost; with a
  DLQ, healthy messages were written to the DLQ and also redelivered. Such a
  message or batch is now left unstored and redelivered (also when `Start` is
  called again on the same consumer), the error handler and the DLQ are not
  invoked, and a handler error returned after the context ended is not counted
  in `HandlerFailures`.
- `Producer.Flush` after `Close` crashed the process (a segmentation fault in
  librdkafka); it now returns `ErrProducerClosed`.
- `HealthChecker.CheckTopic` asked the broker to auto-create the topic it
  probed, so a missing topic was created and reported UP. The health checker
  never auto-creates topics now; a missing topic is DOWN.
- `Close` without cancelling the `Start` context could leave `Start` blocked
  forever when a DLQ delivery wait was in flight and the DLQ was unreachable.
- `Check` and `CheckTopic` could pass librdkafka a non-positive timeout when the
  context deadline was about to expire; they now report DOWN with the deadline
  error.

### Security

- The message key is no longer recorded in producer and consumer spans by
  default (keys may contain personal data); opt in with
  `TracingConfig.IncludeMessageKey`.
- The `x-dlq-error-message` header (`DLQConfig.IncludeErrorInfo`) is capped at
  1024 bytes, cut on a rune boundary.
- `NewProducer`, `NewConsumer` and `NewHealthChecker` log a warning when SASL is
  configured without TLS (credentials would travel unencrypted); the warning
  never includes the username or password. The same applies to a DLQ cluster
  configured through `DLQConfig`. A raw `security.protocol` is matched in any
  case (`SASL_PLAINTEXT` warns too).
- The SASL password is redacted by `SASLConfig` when printed with `%v`, `%+v` or
  `%#v`, when logged through `slog`'s text or JSON handler, and in
  `encoding/json` output (new `String`, `GoString`, `LogValue` and `MarshalJSON`
  methods), also when the `SASLConfig` or a pointer to it is nested in another
  struct such as `DLQConfig`. JSON output is redacted, so a config cannot be
  round-tripped through JSON with its password; decoding still reads it.
- Idempotency keys are no longer written to the debug log; the duplicate-skip
  line now logs topic, partition and offset.
- CI runs with least-privilege permissions (`contents: read`) and a `govulncheck`
  job (`make vuln` runs the same check locally).

### Added

- `TracingConfig.IncludeMessageKey`, `Metrics`, `TopicMetrics`, `RebalanceType`,
  `RebalanceAssigned`, `RebalanceRevoked`, `ErrDLQTopicRequired`,
  `ErrInvalidBatchSize` and `ErrInvalidBatchTimeout` (described above).
- `ErrCloseTimeout`, returned by `Consumer.Close` (see Fixed).
- Doc comments on every exported identifier, compiled `Example` functions for
  the producer, async producer, consumer, batch and grouped-batch consumers,
  `Commit` and the health checker, and a README rewritten to match.
- A `LICENSE` file (MIT).

### Migrating from 1.1

Every "new" form below compiles against the final v1.2.0 API.

| 1.1 | 1.2 |
|---|---|
| `ConsumerWithBatchProcessing(true)` + `OnBatch(h)` | `OnBatch(h)`; keep `ConsumerWithBatchSize` / `ConsumerWithBatchTimeout` |
| `ConsumerWithBatchProcessing(true)` + `OnMessage(h)` (messages handled one by one inside batches) | delete the option; `OnMessage(h)` is plain single-message handling, the batch size and timeout no longer apply |
| `ConsumerWithBatchProcessing(true)` + `ConsumerWithGroupByKey(true)` + grouped handler | `OnGroupedBatch(h)` |
| `c.DLQMetrics()` | `c.Metrics()` |
| `m.Global.HandlerRetries` | `m.HandlerFailures` (renamed; single-message counting is unchanged, failed batch handler invocations are now counted, one per message of the batch) |
| `m.Global.MessagesSentToDLQ` | `m.SentToDLQ` |
| `m.ByTopic[t].HandlerRetries` | `m.ByTopic[t].HandlerFailures` |
| `m.ByTopic[t].SentToDLQ` | `m.ByTopic[t].SentToDLQ`; `t` is now the source topic, not the DLQ topic |
| `m.BlockedPartitions` | unchanged |
| `m.Global.Reprocess*`, `m.ByTopic[t].ReprocessAttempts` | removed with DLQ retry |
| types `DLQMetrics`, `DLQGlobalMetrics`, `DLQTopicMetrics` | `Metrics`, `TopicMetrics` |
| metrics JSON `{"global":{"handlerRetries":..,"messagesSentToDlq":..},"byTopic":{"t":{"handlerRetries":..}}}` | `{"handlerFailures":..,"sentToDlq":..,"byTopic":{"t":{"handlerFailures":..,"sentToDlq":..}}}` |
| `kafka.CompressionGZIP` etc. | no change: the constant names are the same. Only code that used the numeric value (`Compression(1)`, `int(c)`, numbers read from config) must switch to the constant or the codec name (`kafka.Compression("gzip")`) |
| `event.Type == "assigned"` | still compiles; prefer `event.Type == kafka.RebalanceAssigned` / `kafka.RebalanceRevoked`. Passing `event.Type` where a `string` is expected needs `string(event.Type)` |
| `hc.SetTimeout(d)` | `ctx, cancel := context.WithTimeout(ctx, d)` and pass `ctx` to `Check`, `CheckTopic`, `CheckConsumerLag` |
| `hc.CheckBrokers(ctx)` | `hc.Check(ctx)` |
| `NewHealthCheckerFromProducer(p)` | `NewHealthChecker(kafka.ProducerWithBrokers(...), ...)` with the same connection options; it owns its connection, so `defer hc.Close()` |
| `ProducerWithRetry(&RetryConfig{...})` | `ProducerWithRawConfig(map[string]any{"retries": 5, "retry.backoff.ms": 100, "retry.backoff.max.ms": 1000})`, or `ProducerWithIdempotent(true)` |
| `DLQConfig{MaxRetries, RetryDelay, RetryBackoffMultiplier}` | delete the fields; the DLQ produce is one attempt, a failure blocks the partition and is retried with the consumer's `RetryConfig` backoff. To change the 30s delivery timeout set `message.timeout.ms` (or `delivery.timeout.ms`) in `ConsumerWithRawConfig`, or in `DLQConfig.Raw` when `DLQConfig.Brokers` is set |
| `ConsumerWithDLQRetry(&DLQRetryConfig{...})` and the final DLQ | delete; to reprocess DLQ messages run a separate `Consumer` on the DLQ topic with your own policy |
| `CircuitBreakerConfig`, `c.CircuitState(t)`, `c.ResetCircuit(t)` | delete; watch `c.Metrics().BlockedPartitions` and the `partition blocked` log lines instead |
| `ProduceMultiTopicBatch(ctx, []TopicBatch{{Topic: t, Messages: ms}})` | loop: `p.ProduceBatch(ctx, t, ms)` per topic (combine with `errors.Join` if you want all errors) |
| `PartitionAny` | delete; the partition is chosen from the key |
| span attribute `messaging.kafka.message.key` present by default | set `TracingConfig{Enabled: true, IncludeMessageKey: true}` to keep it |
| `DefaultBatchSize`, `Default*`, `Messaging*Key` constants, `NewIdempotencyStore`, `NewTracingService`, `NewDLQMetricsCollector` | removed; use the option defaults documented on each `ConsumerWith*` / `ProducerWith*` and the attribute names `"messaging.kafka.offset"`, `"messaging.kafka.consumer.group"`, ... as strings |
| `Start` blocking forever after a fatal client error | `Start` returns the error; call `Close` afterwards |

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
- Minimum Go 1.26.11 (was 1.24.10): it is the lowest release in which every
  standard-library vulnerability `govulncheck` reported for this module is fixed.

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
