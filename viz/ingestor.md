# Ingestor — How It Works

This document explains the **Kafka ingestor** path centered on [`ingestor_runner.go`](../glassflow-api/internal/service/ingestor_runner.go). A companion diagram lives in [`ingestor.svg`](ingestor.svg).

The ingestor is one role of the shared `glassflow-api` binary (`-role ingestor`). Each running instance handles **exactly one Kafka topic** and publishes validated events into **NATS JetStream** for downstream pipeline stages (dedup → join → sink → ClickHouse).

---

## Role in the pipeline

```
Kafka  →  [Ingestor]  →  NATS JetStream  →  [Dedup]  →  [Join]  →  [Sink]  →  ClickHouse
                ↓
           DLQ (failures)
```

The ingestor does **not** write to ClickHouse. It is the boundary between external Kafka and the internal GlassFlow messaging fabric.

---

## Layered architecture

| Layer | Package / file | Responsibility |
|-------|----------------|----------------|
| Entry | `cmd/glassflow/main.go` | Parse config, build runtime routing from env, construct runner, graceful shutdown |
| Runner | `internal/service/ingestor_runner.go` | Wire NATS publishers, schema, signals, samplers; start component in a goroutine |
| Component | `internal/component/ingestor.go` | Lifecycle wrapper (`Start` / `Stop` / `doneCh`) |
| Ingestor | `internal/ingestor/kafka.go` | Compose Kafka consumer + message processor |
| Processor | `internal/ingestor/processor.go` | Validate, route subjects, publish (sync or async), DLQ, backpressure |
| Consumer | `internal/kafka/consumer.go` | franz-go poll loop, batching, manual offset commits |
| Publish | `internal/stream/publisher.go` | JetStream sync/async publish with ack futures |

`IngestorRunner` is the **orchestration shell**; the hot path lives in `KafkaMsgProcessor` and `kafka.Consumer`.

---

## 1. Process startup (`mainIngestor`)

When the binary starts with `-role ingestor`:

1. **Pipeline config** is loaded from `PIPELINE_CONFIG` (JSON blob, typically from PostgreSQL via the orchestrator).
2. **Topic name** comes from `INGESTOR_TOPIC` — must match one entry in `pipelineCfg.Ingestor.KafkaTopics`.
3. **Runtime NATS routing** is resolved from environment variables in `getIngestorRuntimeConfigFromEnv()`:

   | Variable | Purpose |
   |----------|---------|
   | `NATS_SUBJECT_PREFIX` | Base prefix for output subjects |
   | `GLASSFLOW_POD_INDEX` | Replica index → `OutputSubject = prefix.{index}` |
   | `NATS_SUBJECT_TOTAL_COUNT` | Optional; if &gt; 1, round-robin across `prefix.{0..N-1}` |
   | `NATS_SUBJECT_COUNT` | Required when topic dedup is enabled; number of dedup shard subjects |

4. `service.NewIngestorRunner(...)` is created and passed to `runWithGracefulShutdown()`.

### Graceful shutdown

`runWithGracefulShutdown` runs `runner.Start(ctx)` in a background goroutine and blocks on:

- **Startup error** from `Start` → process exits with error
- **`runner.Done()`** → component goroutine finished (crash path)
- **`ctx.Done()`** (SIGTERM) → `runner.Shutdown()` then exit cleanly

---

## 2. `IngestorRunner.Start` — wiring dependencies

`IngestorRunner.Start` is synchronous for setup; the actual Kafka loop runs asynchronously.

### Configuration resolution

1. **`getTopicConfig()`** — finds `models.KafkaTopicsConfig` by `topicName` in `pipelineCfg.Ingestor.KafkaTopics`.
2. **Validates runtime config**:
   - `OutputSubject` must be non-empty (set by orchestrator / env resolution).
   - If `topicCfg.Deduplication.Enabled`, requires `DedupSubjectPrefix` and `DedupSubjectCount > 0`.

### Publishers and side channels

| Dependency | Subject / target | Purpose |
|------------|------------------|---------|
| `streamPublisher` | `runtimeCfg.OutputSubject` (per-message subject may override) | Main pipeline output |
| `dlqStreamPublisher` | `models.GetDLQStreamSubjectName(pipelineID)` | Failed records |
| `signalPublisher` | Component signals (NATS) | Pipeline alerts (e.g. incompatible schema) |
| `schema` | PostgreSQL + optional Schema Registry | Validate payload; extract dedup key field |

Optional **Confluent Schema Registry** client is created when `topicCfg.SchemaRegistryConfig.URL` is set. That switches `schema_v2.Schema` into **external** mode (wire-format messages with 5-byte header).

### Component start

```text
component.NewIngestorComponent(...)
  → ingestor.NewKafkaIngestor(...)   // only KafkaIngestorType today
  → go component.Start(ctx, errChan)
```

`startStreamSamplers()` runs in parallel (see §7).

On shutdown, `IngestorRunner.Shutdown()` cancels samplers and calls `component.Stop(WithNoWait(true))`, which stops the Kafka consumer.

---

## 3. Kafka consumption loop

`KafkaIngestor.Start` delegates to `kafka.Consumer.Start(ctx, processor)`.

### Poll / batch invariant

The consumer maintains `c.batch []*kgo.Record`:

1. **While `len(c.batch) > 0`**, it only calls `processBatch` — it does **not** poll again.
2. This prevents franz-go’s polled cursor from advancing past records the processor has not finished with.
3. On empty batch, `PollFetches` with timeout appends records, then `processBatch`.

Manual commits (`DisableAutoCommit`):

- **Success**: `CommitUncommittedOffsets` after the whole batch completes processing.
- **Failure**: if `ProcessBatch` returns `lastProcessed != nil`, commits offset **up to that record** (partial progress). On context cancellation, commit uses a fresh bounded timeout so shutdown can still persist partial offsets.

---

## 4. Per-record processing (`prepareMessage`)

For each `kgo.Record`, `KafkaMsgProcessor.prepareMesssage` (sic in code) runs:

### Step A — Schema validation

`schema.Validate(ctx, msg.Value)` returns a **version ID string**.

| Outcome | Behavior |
|---------|----------|
| `IsIncompatibleSchemaError` or `ErrSchemaNotFound` | Send `ComponentSignal` to operators; return **fatal error** (batch fails, no DLQ for this record) |
| Other validation errors (parse, too short, etc.) | `pushMsgToDLQ` → return `nil` nats.Msg (treated as **processed** for offset purposes) |
| Success | Continue |

### Step B — Payload for NATS

- **External schema**: strip first 5 bytes (magic + schema ID) before publishing body.
- **Internal schema**: publish raw `msg.Value`.

### Step C — Subject and dedup header

`getSubjectAndDedupKey()`:

**Without dedup** (`topic.Deduplication.Enabled == false`):

- `getSubject()` returns `OutputSubject`, or round-robin `OutputSubjectPrefix.{i}` when `TotalSubjectCount > 1`.

**With dedup**:

1. `schema.Get(ctx, version, topic.Deduplication.ID, msgData)` extracts the dedup key field.
2. Subject = `DedupSubjectPrefix.(fnv64(key) % DedupSubjectCount)` — stable sharding to dedup replicas.
3. If `DedupSubjectCount == 1`, subject is the single `OutputSubject`.
4. Sets NATS header `Nats-Msg-Id` to the stringified dedup key (downstream NATS-native dedup).

Also sets `SchemaVersionIDHeader` on the NATS message.

---

## 5. Publishing modes

Controlled by `internal.DefaultProcessorMode` (default: **`async`**).

### Sync mode (`processBatchSync`)

For each record in order:

1. `prepareMessage`
2. `publisher.PublishNatsMsg(ctx, natsMsg, stream.WithUntilAck())` — retries until ack or error
3. Publish failure (non-recoverable) → DLQ, record still counted processed

Simple and strictly sequential; no cross-record backpressure batching.

### Async mode (`processBatchAsync`) — default

Uses an **internal retry loop** per Kafka batch with state:

| Field | Meaning |
|-------|---------|
| `cachedMsgs` | Prepared `*nats.Msg` per index (prepare once) |
| `completed` | Record fully handled (acked or DLQ at prepare) |
| `backpressure` | Indices waiting for NATS capacity |
| `dlqOnExit` | Indices to DLQ on fatal shutdown |
| `cursor` | Next fresh index to publish |
| `lastAckedIdx` | Highest **contiguous** completed prefix |

Each iteration:

1. Re-publish `backpressure` slice (async futures).
2. Publish from `cursor` until throttle or end of batch.
3. `WaitForAsyncPublishAcks()` → `classifyFutures`.
4. `advanceLastAckedIdx` — only contiguous prefix counts for Kafka `lastProcessed`.
5. Exit when all done; else exponential backoff (`IngestorBackpressureInitialDelay` → max).

**Backpressure** (`stream.IsBackpressureErr`): NATS/JetStream is full. Record stays uncommitted at Kafka (or only partial prefix committed). Metrics: `bpStart` / `bpStop` episode histograms.

**Fatal errors**: non-backpressure publish/ack failures → `cleanupAndReturn` → `drainToDLQ` for `dlqOnExit` (and backpressure carry on fatal, not on ctx cancel).

**Context cancellation**: only `dlqOnExit` records go to DLQ; backpressure carry is left in Kafka for re-consumption (downstream dedup handles at-least-once duplicates from records that acked past a gap).

`pendingPublishesLimit` caps in-flight async publishes: `min(PublisherMaxPendingAcks, NATSMaxBufferedMsgs / topic.Replicas)`.

---

## 6. Dead letter queue

`pushMsgToDLQ` wraps the original Kafka bytes in `models.DLQMessage` (role=ingestor, error text, payload) and publishes to the pipeline DLQ subject.

Reasons include:

- Parse / validation errors (non-incompatible)
- Dedup key extraction failure
- Unrecoverable publish failures
- Async cleanup after fatal batch errors

Observability: `RecordDLQWrite` with reason labels.

---

## 7. Stream samplers (observability only)

`startStreamSamplers` does **not** affect data path correctness.

1. `ingestorOutputSubjects(runtimeCfg)` lists every subject the ingestor may publish to (dedup shards, multi-subject prefix, or single `OutputSubject`).
2. For each subject, `JetStream.StreamNameBySubject` discovers the bound stream (local vs K8s topology differs).
3. One `stream.StreamSampler` goroutine per unique stream polls `StreamInfo` on an interval and emits depth gauges.

Failures are logged and skipped — sampling never crashes the ingestor.

---

## 8. Multi-replica deployment model

One ingestor **process** = one **Kafka topic** (consumer group member).

The orchestrator (Docker local or Kubernetes) sets per-replica env so that:

- **Without dedup**: each replica may publish to `prefix.{podIndex}` or round-robin across `NATS_SUBJECT_TOTAL_COUNT` subjects.
- **With dedup**: all replicas share the same `DedupSubjectPrefix` and `NATS_SUBJECT_COUNT`; hashing routes each event to the correct dedup shard.

`IngestorRunner` creates the main `streamPublisher` with `PublisherConfig{Subject: outputSubject}` — the **per-message** `nats.Msg.Subject` from `getSubjectAndDedupKey` is what JetStream actually receives; the publisher’s configured subject is the default/fallback for simple `Publish()` calls (DLQ uses its own publisher).

---

## 9. Key invariants

1. **At-least-once from Kafka**: offsets advance only after contiguous successful processing (or DLQ-at-prepare for that record).
2. **No poll past unprocessed batch**: protects offset correctness under async retries.
3. **Incompatible schema stops the batch** (operator signal), not silent DLQ — intentional operational visibility.
4. **Dedup routing is consistent**: same key → same subject index → same dedup replica.
5. **Backpressure preserves Kafka records**: throttle does not commit past the blocked contiguous prefix.

---

## 10. Related reading

- Pipeline-wide context: [`architecture.md`](architecture.md)
- Local dev: [`DEVELOPMENT.md`](DEVELOPMENT.md)
- E2E scenarios: `glassflow-api/tests/features/ingestor/`, `tests/steps/ingestor_steps.go`

---

## Quick reference — `IngestorRunner` start sequence

```text
mainIngestor
  └─ NewIngestorRunner.Start
       ├─ getTopicConfig()
       ├─ validate runtime (output + dedup subjects)
       ├─ NewNATSPublisher(outputSubject)
       ├─ NewNATSPublisher(dlqSubject)
       ├─ componentsignals.NewPublisher
       ├─ schemav2.NewSchema(pipelineID, topicID, db, srClient?)
       ├─ component.NewIngestorComponent → KafkaIngestor + KafkaMsgProcessor
       ├─ startStreamSamplers()
       └─ go IngestorComponent.Start → KafkaIngestor.Start → Consumer.consumeLoop
```
