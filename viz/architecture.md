# Architecture Guide

A module-by-module walkthrough of the codebase for new contributors.

## Overview

The entire project ships as **one binary** (`glassflow-api`) that selects its
role at runtime via a `-role` flag. Each role runs as a separate container in
production but shares the same compiled code.

```
Kafka → [Ingestor] → NATS → [Dedup] → NATS → [Join] → NATS → [Sink] → ClickHouse
                                                                    ↓
                                                              Dead Letter Queue
```

---

## 1. Entry Point — `cmd/glassflow/main.go`

`main()` parses the `-role` flag then routes to the matching handler:

```
main()
  ├── parse -role flag
  ├── load env config  (envconfig.Process)
  ├── setup observability (logs, metrics, traces)
  ├── connect NATS + PostgreSQL
  └── switch role:
        "sink"          → mainSink()
        "join"          → mainJoin()
        "ingestor"      → mainIngestor()
        "dedup"         → mainDeduplicatorV2()
        "" (no flag)    → mainEtl()   ← orchestrator: creates/manages pipelines
```

Every `mainXxx()` follows the same pattern:
1. Load `PipelineConfig` from JSON (stored in PostgreSQL)
2. Construct the runner for that role
3. Call `runWithGracefulShutdown()` — handles `SIGTERM` cleanly

**Where to start reading**: pick the role you care about, jump straight to its
`mainXxx()` function.

---

## 2. Ingestor — `internal/ingestor/processor.go`

**Job**: read records from Kafka, validate schema, publish to NATS.

```
Kafka record
  │
  ▼ prepareMessage()
  ├── schema.Validate()         validate JSON against registered schema
  ├── schema.Get(dedupKey)      extract the field used for deduplication
  ├── compute NATS subject      hash(dedupKey) % N  →  subject index
  ├── set "Nats-Msg-Id" header  enables NATS native dedup downstream
  └── publish to NATS
        sync mode:   publish → waitAck → next record
        async mode:  bulk publish → wait for ack batch → classify results
```

**Backpressure**: when NATS is full the ingestor does not commit the Kafka
offset — the record stays in Kafka and will be retried on the next poll.

**DLQ**: schema violations or publish failures call `pushMsgToDLQ()`, which
wraps the original bytes in a `DLQMessage` and publishes to the pipeline's DLQ
stream.

---

## 3. Processor Chain — `internal/processor/`

This is the **shared execution framework** used by Dedup, Join, and Sink.
Understanding this package unlocks ~80 % of the runtime behaviour.

```
Component.Run():
  loop:
    batch   = reader.ReadBatch()
    result  = processor1(processor2(processor3(batch)))
    writer.WriteBatch(result.Messages)
    dlqWriter.WriteBatch(result.FailedMessages)
    batch.Commit()   ACK NATS messages only after all writes succeed
```

Processors compose via a middleware pattern identical to HTTP middleware:

```go
// ChainProcessors applies middlewares outermost-first
// result: DLQMiddleware → FilterProcessor → DedupProcessor → base
```

The central data structure is `ProcessorBatch`:

```go
type ProcessorBatch struct {
    Messages       []models.Message       // in-flight messages
    FailedMessages []models.FailedMessage // errors → DLQ
    FatalError     error                  // stop the whole component
    CommitFn       func() error           // ACK after successful write
}
```

---

## 4. Deduplication — `internal/deduplication/badger/`

**Job**: filter duplicate messages within a configurable time window using
BadgerDB (an embedded key-value store — no external process required).

```
FilterDuplicates(messages):
  for each msg:
    id = msg.Header("Nats-Msg-Id")
    if id == "":        pass through  (no ID → not deduplicated)
    elif id in badger:  drop          (seen before)
    else:               keep          (first occurrence)

SaveKeys(kept_messages):
  for each kept msg:
    badger.Set(key=id, value=empty, ttl=window)
    entries expire automatically — prevents unbounded growth
```

**Why BadgerDB**: embedded, zero ops overhead, native TTL support.

The ingestor sets `Nats-Msg-Id` to `hash(dedupKeyFieldValue)`. The
deduplicator only sees the header — it never parses message payloads.

---

## 5. Join — `internal/join/temporal.go`

**Job**: match records from two streams on a shared key within a time window.

```
LEFT stream (e.g. orders):
  HandleLeftStreamEvents(msg):
    key = extract(msg, leftJoinKey)
    right = rightKV.Get(key)
    if right found:
      emit joined(msg, right)   right was already buffered → join immediately
    else:
      store msg in leftBuffer   wait for right to arrive

RIGHT stream (e.g. customers):
  HandleRightStreamEvents(msg):
    key = extract(msg, rightJoinKey)
    rightKV.Set(key, msg)       keep only the latest right value per key
    for each left waiting on key:
      emit joined(left, msg)    drain the wait queue
```

**Design**: left records are buffered (many per key); right keeps only the
**latest value** per key. This is a "latest-right join" — well suited when
the right stream is a slowly-changing dimension table (customers, products).

---

## 6. Sink — `internal/sink/clickhouse.go`

**Job**: consume from NATS, map fields, write batches to ClickHouse.

```
ClickHouseSink.Run():
  ├── worker pool (N goroutines, process messages in parallel)
  │     each worker:
  │       schemaVersion = msg.Header("schema-version")
  │       config        = cfgStore.Get(schemaVersion)
  │       values        = mapper.Map(msg.data, config)
  │
  ├── buffer accumulator
  │     collect incoming messages
  │     flush when: buffer reaches maxBatchSize  OR  flush timer fires
  │
  └── sendBatch():
        clickhouse.InsertBatch(rows)
        success        → ACK NATS messages
        retryable err  → NACK  (message returns to NATS queue)
        fatal err      → DLQ
```

**Schema versioning**: every NATS message carries a `schema-version` header.
The sink looks up the matching column-mapping config for that version, so
schema changes can be rolled out without restarting the sink.

---

## 7. Data Model — `internal/models/configs.go`

`PipelineConfig` is the "blueprint" of a pipeline, persisted in PostgreSQL.

```
PipelineConfig
├── ID, Name, Status
├── SourceType          kafka | otlp.logs | otlp.traces | otlp.metrics
├── IngestorConfig
│     └── Topics[]
│           ├── ConnectionParams   broker address, auth
│           ├── SchemaFields[]     field name + type
│           └── Deduplication      which field is the dedup key
├── JoinComponentConfig (optional)
│     ├── Sources[]    left source, right source
│     └── Rules[]      which fields to take from each side
├── SinkConfig
│     └── Mapper[]     source field → ClickHouse column
└── FilterConfig, StatelessTransform (optional)
```

---

## 8. NATS Stream Naming Convention

All inter-role communication goes through NATS JetStream. Stream names are
derived from the pipeline ID via a short hash to avoid collisions across
pipelines.

| Purpose | Name pattern |
|---|---|
| Ingestor output (per topic) | `gfm-{hash}-{topic}` |
| Join output | `gfm-{hash}-joined` |
| DLQ | `gfm-{hash}-DLQ` |
| Component signals | `component-signals.failures` |

---

## 9. How to Navigate the Code

```
To understand a feature, read in this order:

1. API handler       internal/api/
   HTTP request in, input validation, JSON → model conversion

2. Service layer     internal/service/
   Business logic, orchestrates components

3. Component/runner  internal/component/  or  internal/processor/
   Runtime message processing

4. Storage           internal/storage/postgres/
   Pipeline config persistence
```

**Worked example — creating a new pipeline**:

```
POST /api/v1/pipeline
  → internal/api/create_pipeline.go       handler
  → internal/api/pipeline_to_model.go     validate + convert JSON to PipelineConfig
  → internal/service/pipeline.go          save to DB, trigger deployment
  → internal/orchestrator/k8s.go          create Kubernetes pods
      or internal/orchestrator/docker.go  create Docker containers (local dev)
```