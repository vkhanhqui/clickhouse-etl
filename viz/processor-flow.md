# Processor Package — Data Flow

**Package:** `glassflow-api/internal/processor/`
**Role:** Message processing pipeline: filter → dedup → transform, with DLQ routing, backpressure handling, and two orchestrator modes (pull/push).

---

## 1. Core Data Unit — `ProcessorBatch`

```
ProcessorBatch
├── Messages       []models.Message      ← surviving messages (pass through chain)
├── FailedMessages []models.FailedMessage ← per-message failures → drained to DLQ
├── FatalError     error                 ← chain-aborting error (NAK entire batch)
└── CommitFn       func() error          ← deferred callback (dedup key persistence only)
```

---

## 2. Processor Interface & Chain

```
Processor interface {
    ProcessBatch(ctx, ProcessorBatch) ProcessorBatch
    Close(ctx) error
}
```

### Implementations

| Processor | State | Role |
|---|---|---|
| `FilterProcessor` | stateless | Predicate drop: `Matches(payload)` → keep/drop/error |
| `DedupProcessor` | **stateful** | `FilterDuplicates()` at process, `SaveKeys()` deferred via CommitFn after write |
| `StatelessTransformerProcessor` | stateless | Per-message `Transform(ctx, msg)` |
| `NoopProcessor` | stateless | Identity passthrough (disabled feature) |
| `dlqMiddleware` | middleware | Wraps any Processor, drains FailedMessages → DLQ |

### Chain Composition

```go
ChainProcessors(middlewares, base Processor) Processor
```

Middleware pattern (first in slice = outermost). Noop short-circuits.

**Wired order:** `Filter → (DLQ) → Dedup → (DLQ) → Transform → (DLQ)`

---

## 3. Architecture Diagram

```
                         ┌──────────────────────────────┐
                         │        BatchReader            │
                         │  (NATS JetStream consumer)    │
                         │  pull: ReadBatch()            │
                         │  push: Consume(handler)       │
                         └─────────────┬────────────────┘
                                       │
                                       │ []models.Message
                                       v
┌──────────────────────────────────────────────────────────────────┐
│                    Component / StreamingComponent                 │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │                 runProcessors()                          │    │
│  │                                                          │    │
│  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐ │    │
│  │  │    Filter    │──>│    Dedup     │──>│  Transform   │ │    │
│  │  │              │   │              │   │              │ │    │
│  │  │ Matches()    │   │FilterDupl.() │   │Transform()   │ │    │
│  │  │  → keep/drop │   │  → dedup'd   │   │  → mutated   │ │    │
│  │  │  → error DLQ │   │  → fatal err │   │  → error DLQ │ │    │
│  │  │              │   │              │   │              │ │    │
│  │  │ CommitFn:nil │   │ CommitFn:    │   │ CommitFn:nil │ │    │
│  │  │              │   │ SaveKeys()   │   │              │ │    │
│  │  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘ │    │
│  │         │                  │                  │          │    │
│  │         v                  v                  v          │    │
│  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐ │    │
│  │  │   DLQ MW     │   │   DLQ MW     │   │   DLQ MW     │ │    │
│  │  │ (filter err) │   │ (dedup err)  │   │ (transform   │ │    │
│  │  │              │   │              │   │  err)        │ │    │
│  │  └──────────────┘   └──────────────┘   └──────────────┘ │    │
│  │                                                          │    │
│  └──────────────────────────┬───────────────────────────────┘    │
│                             │                                    │
│              ┌──────────────┴──────────────┐                     │
│              │    messages, commitFns       │                     │
│              └──────────────┬──────────────┘                     │
└─────────────────────────────┼────────────────────────────────────┘
                              │
                              v
            ┌─────────────────────────────────┐
            │      writer.WriteBatch()        │
            │      or writeWithBackpressure() │
            └────────────────┬────────────────┘
                             │
              ┌──────────────┴──────────────┐
              v                             v
        (success)                    (failure)
              │                             │
              v                             │
        execute CommitFns                   │
        (Dedup: SaveKeys)                   │
              │                             │
              v                             │
        reader.Ack()                   hard error?
              │                        ┌────┴────┐
              v                        v         v
           (done)                  write DLQ   NAK batch
                                    (recover)  (retry)
                                              (Component only)

                                       backpressure?
                                           (StreamingComponent only)
                                              │
                                              v
                                    retry loop: exp backoff
                                    initial 50ms, max 5s
                                    extend AckWait via InProgress()
                                    signal cooldown 5min
```

---

## 4. Processor Detail

### 4a. FilterProcessor

```
ProcessBatch(batch)
  for each msg in batch.Messages:
    filter.Matches(payload)
      ├── error → append to FailedMessages ("filter evaluation error")
      ├── false → drop (filtered out)
      └── true  → keep
  return batch (CommitFn: nil)
```

### 4b. DedupProcessor

```
ProcessBatch(batch)
  dedup.FilterDuplicates(ctx, batch.Messages)
    ├── error → return ProcessorBatch{FatalError: err}  ← ABORT CHAIN
    └── ok   → set deferred CommitFn:
                  func() error { dedup.SaveKeys(ctx, deduplicatedMessages) }
  return batch (CommitFn: saved closure)
```

`SaveKeys()` runs **after** successful write. At-most-once semantics.

### 4c. StatelessTransformerProcessor

```
ProcessBatch(batch)
  for each msg in batch.Messages:
    transform.Transform(ctx, msg)
      ├── ErrSignalSent → return FatalError  ← ABORT CHAIN (clean shutdown)
      ├── other error   → append to FailedMessages
      └── success       → replace msg in Messages
  return batch (CommitFn: nil)
```

### 4d. NoopProcessor

```
ProcessBatch(batch) → return batch (identity)
```

### 4e. dlqMiddleware

```
ProcessBatch(batch)
  result = next.ProcessBatch(ctx, batch)
  if result.FatalError != nil → return result (no DLQ)
  if len(result.FailedMessages) > 0:
    dlqWriter.WriteBatch(ctx, convertToDLQMsgs(failedMessages))
      ├── error → result.FatalError = err
      └── ok    → clear result.FailedMessages, record DLQ metric
  return result
```

---

## 5. Two Orchestrators

### 5a. Component (pull-based, batch reader)

```
Start(ctx)
  loop:
    batch = reader.ReadBatch(ctx, size=100, timeout=1ms)
    if ctx cancelled → handleShutdown():
      reader.ReadBatchNoWait()  // drain remaining
      ProcessBatch(remaining)
      close all processors
      return

    ProcessBatch(batch):
      msgs, commitFns, err = runProcessors(ctx, batch)
      if err → reader.Nak(ctx, batch)  ← redeliver
      if no msgs → reader.Ack(ctx, batch)
      else:
        writer.WriteBatch(ctx, msgs)
          ├── fail → write DLQ, execute commitFns, reader.Ack(ctx, batch)
          └── ok   → execute commitFns, reader.Ack(ctx, batch)
```

### 5b. StreamingComponent (push-based, buffer + flush)

```
Start(ctx)
  buffer = []
  start flush goroutine (tick every 100ms)
  reader.Consume(ctx, messageHandler):
    buffer.append(msg)
    if len(buffer) >= 50000 → flushBuffer()

  flushBuffer():
    copy buffer under lock → reset buffer
    ProcessBatch(copied)

  ProcessBatch(batch):
    msgs, commitFns, err = runProcessors(ctx, batch)
    writeWithBackpressure(ctx, msgs):    ← instead of direct write
      1. writer.WriteBatch(ctx, msgs)
      2. classify failures:
         - backpressure err → retry loop
         - hard err → DLQ
      3. retry loop:
         - exponential backoff (50ms → 5s)
         - extend AckWait via InProgress()
         - emit ComponentSignal (capped 5min cooldown)
    execute commitFns  (always)

  on shutdown:
    consumeContext.Stop()
    flushBuffer() one last time
    close all processors
```

---

## 6. Metrics

| Metric | Labels | Emitter |
|---|---|---|
| `bytes_processed` | component, pipeline_id, direction=in\|out | Filter, Dedup, Transform |
| `processing_duration` | component, pipeline_id, stage | Filter, Dedup (lookup/commit), Transform |
| `processor_messages` | component, pipeline_id, status | Filter (success/filtered/error), Dedup (duplicate/success), Transform (success/error/schema_error) |
| `DLQWrite` | component, pipeline_id, reason | Component, StreamingComponent, dlqMiddleware |
| `BackpressureStart` / `BackpressureStop` | component, pipeline_id | StreamingComponent |

---

## 7. Error Handling Summary

| Error Location | Severity | Action |
|---|---|---|
| Filter eval error | per-message | → FailedMessages → DLQ |
| Dedup FilterDuplicates error | **fatal** | FatalError → NAK entire batch |
| Transform error (general) | per-message | → FailedMessages → DLQ |
| Transform ErrSignalSent | **fatal** | FatalError → clean component exit |
| Write failure (Component) | batch | NAK the batch for redelivery |
| Write failure (Streaming): backpressure | retriable | Exponential backoff, InProgress() keepalive |
| Write failure (Streaming): hard | per-message | → DLQ |
| DLQ write failure | **fatal** | Sets FatalError on the result batch |

---

## 8. Transient/Container Guarantees

```
Filter      ── no state, no CommitFn
Dedup       ── stateful (BadgerDB). CommitFn runs after write → at-most-once dedup persistence
Transform   ── no state, no CommitFn
DLQ Middleware ── no state, no CommitFn
```
