# CdcStream Implementation Spec

Port Rust `CdcStream` from `.repos/amp/crates/clients/flight/src/cdc.rs` to TypeScript with Effect, providing CDC (Change Data Capture) semantics with Insert/Delete events and at-least-once delivery.

## Overview

The CDC stream wraps a `TransactionalStream` and adds a `BatchStore` for persisting batch content. This enables emitting `Delete` events with the original data during reorgs and rewinds, which forwarding consumers need to propagate deletions downstream.

**Key difference from TransactionalStream**: TransactionalStream emits raw `Undo` events with transaction ID ranges — the consumer must track what data those IDs correspond to. CdcStream handles this internally by storing batches in a `BatchStore` and lazily loading them for `Delete` events.

### What CdcStream adds on top of TransactionalStream

| Concern | TransactionalStream | CdcStream |
|---------|---------------------|-----------|
| New data | `Data` event with records | `Insert` event with records |
| Rollback | `Undo` event with TX ID range | `Delete` event with original batch data |
| Watermarks | Exposed to consumer | Handled internally (pruning only) |
| Batch persistence | None | `BatchStore` stores batches for replay |
| Delivery semantics | Exactly-once (manual commit) | At-least-once (manual commit) |

## Rust Source Reference

- **CDC stream**: `.repos/amp/crates/clients/flight/src/cdc.rs`
- **BatchStore trait**: `.repos/amp/crates/clients/flight/src/store/mod.rs` (lines 131-179)
- **InMemoryBatchStore**: `.repos/amp/crates/clients/flight/src/store/memory.rs`

## File Structure

```
packages/amp/src/cdc-stream/
├── types.ts              # CdcEvent union, DeleteBatchIterator
├── errors.ts             # BatchStoreError, CdcStreamError
├── batch-store.ts        # BatchStore Context.Tag service interface
├── memory-batch-store.ts # InMemoryBatchStore Layer implementation
├── stream.ts             # CdcStream Context.Tag and Layer
└── (no state-actor — CdcStream wraps TransactionalStream directly)

packages/amp/src/cdc-stream.ts   # Root-level barrel

packages/amp/test/cdc-stream/
├── memory-batch-store.test.ts   # InMemoryBatchStore conformance tests
└── stream.test.ts               # CdcStream integration tests
```

## Public API (cdc-stream.ts barrel)

```typescript
// =============================================================================
// Events
// =============================================================================

export {
  CdcEvent,
  type CdcEventDelete,
  type CdcEventInsert,
  type DeleteBatchIterator
} from "./cdc-stream/types.ts"

// =============================================================================
// Errors
// =============================================================================

export {
  BatchStoreError,
  type CdcStreamError
} from "./cdc-stream/errors.ts"

// =============================================================================
// BatchStore Service
// =============================================================================

export {
  BatchStore,
  type BatchStoreService
} from "./cdc-stream/batch-store.ts"

// =============================================================================
// InMemoryBatchStore Layer
// =============================================================================

export * as InMemoryBatchStore from "./cdc-stream/memory-batch-store.ts"

// =============================================================================
// CdcStream Service
// =============================================================================

export {
  CdcStream,
  type CdcStreamOptions,
  type CdcStreamService,
  layer
} from "./cdc-stream/stream.ts"
```

**Usage from consumer code:**

```typescript
import {
  BatchStore,
  CdcStream,
  type CdcEvent,
  InMemoryBatchStore
} from "@edgeandnode/amp/cdc-stream"
```

## Type Definitions

### types.ts

```typescript
import type * as Effect from "effect/Effect"
import type { BlockRange } from "../core/domain.ts"
import type { BatchStoreError } from "./errors.ts"
import type { TransactionId } from "../transactional-stream/types.ts"

/**
 * Iterator over delete batches that loads them lazily from the BatchStore.
 *
 * Returned for Delete events. Allows consumers to process batches one at a
 * time without loading all into memory.
 */
export interface DeleteBatchIterator {
  /**
   * Get the next batch to delete.
   * Returns the transaction ID and decoded batch data.
   * Returns None when all batches have been yielded.
   */
  readonly next: Effect.Effect<
    readonly [TransactionId, ReadonlyArray<Record<string, unknown>>] | undefined,
    BatchStoreError
  >
}

/**
 * Insert event — new data to forward downstream.
 */
export interface CdcEventInsert {
  readonly _tag: "Insert"
  readonly id: TransactionId
  readonly data: ReadonlyArray<Record<string, unknown>>
  readonly ranges: ReadonlyArray<BlockRange>
}

/**
 * Delete event — data that must be removed/reverted downstream.
 *
 * Emitted during rewind (crash recovery) or reorg (chain reorganization).
 * The iterator lazily loads batches from the BatchStore.
 */
export interface CdcEventDelete {
  readonly _tag: "Delete"
  readonly id: TransactionId
  readonly batches: DeleteBatchIterator
}

/**
 * CDC event union — simplified interface for forwarding consumers.
 * Watermarks are handled internally; consumers only see Insert and Delete.
 */
export type CdcEvent = CdcEventInsert | CdcEventDelete
```

**Design note — DeleteBatchIterator**: The Rust implementation uses a mutable struct with `async fn next()`. In TypeScript/Effect, we model this as an interface whose `next` property returns an `Effect` that yields `[TransactionId, data] | undefined`. Internally, the iterator holds a `Ref<number>` cursor and the batch IDs list.

### errors.ts

```typescript
import * as Schema from "effect/Schema"
import type { TransactionalStreamError } from "../transactional-stream/errors.ts"

/**
 * Error from BatchStore operations.
 */
export class BatchStoreError extends Schema.TaggedError<BatchStoreError>(
  "Amp/CdcStream/BatchStoreError"
)("BatchStoreError", {
  reason: Schema.String,
  operation: Schema.Literal("append", "seek", "load", "prune"),
  cause: Schema.optional(Schema.Defect)
}) {}

/**
 * Union of all CDC stream errors.
 */
export type CdcStreamError =
  | BatchStoreError
  | TransactionalStreamError
```

### batch-store.ts

Mirrors the Rust `BatchStore` trait. Provides persistence for batch content so that `Delete` events can replay original data.

```typescript
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type { BatchStoreError } from "./errors.ts"
import type { TransactionId, TransactionIdRange } from "../transactional-stream/types.ts"

/**
 * BatchStore service interface — pluggable persistence for batch content.
 *
 * Stores decoded JSON batch data (not raw Arrow IPC) since the TypeScript SDK
 * works with decoded records rather than RecordBatch objects.
 *
 * Implementations:
 * - InMemoryBatchStore: Development/testing (no persistence)
 * - IndexedDBBatchStore: Browser persistence (future)
 * - SqliteBatchStore: Node.js file-based persistence (future)
 */
export interface BatchStoreService {
  /**
   * Store batch data for a transaction.
   *
   * Called before emitting Insert events to guarantee data is available
   * for future Delete events.
   */
  readonly append: (
    data: ReadonlyArray<Record<string, unknown>>,
    id: TransactionId
  ) => Effect.Effect<void, BatchStoreError>

  /**
   * Find all stored batch IDs within a transaction ID range.
   *
   * Lightweight operation — returns only IDs, not batch data.
   * Used to build the DeleteBatchIterator.
   */
  readonly seek: (
    range: TransactionIdRange
  ) => Effect.Effect<ReadonlyArray<TransactionId>, BatchStoreError>

  /**
   * Load a single batch by transaction ID.
   *
   * Returns undefined if no batch exists for this ID (e.g. it was a
   * watermark or undo event, not a data event).
   */
  readonly load: (
    id: TransactionId
  ) => Effect.Effect<ReadonlyArray<Record<string, unknown>> | undefined, BatchStoreError>

  /**
   * Prune batches up to cutoff transaction ID (inclusive).
   *
   * Deletes all batches with IDs <= cutoff. Must be idempotent.
   * Best-effort — failures are logged but not fatal.
   */
  readonly prune: (
    cutoff: TransactionId
  ) => Effect.Effect<void, BatchStoreError>
}

export class BatchStore extends Context.Tag("Amp/CdcStream/BatchStore")<
  BatchStore,
  BatchStoreService
>() {}
```

**Key difference from Rust**: The Rust `BatchStore` stores `RecordBatch` (Arrow columnar format) and uses Arrow IPC serialization. The TypeScript SDK works with decoded JSON records (`ReadonlyArray<Record<string, unknown>>`), so the `BatchStore` stores decoded data directly. This avoids re-serialization overhead and aligns with how the rest of the TypeScript SDK handles data.

### memory-batch-store.ts

```typescript
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import type { TransactionId, TransactionIdRange } from "../transactional-stream/types.ts"
import { BatchStore, type BatchStoreService } from "./batch-store.ts"

type BatchMap = ReadonlyMap<TransactionId, ReadonlyArray<Record<string, unknown>>>

const make = Effect.gen(function*() {
  const mapRef = yield* Ref.make<BatchMap>(new Map())

  const append = (data: ReadonlyArray<Record<string, unknown>>, id: TransactionId) =>
    Ref.update(mapRef, (map) => new Map([...map, [id, data]]))

  const seek = (range: TransactionIdRange) =>
    Ref.get(mapRef).pipe(
      Effect.map((map) => {
        const ids: Array<TransactionId> = []
        for (const id of map.keys()) {
          if (id >= range.start && id <= range.end) ids.push(id)
        }
        return ids.sort((a, b) => a - b)
      })
    )

  const load = (id: TransactionId) =>
    Ref.get(mapRef).pipe(
      Effect.map((map) => map.get(id))
    )

  const prune = (cutoff: TransactionId) =>
    Ref.update(mapRef, (map) => {
      const next = new Map<TransactionId, ReadonlyArray<Record<string, unknown>>>()
      for (const [id, data] of map) {
        if (id > cutoff) next.set(id, data)
      }
      return next
    })

  return { append, seek, load, prune } satisfies BatchStoreService
})

export const layer: Layer.Layer<BatchStore> = Layer.effect(BatchStore, make)
```

## CdcStream Service

### stream.ts

```typescript
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import type { CommitHandle } from "../transactional-stream/commit-handle.ts"
import { TransactionalStream, type TransactionalStreamOptions } from "../transactional-stream/stream.ts"
import type { TransactionId } from "../transactional-stream/types.ts"
import { BatchStore } from "./batch-store.ts"
import type { CdcStreamError } from "./errors.ts"
import type { CdcEvent, DeleteBatchIterator } from "./types.ts"

export interface CdcStreamOptions extends TransactionalStreamOptions {}

export interface CdcStreamService {
  /**
   * Create a CDC stream from a SQL query.
   *
   * Returns tuples of [CdcEvent, CommitHandle]. The consumer must call
   * commit() after processing each event to advance the stream state.
   */
  readonly streamCdc: (
    sql: string,
    options?: CdcStreamOptions
  ) => Stream.Stream<readonly [CdcEvent, CommitHandle], CdcStreamError>

  /**
   * High-level consumer: auto-commit after callback succeeds.
   */
  readonly forEach: <E, R>(
    sql: string,
    options: CdcStreamOptions,
    handler: (event: CdcEvent) => Effect.Effect<void, E, R>
  ) => Effect.Effect<void, CdcStreamError | E, R>
}

export class CdcStream extends Context.Tag("Amp/CdcStream")<
  CdcStream,
  CdcStreamService
>() {}
```

### Implementation Logic (make)

The implementation wraps `TransactionalStream.streamTransactional()` and transforms events:

```typescript
const make = Effect.gen(function*() {
  const txStream = yield* TransactionalStream
  const batchStore = yield* BatchStore

  const streamCdc = (sql: string, options?: CdcStreamOptions) => {
    return txStream.streamTransactional(sql, options).pipe(
      Stream.mapEffect(
        Effect.fnUntraced(function*([event, commit]): Effect.fn.Return<
          Option.Option<readonly [CdcEvent, CommitHandle]>,
          CdcStreamError
        > {
          switch (event._tag) {
            case "Data": {
              // Store batch BEFORE emitting — guarantees availability for Delete
              yield* batchStore.append(event.data, event.id)
              const insert: CdcEvent = {
                _tag: "Insert",
                id: event.id,
                data: event.data,
                ranges: event.ranges
              }
              return Option.some([insert, commit] as const)
            }

            case "Undo": {
              // Find batch IDs in the invalidation range
              const ids = yield* batchStore.seek(event.invalidate)

              if (ids.length === 0) {
                // No batches to delete — skip (don't emit empty Delete)
                return Option.none()
              }

              // Build lazy iterator
              const iterator = makeDeleteBatchIterator(batchStore, ids)
              const del: CdcEvent = {
                _tag: "Delete",
                id: event.id,
                batches: iterator
              }
              return Option.some([del, commit] as const)
            }

            case "Watermark": {
              // Handle batch pruning when retention window moves
              if (Option.isSome(event.prune)) {
                // Best-effort pruning
                yield* batchStore.prune(event.prune.value).pipe(
                  Effect.catchAll((error) =>
                    Effect.logWarning("Batch pruning failed (will retry on next watermark)", error)
                  )
                )
              }
              // Watermarks are not exposed to CDC consumers
              // Still need to commit so the underlying TransactionalStream advances
              yield* commit.commit
              return Option.none()
            }
          }
        })
      ),
      Stream.filterMap((x) => x),
      Stream.withSpan("CdcStream.streamCdc")
    )
  }

  // forEach auto-commits after handler succeeds
  const forEach = <E, R>(
    sql: string,
    options: CdcStreamOptions,
    handler: (event: CdcEvent) => Effect.Effect<void, E, R>
  ): Effect.Effect<void, CdcStreamError | E, R> =>
    streamCdc(sql, options).pipe(
      Stream.runForEach(
        Effect.fnUntraced(function*([event, commitHandle]) {
          yield* handler(event)
          yield* commitHandle.commit
        })
      ),
      Effect.withSpan("CdcStream.forEach")
    )

  return { streamCdc, forEach } satisfies CdcStreamService
})

export const layer: Layer.Layer<
  CdcStream,
  never,
  TransactionalStream | BatchStore
> = Layer.effect(CdcStream, make)
```

### DeleteBatchIterator Implementation

```typescript
/**
 * Create a lazy batch iterator that loads batches one-by-one from the store.
 * Skips missing batches (watermark-only transactions).
 */
const makeDeleteBatchIterator = (
  store: BatchStoreService,
  ids: ReadonlyArray<TransactionId>
): DeleteBatchIterator => {
  // Mutable cursor via Ref not needed here — we use a simple closure
  // since the iterator is consumed linearly by a single consumer.
  let cursor = 0

  return {
    next: Effect.suspend(() => {
      const loop = (): Effect.Effect<
        readonly [TransactionId, ReadonlyArray<Record<string, unknown>>] | undefined,
        BatchStoreError
      > => {
        if (cursor >= ids.length) return Effect.succeed(undefined)
        const id = ids[cursor]!
        cursor++
        return store.load(id).pipe(
          Effect.flatMap((batch) =>
            batch !== undefined
              ? Effect.succeed([id, batch] as const)
              : loop() // Skip missing batches (watermarks)
          )
        )
      }
      return loop()
    })
  }
}
```

## Service Dependency Chain

```
CdcStream → TransactionalStream → ProtocolStream → ArrowFlight → Transport
          → BatchStore              → StateStore
```

## Layer Composition

```typescript
import { ArrowFlight, Transport } from "@edgeandnode/amp"
import { ProtocolStream } from "@edgeandnode/amp/protocol-stream"
import { InMemoryStateStore, TransactionalStream } from "@edgeandnode/amp/transactional-stream"
import { CdcStream, InMemoryBatchStore } from "@edgeandnode/amp/cdc-stream"

// Development layer
const DevLayer = CdcStream.layer.pipe(
  Layer.provide(InMemoryBatchStore.layer),
  Layer.provide(TransactionalStream.layer),
  Layer.provide(InMemoryStateStore.layer),
  Layer.provide(ProtocolStream.layer),
  Layer.provide(ArrowFlight.layer),
  Layer.provide(MyTransportLayer)
)

// Usage
const program = Effect.gen(function*() {
  const cdc = yield* CdcStream

  yield* cdc.forEach(
    "SELECT * FROM eth.logs WHERE address = '0x...'",
    { retention: 128 },
    Effect.fnUntraced(function*(event) {
      switch (event._tag) {
        case "Insert":
          yield* forwardInsert(event.id, event.data)
          break
        case "Delete": {
          let batch = yield* event.batches.next
          while (batch !== undefined) {
            const [id, data] = batch
            yield* forwardDelete(id, data)
            batch = yield* event.batches.next
          }
          break
        }
      }
    })
  )
})

Effect.runPromise(program.pipe(Effect.provide(DevLayer)))
```

## Implementation Order

1. **types.ts** — CdcEvent, CdcEventInsert, CdcEventDelete, DeleteBatchIterator
2. **errors.ts** — BatchStoreError, CdcStreamError
3. **batch-store.ts** — BatchStore Context.Tag and interface
4. **memory-batch-store.ts** — InMemoryBatchStore with Ref (+ tests)
5. **stream.ts** — CdcStream service, makeDeleteBatchIterator, layer
6. **cdc-stream.ts** — Root-level barrel exports
7. **Tests** — BatchStore conformance, CdcStream integration

## Test Plan

### memory-batch-store.test.ts

- `append` stores data retrievable by `load`
- `load` returns undefined for missing IDs
- `seek` returns sorted IDs within range
- `seek` returns empty array for empty range
- `prune` removes batches up to cutoff (inclusive)
- `prune` is idempotent

### stream.test.ts

These tests require mocking or providing a `TransactionalStream` that emits controlled `TransactionEvent` sequences:

- **Insert passthrough**: Data events become Insert events with same data
- **Insert stores before emitting**: Verify batch is in store before consumer sees Insert
- **Undo becomes Delete**: Undo events with matching batch IDs become Delete events
- **Undo with no batches is skipped**: Undo for watermark-only range emits nothing
- **Delete iterator loads lazily**: Batches are loaded one at a time, not all at once
- **Delete iterator skips missing**: Watermark-only IDs are skipped in iterator
- **Watermark auto-commits**: Watermark events are not exposed but commit is called
- **Watermark triggers prune**: Prune point from watermark calls `batchStore.prune`
- **Prune failure is non-fatal**: BatchStore.prune error is logged, not propagated
- **forEach auto-commits**: Handler runs, then commit is called automatically

## Critical Files to Reference

| File | Purpose |
|------|---------|
| `packages/amp/src/transactional-stream/stream.ts` | TransactionalStream to wrap |
| `packages/amp/src/transactional-stream/types.ts` | TransactionEvent, TransactionId to consume |
| `packages/amp/src/transactional-stream/commit-handle.ts` | CommitHandle to pass through |
| `packages/amp/src/transactional-stream/errors.ts` | TransactionalStreamError to include in union |
| `.repos/amp/crates/clients/flight/src/cdc.rs` | Rust source to port |
| `.repos/amp/crates/clients/flight/src/store/mod.rs` | Rust BatchStore trait |
| `.repos/amp/crates/clients/flight/src/store/memory.rs` | Rust InMemoryBatchStore |

## Design Decisions

### JSON records vs Arrow IPC for BatchStore

The Rust SDK stores `RecordBatch` (Arrow columnar) and serializes via Arrow IPC. The TypeScript SDK already decodes batches to JSON records (`ReadonlyArray<Record<string, unknown>>`) in the ArrowFlight service before they reach TransactionalStream. Storing decoded JSON avoids:
- Re-serialization overhead
- Dependency on Arrow IPC writer (not currently in the TS SDK)
- Schema management for deserialization

Trade-off: larger storage footprint for in-memory stores. Acceptable because the retention window bounds memory usage.

### Watermark auto-commit

In Rust, watermarks are silently committed inside the CDC stream. We follow the same pattern: when a Watermark event arrives from TransactionalStream, CdcStream calls `commit.commit` internally and does not expose the watermark to the consumer. This keeps the CDC API simple (Insert/Delete only).

### DeleteBatchIterator as Effect

The Rust `DeleteBatchIterator` uses `async fn next(&mut self)`. In Effect, we model `next` as a property returning an `Effect` that can be called repeatedly. Each call advances an internal cursor. This is idiomatic Effect — the consumer calls `yield* iterator.next` in a loop.

## Verification

1. `pnpm vitest run packages/amp/test/cdc-stream/` — all tests pass
2. `pnpm check:recursive` — type check clean
3. `pnpm lint` — no lint or format errors
