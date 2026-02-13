# TransactionalStream Implementation Spec

Port Rust `TransactionalStream` from `.repos/amp/crates/clients/flight/src/transactional.rs` to TypeScript with Effect, providing exactly-once semantics, crash recovery, and reorg handling.

## Overview

The current TypeScript implementation has stateless reorg detection (`streamProtocol` in `arrow-flight.ts`). This spec adds a full transactional layer matching Rust parity:

- **Transaction IDs**: Monotonically increasing IDs for each event
- **State Store**: Pluggable persistence with InMemoryStateStore reference implementation
- **Watermark Buffer**: Tracks watermarks for reorg recovery point calculation
- **Commit Handles**: Exactly-once semantics via explicit commit
- **Rewind Detection**: Detects and invalidates uncommitted transactions on restart
- **Retention Window**: Prunes old watermarks outside configurable block window

## File Structure

```
packages/amp/src/transactional-stream/
├── index.ts                 # Public exports (see below)
├── types.ts                 # TransactionEvent, TransactionId, UndoCause, TransactionIdRange
├── errors.ts                # StateStoreError, UnrecoverableReorgError, PartialReorgError
├── state-store.ts           # StateStore Context.Tag, StateSnapshot, Commit
├── memory-store.ts          # InMemoryStateStore Layer implementation
├── algorithms.ts            # findRecoveryPoint, findPruningPoint (pure functions)
├── state-actor.ts           # StateActor (internal, not exported)
├── commit-handle.ts         # CommitHandle interface
└── stream.ts                # TransactionalStream Context.Tag and Layer

packages/amp/test/transactional-stream/
├── algorithms.test.ts       # Recovery/pruning algorithm tests
├── memory-store.test.ts     # InMemoryStateStore conformance tests
├── reorg.test.ts            # Reorg scenarios (port Rust tests)
├── rewind.test.ts           # Crash recovery scenarios
└── integration.test.ts      # Full stream integration
```

## Public API (index.ts exports)

```typescript
// =============================================================================
// Types
// =============================================================================
export {
  TransactionEvent,
  TransactionEventData,
  TransactionEventUndo,
  TransactionEventWatermark,
  TransactionId,
  TransactionIdRange,
  UndoCause
} from "./types.ts"

// =============================================================================
// Errors
// =============================================================================
export { PartialReorgError, StateStoreError, type TransactionalStreamError, UnrecoverableReorgError } from "./errors.ts"

// =============================================================================
// StateStore Service
// =============================================================================
export {
  type Commit,
  emptySnapshot,
  type StateSnapshot,
  StateStore // Context.Tag for DI
} from "./state-store.ts"

// =============================================================================
// InMemoryStateStore Layer
// =============================================================================
export * as InMemoryStateStore from "./memory-store.ts"

// =============================================================================
// CommitHandle
// =============================================================================
export { type CommitHandle } from "./commit-handle.ts"

// =============================================================================
// TransactionalStream Service
// =============================================================================
export {
  layer, // Layer.Layer<TransactionalStream, never, ArrowFlight | StateStore>
  TransactionalStream, // Context.Tag for DI
  type TransactionalStreamOptions
} from "./stream.ts"
```

**Usage from consumer code:**

```typescript
import {
  type CommitHandle,
  InMemoryStateStore,
  StateStore,
  TransactionalStream,
  type TransactionEvent
} from "@edgeandnode/amp/transactional-stream"
```

## Type Definitions

### types.ts

```typescript
// TransactionId - branded non-negative integer
export const TransactionId = Schema.NonNegativeInt.pipe(
  Schema.brand("Amp/TransactionalStream/TransactionId")
)

// UndoCause - discriminated union
export const UndoCause = Schema.Union(
  Schema.TaggedStruct("Reorg", { invalidation: Schema.Array(InvalidationRange) }),
  Schema.TaggedStruct("Rewind", {})
)

// TransactionEvent - three variants matching Rust
export const TransactionEventData = Schema.TaggedStruct("Data", {
  id: TransactionId,
  data: Schema.Array(Schema.Record({ key: Schema.String, value: Schema.Unknown })),
  ranges: Schema.Array(BlockRange)
})

export const TransactionEventUndo = Schema.TaggedStruct("Undo", {
  id: TransactionId,
  cause: UndoCause,
  invalidate: Schema.Struct({ start: TransactionId, end: TransactionId })
})

export const TransactionEventWatermark = Schema.TaggedStruct("Watermark", {
  id: TransactionId,
  ranges: Schema.Array(BlockRange),
  prune: Schema.OptionFromNullOr(TransactionId)
})

export const TransactionEvent = Schema.Union(
  TransactionEventData,
  TransactionEventUndo,
  TransactionEventWatermark
)
```

### state-store.ts

The StateStore is defined as an Effect **Context.Tag service**, allowing different implementations to be swapped via Layers. This mirrors the Rust pattern where `StateStore` is a trait implemented by `InMemoryStateStore`, `LmdbStateStore`, and `PostgresStateStore`.

```typescript
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

// =============================================================================
// Data Types
// =============================================================================

/**
 * StateSnapshot - persisted state for crash recovery.
 * Loaded once on startup, then maintained in-memory.
 */
export interface StateSnapshot {
  readonly buffer: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>
  readonly next: TransactionId
}

/**
 * Commit - atomic state update for persistence.
 * Batches multiple watermark inserts with optional pruning.
 */
export interface Commit {
  readonly insert: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>
  readonly prune: TransactionId | undefined
}

// =============================================================================
// StateStore Service (Context.Tag)
// =============================================================================

/**
 * StateStore service interface - pluggable persistence for stream state.
 *
 * Different implementations provide crash recovery guarantees:
 * - InMemoryStateStore: No persistence (development/testing)
 * - IndexedDBStateStore: Browser persistence (future)
 * - SqliteStateStore: Node.js file-based persistence (future)
 * - PostgresStateStore: Distributed persistence (future)
 */
export interface StateStore {
  /**
   * Pre-allocate next transaction ID.
   * Called immediately after incrementing in-memory counter.
   * Ensures ID monotonicity survives crashes.
   */
  readonly advance: (next: TransactionId) => Effect.Effect<void, StateStoreError>

  /**
   * Atomically commit watermarks and apply pruning.
   * Called when user invokes CommitHandle.commit().
   * Must be idempotent (safe to call multiple times with same data).
   */
  readonly commit: (commit: Commit) => Effect.Effect<void, StateStoreError>

  /**
   * Truncate buffer during reorg handling.
   * Removes all watermarks with ID >= from.
   * Called immediately when reorg detected (before emitting Undo).
   */
  readonly truncate: (from: TransactionId) => Effect.Effect<void, StateStoreError>

  /**
   * Load initial state on startup.
   * Called once when TransactionalStream is created.
   * Returns empty state if no prior state exists.
   */
  readonly load: () => Effect.Effect<StateSnapshot, StateStoreError>
}

/**
 * StateStore Context.Tag - use this to depend on StateStore in Effects.
 */
export class StateStore extends Context.Tag("Amp/TransactionalStream/StateStore")<
  StateStore,
  StateStore
>() {}

// =============================================================================
// Empty State Helper
// =============================================================================

/**
 * Initial empty state for new streams or cleared stores.
 */
export const emptySnapshot: StateSnapshot = {
  buffer: [],
  next: 0 as TransactionId
}
```

### memory-store.ts (InMemoryStateStore Layer)

Reference implementation using Effect Ref. Not crash-safe but suitable for development, testing, and ephemeral use cases.

````typescript
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import { type Commit, emptySnapshot, type StateSnapshot, StateStore } from "./state-store.ts"

/**
 * Create InMemoryStateStore Layer.
 *
 * @example
 * ```typescript
 * const program = Effect.gen(function*() {
 *   const store = yield* StateStore
 *   const snapshot = yield* store.load()
 *   // ...
 * })
 *
 * Effect.runPromise(
 *   program.pipe(Effect.provide(InMemoryStateStore.layer))
 * )
 * ```
 */
const make = Effect.gen(function*() {
  const stateRef = yield* Ref.make<StateSnapshot>(emptySnapshot)

  const advance = (next: TransactionId) => Ref.update(stateRef, (state) => ({ ...state, next }))

  const commit = (commit: Commit) =>
    Ref.update(stateRef, (state) => {
      let buffer = [...state.buffer]

      // Remove pruned watermarks (all IDs <= prune)
      if (commit.prune !== undefined) {
        buffer = buffer.filter(([id]) => id > commit.prune!)
      }

      // Add new watermarks
      for (const entry of commit.insert) {
        buffer.push(entry)
      }

      return { ...state, buffer }
    })

  const truncate = (from: TransactionId) =>
    Ref.update(stateRef, (state) => ({
      ...state,
      buffer: state.buffer.filter(([id]) => id < from)
    }))

  const load = () => Ref.get(stateRef)

  return { advance, commit, truncate, load } satisfies StateStore
})

/**
 * Layer providing InMemoryStateStore.
 */
export const layer: Layer.Layer<StateStore> = Layer.effect(StateStore, make)

/**
 * Create layer with initial state (useful for testing).
 */
export const layerWithState = (
  initial: StateSnapshot
): Layer.Layer<StateStore> =>
  Layer.effect(
    StateStore,
    Effect.gen(function*() {
      const stateRef = yield* Ref.make<StateSnapshot>(initial)
      // ... same implementation as above
    })
  )
````

### Future Store Implementations

Additional StateStore implementations can be added as separate packages or modules:

```typescript
// packages/amp-store-indexeddb/src/index.ts
export const layer: Layer.Layer<StateStore> = Layer.effect(StateStore, make)

// packages/amp-store-sqlite/src/index.ts
export const layer: Layer.Layer<StateStore> = Layer.effect(StateStore, make)

// packages/amp-store-postgres/src/index.ts
export const layer: Layer.Layer<StateStore, never, PostgresClient> = Layer.effect(StateStore, make)
```

## Key Algorithms

### findRecoveryPoint (algorithms.ts)

Walk backwards through watermark buffer to find last unaffected watermark:

```typescript
export const findRecoveryPoint = (
  buffer: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>,
  invalidation: ReadonlyArray<InvalidationRange>
): readonly [TransactionId, ReadonlyArray<BlockRange>] | undefined => {
  // Build map: network -> first invalid block
  const points = new Map(invalidation.map((inv) => [inv.network, inv.start]))

  // Walk backwards (newest to oldest)
  for (let i = buffer.length - 1; i >= 0; i--) {
    const [id, ranges] = buffer[i]!
    const affected = ranges.some((range) => {
      const point = points.get(range.network)
      return point !== undefined && range.numbers.start >= point
    })
    if (!affected) return [id, ranges]
  }
  return undefined
}
```

### findPruningPoint (algorithms.ts)

Find oldest watermark outside retention window:

```typescript
export const findPruningPoint = (
  buffer: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>,
  retention: number
): TransactionId | undefined => {
  if (buffer.length === 0) return undefined

  const [, latestRanges] = buffer[buffer.length - 1]!
  const cutoffs = new Map(latestRanges.map((r) => [r.network, Math.max(0, r.numbers.start - retention)]))

  let last: TransactionId | undefined
  for (let i = 0; i < buffer.length - 1; i++) {
    const [id, ranges] = buffer[i]!
    const outside = ranges.every((r) => r.numbers.end < cutoffs.get(r.network)!)
    if (outside) last = id
    else break
  }
  return last
}
```

## StateActor (Internal)

StateActor is an **internal** component (not exported publicly). It wraps a StateStore instance with a Ref for concurrent-safe in-memory state management. Created once per stream instance.

```typescript
import * as Ref from "effect/Ref"

/**
 * Internal state container - in-memory copy of persisted state.
 * The store is the source of truth; this is a working cache.
 */
interface StateContainer {
  readonly store: StateStore // From context
  readonly retention: number // Configured retention window
  next: TransactionId // Next ID to assign
  buffer: Array<readonly [TransactionId, ReadonlyArray<BlockRange>]>
  uncommitted: Array<readonly [TransactionId, PendingCommit]>
}

/**
 * StateActor interface - manages transactional stream state.
 */
export interface StateActor {
  /** Get last watermark from buffer */
  readonly watermark: () => Effect.Effect<[TransactionId, ReadonlyArray<BlockRange>] | undefined>

  /** Get next transaction ID without incrementing */
  readonly peek: () => Effect.Effect<TransactionId>

  /** Execute an action and return event with commit handle */
  readonly execute: (action: Action) => Effect.Effect<
    [TransactionEvent, CommitHandle],
    TransactionalStreamError
  >

  /** Commit pending changes up to and including this ID */
  readonly commit: (id: TransactionId) => Effect.Effect<void, StateStoreError>
}

// Action union for execute()
type Action =
  | { readonly _tag: "Message"; readonly message: ProtocolMessage }
  | { readonly _tag: "Rewind" }

/**
 * Create a StateActor from a StateStore.
 * Called internally by TransactionalStream.
 */
export const makeStateActor = (
  store: StateStore,
  retention: number
): Effect.Effect<StateActor, StateStoreError> =>
  Effect.gen(function*() {
    // Load initial state from store
    const snapshot = yield* store.load()

    // Create mutable state container wrapped in Ref
    const containerRef = yield* Ref.make<StateContainer>({
      store,
      retention,
      next: snapshot.next,
      buffer: [...snapshot.buffer],
      uncommitted: []
    })

    // ... implement watermark, peek, execute, commit
    return { watermark, peek, execute, commit }
  })
```

**execute() Logic:**

1. **Pre-allocate monotonic ID**: Increment `next` in Ref, call `store.advance(next)` immediately
2. **Match on action type**:
   - **Rewind**: Compute invalidate range from buffer state, emit `Undo(Rewind)`
   - **Message(Data)**: Pass through as `TransactionEventData` (no buffer mutation)
   - **Message(Watermark)**: Add to buffer, compute prune via `findPruningPoint`, add to uncommitted queue, emit `TransactionEventWatermark`
   - **Message(Reorg)**: Find recovery point via `findRecoveryPoint`, check for partial reorg (error if unrecoverable), truncate buffer + call `store.truncate()`, clear uncommitted after recovery point, emit `Undo(Reorg)`
3. **Return** `[event, CommitHandle]` where CommitHandle captures actor + ID

## TransactionalStream Service

The TransactionalStream service depends on both `ArrowFlight` and `StateStore` via the Layer system. Users compose layers to provide the desired StateStore implementation.

```typescript
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

/**
 * Options for creating a transactional stream.
 * Note: StateStore is NOT passed here - it comes from the Layer context.
 */
export interface TransactionalStreamOptions {
  /** Retention window in blocks for pruning old watermarks */
  readonly retention: number
  /** Optional schema for data validation */
  readonly schema?: Schema.Any
}

/**
 * TransactionalStream service interface.
 */
export interface TransactionalStream {
  /**
   * Create a transactional stream from a SQL query.
   * Returns tuples of [event, commitHandle] for manual commit control.
   */
  readonly streamTransactional: (
    sql: string,
    options: TransactionalStreamOptions
  ) => Stream.Stream<readonly [TransactionEvent, CommitHandle], TransactionalStreamError>

  /**
   * High-level consumer: auto-commit after callback succeeds.
   * If callback fails, stream stops and pending batch remains uncommitted.
   * On restart, uncommitted batch triggers Rewind.
   */
  readonly forEach: <E, R>(
    sql: string,
    options: TransactionalStreamOptions,
    handler: (event: TransactionEvent) => Effect.Effect<void, E, R>
  ) => Effect.Effect<void, TransactionalStreamError | E, R>
}

/**
 * TransactionalStream Context.Tag
 */
export class TransactionalStream extends Context.Tag("Amp/TransactionalStream")<
  TransactionalStream,
  TransactionalStream
>() {}

/**
 * Layer providing TransactionalStream.
 * Requires ArrowFlight and StateStore in context.
 */
export const layer: Layer.Layer<TransactionalStream, never, ArrowFlight | StateStore> = Layer.effect(
  TransactionalStream,
  make
)
```

**streamTransactional() Logic:**

1. Get StateStore from context via `yield* StateStore`
2. Create StateActor wrapping the store
3. Get watermark for resume cursor
4. Detect rewind: `next > watermark[0] + 1` or `next > 0` (no watermark)
5. Get protocol stream with resume watermark
6. Emit Rewind if needed, then map protocol messages through actor.execute()

## Layer Composition

Users compose layers to wire up the full dependency graph:

```typescript
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { TransactionalStream } from "@edgeandnode/amp/transactional-stream"
import { InMemoryStateStore } from "@edgeandnode/amp/transactional-stream/memory-store"
import { ArrowFlight, Transport } from "@edgeandnode/amp"

// =============================================================================
// Example 1: Development/Testing with InMemoryStateStore
// =============================================================================

const DevLayer = TransactionalStream.layer.pipe(
  Layer.provide(InMemoryStateStore.layer),
  Layer.provide(ArrowFlight.layer),
  Layer.provide(MyTransportLayer)
)

const program = Effect.gen(function*() {
  const stream = yield* TransactionalStream

  yield* stream.forEach(
    "SELECT * FROM eth.logs",
    { retention: 128 },
    (event) => Effect.gen(function*() {
      switch (event._tag) {
        case "Data":
          yield* processData(event.id, event.data, event.ranges)
          break
        case "Undo":
          yield* rollback(event.invalidate)
          break
        case "Watermark":
          yield* checkpoint(event.id, event.ranges)
          break
      }
    })
  )
})

Effect.runPromise(program.pipe(Effect.provide(DevLayer)))

// =============================================================================
// Example 2: Production with Persistent StateStore (future)
// =============================================================================

import { IndexedDBStateStore } from "@edgeandnode/amp-store-indexeddb"

const ProdLayer = TransactionalStream.layer.pipe(
  Layer.provide(IndexedDBStateStore.layer({ dbName: "amp-state" })),
  Layer.provide(ArrowFlight.layer),
  Layer.provide(MyTransportLayer)
)

// =============================================================================
// Example 3: Testing with Pre-populated State
// =============================================================================

const testState: StateSnapshot = {
  buffer: [[5 as TransactionId, [{ network: "eth", ... }]]],
  next: 10 as TransactionId
}

const TestLayer = TransactionalStream.layer.pipe(
  Layer.provide(InMemoryStateStore.layerWithState(testState)),
  Layer.provide(ArrowFlight.layer),
  Layer.provide(MockTransportLayer)
)

// =============================================================================
// Example 4: Manual Stream Control with CommitHandle
// =============================================================================

const manualProgram = Effect.gen(function*() {
  const stream = yield* TransactionalStream

  const txStream = stream.streamTransactional(
    "SELECT * FROM eth.logs",
    { retention: 128 }
  )

  yield* txStream.pipe(
    Stream.runForEach(([event, commitHandle]) =>
      Effect.gen(function*() {
        // Process event
        yield* processEvent(event)

        // Manually decide when to commit
        if (shouldCommit(event)) {
          yield* commitHandle.commit()
        }
        // If we don't commit and crash, this event will be replayed via Rewind
      })
    )
  )
})
```

## Implementation Order

1. **types.ts** - TransactionId, UndoCause, TransactionEvent schemas
2. **errors.ts** - StateStoreError, UnrecoverableReorgError, PartialReorgError
3. **state-store.ts** - StateStore interface, StateSnapshot, Commit types
4. **algorithms.ts** - findRecoveryPoint, findPruningPoint (with tests)
5. **memory-store.ts** - InMemoryStateStore with Ref
6. **commit-handle.ts** - CommitHandle interface
7. **state-actor.ts** - StateActor service with execute() logic
8. **stream.ts** - TransactionalStream service
9. **index.ts** - Public exports
10. **Tests** - Port Rust test scenarios

## Test Plan

### algorithms.test.ts

- findRecoveryPoint: empty buffer, partial buffer, all affected, multi-network
- findPruningPoint: empty buffer, within retention, outside retention

### reorg.test.ts (port from Rust)

- `reorg_invalidates_affected_batches`
- `reorg_invalidates_multiple_consecutive_batches`
- `reorg_does_not_invalidate_unaffected_batches`
- `multi_network_reorg_partial_invalidation`
- `consecutive_reorgs_cumulative_invalidation`
- `reorg_with_backwards_jump_succeeds`

### rewind.test.ts

- Uncommitted after watermark triggers rewind
- Multiple uncommitted batches
- No rewind when fully committed
- Rewind with empty buffer (early crash)

### integration.test.ts

- Full stream lifecycle with InMemoryStateStore
- Auto-commit via forEach()
- Manual commit via CommitHandle

## Critical Files to Modify/Reference

| File                                                    | Purpose                                                   |
| ------------------------------------------------------- | --------------------------------------------------------- |
| `packages/amp/src/arrow-flight.ts`                      | Pattern for Layer.effect, contains streamProtocol to wrap |
| `packages/amp/src/protocol-stream/messages.ts`          | ProtocolMessage, InvalidationRange to consume             |
| `packages/amp/src/protocol-stream/errors.ts`            | ProtocolStreamError to include in union                   |
| `packages/amp/src/models.ts`                            | BlockRange type                                           |
| `.repos/amp/crates/clients/flight/src/transactional.rs` | Rust source to port                                       |

## Verification

1. Run `pnpm vitest run packages/amp/test/transactional-stream/` - all tests pass
2. Run `pnpm oxlint packages/amp/src/transactional-stream/` - no lint errors
3. Run `pnpm dprint check` - formatting correct
4. Verify reorg test parity with Rust scenarios
