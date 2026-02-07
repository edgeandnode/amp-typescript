/**
 * StateStore service definition.
 *
 * The StateStore is defined as an Effect Context.Tag service, allowing different
 * implementations to be swapped via Layers. This mirrors the Rust pattern where
 * `StateStore` is a trait implemented by `InMemoryStateStore`, `LmdbStateStore`,
 * and `PostgresStateStore`.
 *
 * @module
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type { BlockRange } from "../core/domain.ts"
import type { StateStoreError } from "./errors.ts"
import type { TransactionId } from "./types.ts"

// =============================================================================
// Data Types
// =============================================================================

/**
 * Persisted state snapshot for crash recovery.
 * Loaded once on startup, then maintained in-memory.
 */
export interface StateSnapshot {
  /**
   * Buffer of watermarks (oldest to newest).
   * Each entry is a tuple of [transactionId, blockRanges].
   * Only watermarks are stored, not data events.
   */
  readonly buffer: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>

  /**
   * Next transaction ID to assign.
   * Pre-allocated to ensure monotonicity survives crashes.
   */
  readonly next: TransactionId
}

/**
 * Atomic state update for persistence.
 * Batches multiple watermark inserts with optional pruning.
 */
export interface Commit {
  /**
   * Watermarks to insert (oldest to newest).
   */
  readonly insert: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>

  /**
   * Last transaction ID to prune (inclusive).
   * All watermarks with ID <= this value are removed.
   */
  readonly prune: TransactionId | undefined
}

// =============================================================================
// StateStore Service Interface
// =============================================================================

/**
 * StateStore service interface - pluggable persistence for stream state.
 *
 * Different implementations provide different crash recovery guarantees:
 * - InMemoryStateStore: No persistence (development/testing)
 * - IndexedDBStateStore: Browser persistence (future)
 * - SqliteStateStore: Node.js file-based persistence (future)
 * - PostgresStateStore: Distributed persistence (future)
 */
export interface StateStoreService {
  /**
   * Pre-allocate next transaction ID.
   *
   * Called immediately after incrementing the in-memory counter.
   * Ensures ID monotonicity survives crashes - IDs are never reused.
   *
   * @param next - The next transaction ID to persist
   */
  readonly advance: (next: TransactionId) => Effect.Effect<void, StateStoreError>

  /**
   * Atomically commit watermarks and apply pruning.
   *
   * Called when user invokes CommitHandle.commit().
   * Must be idempotent - safe to call multiple times with same data.
   *
   * @param commit - The commit containing watermarks to insert and optional prune point
   */
  readonly commit: (commit: Commit) => Effect.Effect<void, StateStoreError>

  /**
   * Truncate buffer during reorg handling.
   *
   * Removes all watermarks with ID >= from.
   * Called immediately when reorg is detected (before emitting Undo).
   *
   * @param from - Remove all watermarks with ID >= this value
   */
  readonly truncate: (from: TransactionId) => Effect.Effect<void, StateStoreError>

  /**
   * Load initial state on startup.
   *
   * Called once when TransactionalStream is created.
   * Returns empty state if no prior state exists.
   */
  readonly load: Effect.Effect<StateSnapshot, StateStoreError>
}

// =============================================================================
// StateStore Context.Tag
// =============================================================================

/**
 * StateStore Context.Tag - use this to depend on StateStore in Effects.
 *
 * @example
 * ```typescript
 * const program = Effect.gen(function*() {
 *   const store = yield* StateStore
 *   const snapshot = yield* store.load
 *   // ...
 * })
 * ```
 */
export class StateStore extends Context.Tag("Amp/TransactionalStream/StateStore")<
  StateStore,
  StateStoreService
>() {}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Initial empty state for new streams or cleared stores.
 */
export const emptySnapshot: StateSnapshot = {
  buffer: [],
  next: 0 as TransactionId
}

/**
 * Create an empty commit (no-op).
 */
export const emptyCommit: Commit = {
  insert: [],
  prune: undefined
}

/**
 * Create a commit with watermarks to insert.
 */
export const makeCommit = (
  insert: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>,
  prune?: TransactionId
): Commit => ({
  insert,
  prune
})
