/**
 * BatchStore service — pluggable persistence for batch content.
 *
 * Stores decoded JSON batch data (not raw Arrow IPC) since the TypeScript SDK
 * works with decoded records rather than RecordBatch objects.
 *
 * @module
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type { TransactionId, TransactionIdRange } from "../transactional-stream/types.ts"
import type { BatchStoreError } from "./errors.ts"

// =============================================================================
// Service Interface
// =============================================================================

/**
 * BatchStore service interface — pluggable persistence for batch content.
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

// =============================================================================
// Context.Tag
// =============================================================================

export class BatchStore extends Context.Tag("Amp/CdcStream/BatchStore")<
  BatchStore,
  BatchStoreService
>() {}
