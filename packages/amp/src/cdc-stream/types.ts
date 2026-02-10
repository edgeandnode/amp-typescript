/**
 * Core type definitions for the CdcStream.
 *
 * @module
 */
import type * as Effect from "effect/Effect"
import type { BlockRange } from "../core/domain.ts"
import type { TransactionId } from "../transactional-stream/types.ts"
import type { BatchStoreError } from "./errors.ts"

// =============================================================================
// DeleteBatchIterator
// =============================================================================

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
   * Returns undefined when all batches have been yielded.
   */
  readonly next: Effect.Effect<
    readonly [TransactionId, ReadonlyArray<Record<string, unknown>>] | undefined,
    BatchStoreError
  >
}

// =============================================================================
// CdcEvent
// =============================================================================

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
