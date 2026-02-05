/**
 * TransactionalStream module - exactly-once semantics for data processing.
 *
 * Provides crash recovery, reorg handling, and commit control on top of
 * the stateless protocol stream.
 *
 * @example
 * ```typescript
 * import {
 *   TransactionalStream,
 *   InMemoryStateStore,
 *   type TransactionEvent,
 *   type CommitHandle
 * } from "@edgeandnode/amp/transactional-stream"
 *
 * const program = Effect.gen(function*() {
 *   const txStream = yield* TransactionalStream
 *
 *   yield* txStream.forEach(
 *     "SELECT * FROM eth.logs",
 *     { retention: 128 },
 *     (event) => Effect.gen(function*() {
 *       switch (event._tag) {
 *         case "Data":
 *           yield* processData(event.data)
 *           break
 *         case "Undo":
 *           yield* rollback(event.invalidate)
 *           break
 *         case "Watermark":
 *           yield* checkpoint(event.ranges)
 *           break
 *       }
 *     })
 *   )
 * })
 *
 * Effect.runPromise(program.pipe(
 *   Effect.provide(TransactionalStream.layer),
 *   Effect.provide(InMemoryStateStore.layer),
 *   Effect.provide(ArrowFlight.layer),
 *   Effect.provide(Transport.layer)
 * ))
 * ```
 *
 * @module
 */

// =============================================================================
// Types
// =============================================================================

export {
  type TransactionId,
  TransactionId as TransactionIdSchema,
  type TransactionIdRange,
  TransactionIdRange as TransactionIdRangeSchema,
  type TransactionEvent,
  TransactionEvent as TransactionEventSchema,
  type TransactionEventData,
  TransactionEventData as TransactionEventDataSchema,
  type TransactionEventUndo,
  TransactionEventUndo as TransactionEventUndoSchema,
  type TransactionEventWatermark,
  TransactionEventWatermark as TransactionEventWatermarkSchema,
  type UndoCause,
  UndoCause as UndoCauseSchema,
  type UndoCauseReorg,
  UndoCauseReorg as UndoCauseReorgSchema,
  type UndoCauseRewind,
  UndoCauseRewind as UndoCauseRewindSchema,
  // Constructors
  dataEvent,
  undoEvent,
  watermarkEvent,
  reorgCause,
  rewindCause
} from "./types.ts"

// =============================================================================
// Errors
// =============================================================================

export {
  StateStoreError,
  UnrecoverableReorgError,
  PartialReorgError,
  type TransactionalStreamError
} from "./errors.ts"

// =============================================================================
// StateStore Service
// =============================================================================

export {
  StateStore,
  type StateStoreService,
  type StateSnapshot,
  type Commit,
  emptySnapshot,
  emptyCommit,
  makeCommit
} from "./state-store.ts"

// =============================================================================
// InMemoryStateStore Layer
// =============================================================================

export * as InMemoryStateStore from "./memory-store.ts"

// =============================================================================
// CommitHandle
// =============================================================================

export { type CommitHandle, makeCommitHandle } from "./commit-handle.ts"

// =============================================================================
// TransactionalStream Service
// =============================================================================

export {
  TransactionalStream,
  layer,
  type TransactionalStreamService,
  type TransactionalStreamOptions
} from "./stream.ts"

// =============================================================================
// Algorithms (for advanced use cases and testing)
// =============================================================================

export {
  findRecoveryPoint,
  findPruningPoint,
  checkPartialReorg,
  compressCommits
} from "./algorithms.ts"
