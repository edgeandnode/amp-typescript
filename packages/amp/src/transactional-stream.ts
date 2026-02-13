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
 *     Effect.fnUntraced(function*(event) {
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
 * const AppLayer = TransactionalStream.layer.pipe(
 *   Layer.provide(InMemoryStateStore.layer),
 *   Layer.provide(ProtocolStream.layer),
 *   Layer.provide(ArrowFlight.layer),
 *   Layer.provide(Transport.layer)
 * )
 *
 * Effect.runPromise(program.pipe(Effect.provide(AppLayer)))
 * ```
 *
 * @module
 */

// =============================================================================
// Types
// =============================================================================

export {
  // Constructors
  dataEvent,
  reorgCause,
  rewindCause,
  type TransactionEvent,
  TransactionEvent as TransactionEventSchema,
  type TransactionEventData,
  TransactionEventData as TransactionEventDataSchema,
  type TransactionEventUndo,
  TransactionEventUndo as TransactionEventUndoSchema,
  type TransactionEventWatermark,
  TransactionEventWatermark as TransactionEventWatermarkSchema,
  type TransactionId,
  TransactionId as TransactionIdSchema,
  type TransactionIdRange,
  TransactionIdRange as TransactionIdRangeSchema,
  type UndoCause,
  UndoCause as UndoCauseSchema,
  type UndoCauseReorg,
  UndoCauseReorg as UndoCauseReorgSchema,
  type UndoCauseRewind,
  UndoCauseRewind as UndoCauseRewindSchema,
  undoEvent,
  watermarkEvent
} from "./transactional-stream/types.ts"

// =============================================================================
// Errors
// =============================================================================

export {
  PartialReorgError,
  StateStoreError,
  type TransactionalStreamError,
  UnrecoverableReorgError
} from "./transactional-stream/errors.ts"

// =============================================================================
// StateStore Service
// =============================================================================

export {
  type Commit,
  emptyCommit,
  emptySnapshot,
  makeCommit,
  type StateSnapshot,
  StateStore,
  type StateStoreService
} from "./transactional-stream/state-store.ts"

// =============================================================================
// InMemoryStateStore Layer
// =============================================================================

export * as InMemoryStateStore from "./transactional-stream/memory-store.ts"

// =============================================================================
// CommitHandle
// =============================================================================

export { type CommitHandle, makeCommitHandle } from "./transactional-stream/commit-handle.ts"

// =============================================================================
// TransactionalStream Service
// =============================================================================

export {
  layer,
  TransactionalStream,
  type TransactionalStreamOptions,
  type TransactionalStreamService
} from "./transactional-stream/stream.ts"

// =============================================================================
// Algorithms (for advanced use cases and testing)
// =============================================================================

export {
  checkPartialReorg,
  compressCommits,
  findPruningPoint,
  findRecoveryPoint
} from "./transactional-stream/algorithms.ts"
