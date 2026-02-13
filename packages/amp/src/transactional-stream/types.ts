/**
 * Core type definitions for the TransactionalStream.
 *
 * @module
 */
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { BlockRange } from "../core/domain.ts"
import { InvalidationRange } from "../protocol-stream/messages.ts"

// =============================================================================
// TransactionId
// =============================================================================

/**
 * Transaction ID - monotonically increasing identifier for each event.
 * Guaranteed to be unique and never reused, even across crashes.
 */
export const TransactionId = Schema.NonNegativeInt.pipe(
  Schema.brand("Amp/TransactionalStream/TransactionId")
).annotations({
  identifier: "TransactionId",
  description: "Monotonically increasing transaction identifier"
})
export type TransactionId = typeof TransactionId.Type

// =============================================================================
// TransactionIdRange
// =============================================================================

/**
 * Inclusive range of transaction IDs for invalidation.
 */
export const TransactionIdRange = Schema.Struct({
  start: TransactionId,
  end: TransactionId
}).annotations({
  identifier: "TransactionIdRange",
  description: "Inclusive range of transaction IDs"
})
export type TransactionIdRange = typeof TransactionIdRange.Type

// =============================================================================
// UndoCause
// =============================================================================

/**
 * Cause of an Undo event - either a blockchain reorg or a rewind on restart.
 */
export const UndoCauseReorg = Schema.TaggedStruct("Reorg", {
  invalidation: Schema.Array(InvalidationRange)
}).annotations({
  identifier: "UndoCause.Reorg",
  description: "Undo caused by blockchain reorganization"
})
export type UndoCauseReorg = typeof UndoCauseReorg.Type

export const UndoCauseRewind = Schema.TaggedStruct("Rewind", {}).annotations({
  identifier: "UndoCause.Rewind",
  description: "Undo caused by rewind on restart (uncommitted transactions)"
})
export type UndoCauseRewind = typeof UndoCauseRewind.Type

export const UndoCause = Schema.Union(UndoCauseReorg, UndoCauseRewind).annotations({
  identifier: "UndoCause",
  description: "Cause of an Undo event"
})
export type UndoCause = typeof UndoCause.Type

// =============================================================================
// TransactionEvent
// =============================================================================

/**
 * Data event - new records to process.
 */
export const TransactionEventData = Schema.TaggedStruct("Data", {
  /** Transaction ID of this event */
  id: TransactionId,
  /** Decoded record batch data */
  data: Schema.Array(
    Schema.Record({
      key: Schema.String,
      value: Schema.Unknown
    })
  ),
  /** Block ranges covered by this data */
  ranges: Schema.Array(BlockRange)
}).annotations({
  identifier: "TransactionEvent.Data",
  description: "New data to process"
})
export type TransactionEventData = typeof TransactionEventData.Type

/**
 * Undo event - consumer must delete/rollback data with the invalidated IDs.
 */
export const TransactionEventUndo = Schema.TaggedStruct("Undo", {
  /** Transaction ID of this event */
  id: TransactionId,
  /** Cause of the undo (reorg or rewind) */
  cause: UndoCause,
  /** Range of transaction IDs to invalidate (inclusive) */
  invalidate: TransactionIdRange
}).annotations({
  identifier: "TransactionEvent.Undo",
  description: "Undo/rollback previously processed data"
})
export type TransactionEventUndo = typeof TransactionEventUndo.Type

/**
 * Watermark event - confirms block ranges are complete.
 */
export const TransactionEventWatermark = Schema.TaggedStruct("Watermark", {
  /** Transaction ID of this event */
  id: TransactionId,
  /** Block ranges confirmed complete */
  ranges: Schema.Array(BlockRange),
  /** Last transaction ID pruned at this watermark, if any */
  prune: Schema.OptionFromNullOr(TransactionId)
}).annotations({
  identifier: "TransactionEvent.Watermark",
  description: "Watermark confirming block ranges are complete"
})
export type TransactionEventWatermark = typeof TransactionEventWatermark.Type

/**
 * Union of all transaction event types.
 */
export const TransactionEvent = Schema.Union(
  TransactionEventData,
  TransactionEventUndo,
  TransactionEventWatermark
).annotations({
  identifier: "TransactionEvent",
  description: "Event emitted by the transactional stream"
})
export type TransactionEvent = typeof TransactionEvent.Type

// =============================================================================
// Constructors
// =============================================================================

/**
 * Create a Data transaction event.
 */
export const dataEvent = (
  id: TransactionId,
  data: ReadonlyArray<Record<string, unknown>>,
  ranges: ReadonlyArray<typeof BlockRange.Type>
): TransactionEventData => ({
  _tag: "Data",
  id,
  data: data as Array<Record<string, unknown>>,
  ranges: ranges as Array<typeof BlockRange.Type>
})

/**
 * Create an Undo transaction event.
 */
export const undoEvent = (
  id: TransactionId,
  cause: UndoCause,
  invalidate: TransactionIdRange
): TransactionEventUndo => ({
  _tag: "Undo",
  id,
  cause,
  invalidate
})

/**
 * Create a Watermark transaction event.
 */
export const watermarkEvent = (
  id: TransactionId,
  ranges: ReadonlyArray<typeof BlockRange.Type>,
  prune: TransactionId | null
): TransactionEventWatermark => ({
  _tag: "Watermark",
  id,
  ranges: ranges as Array<typeof BlockRange.Type>,
  prune: Option.fromNullable(prune)
})

/**
 * Create a Reorg cause.
 */
export const reorgCause = (
  invalidation: ReadonlyArray<typeof InvalidationRange.Type>
): UndoCauseReorg => ({
  _tag: "Reorg",
  invalidation: invalidation as Array<typeof InvalidationRange.Type>
})

/**
 * Create a Rewind cause.
 */
export const rewindCause = (): UndoCauseRewind => ({
  _tag: "Rewind"
})
