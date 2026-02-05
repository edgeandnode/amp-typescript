/**
 * This module contains the protocol message types emitted by the ProtocolStream.
 *
 * The ProtocolStream transforms raw Arrow Flight responses into three types of
 * protocol messages:
 * - Data: New data to process
 * - Reorg: Chain reorganization detected
 * - Watermark: Confirmed completion marker
 *
 * @module
 */
import * as Schema from "effect/Schema"
import { BlockNumber, BlockRange, Network } from "../models.ts"

// =============================================================================
// Invalidation Range
// =============================================================================

/**
 * Represents a range of blocks that must be invalidated due to a reorg.
 *
 * When a chain reorganization is detected, this type describes which blocks
 * on a specific network need to be considered invalid.
 */
export const InvalidationRange = Schema.Struct({
  /**
   * The network where blocks need to be invalidated.
   */
  network: Network,
  /**
   * The start of the invalidation range (inclusive).
   */
  start: BlockNumber,
  /**
   * The end of the invalidation range (inclusive).
   */
  end: BlockNumber
}).annotations({
  identifier: "InvalidationRange",
  description: "A range of blocks that must be invalidated due to a reorg"
})
export type InvalidationRange = typeof InvalidationRange.Type

/**
 * Creates an InvalidationRange.
 *
 * @param network - The network where blocks need to be invalidated.
 * @param start - The start of the invalidation range (inclusive).
 * @param end - The end of the invalidation range (inclusive).
 * @returns An InvalidationRange instance.
 */
export const makeInvalidationRange = (
  network: string,
  start: number,
  end: number
): InvalidationRange => ({
  network: network as typeof Network.Type,
  start: start as typeof BlockNumber.Type,
  end: end as typeof BlockNumber.Type
})

/**
 * Checks if a block range overlaps with an invalidation range.
 *
 * Returns true if the block range is on the same network and has any
 * overlapping block numbers with the invalidation range.
 *
 * @param invalidation - The invalidation range to check against.
 * @param range - The block range to check.
 * @returns True if the ranges overlap, false otherwise.
 */
export const invalidates = (
  invalidation: InvalidationRange,
  range: typeof BlockRange.Type
): boolean => {
  if (invalidation.network !== range.network) {
    return false
  }
  // Check for no overlap: invalidation ends before range starts OR range ends before invalidation starts
  const noOverlap = invalidation.end < range.numbers.start || range.numbers.end < invalidation.start
  return !noOverlap
}

// =============================================================================
// Protocol Messages
// =============================================================================

/**
 * Represents new data received from the server.
 *
 * Contains a batch of records and the block ranges they cover.
 */
export const ProtocolMessageData = Schema.TaggedStruct("Data", {
  /**
   * The decoded record batch data as an array of records.
   */
  data: Schema.Array(Schema.Record({
    key: Schema.String,
    value: Schema.Unknown
  })),
  /**
   * The block ranges covered by this batch.
   */
  ranges: Schema.Array(BlockRange)
}).annotations({
  identifier: "ProtocolMessage.Data",
  description: "New data to process from the protocol stream"
})
export type ProtocolMessageData = typeof ProtocolMessageData.Type

/**
 * Represents a chain reorganization detected by the protocol.
 *
 * When a reorg is detected, this message contains:
 * - The previous block ranges that were known
 * - The new incoming block ranges from the server
 * - The specific ranges that need to be invalidated
 */
export const ProtocolMessageReorg = Schema.TaggedStruct("Reorg", {
  /**
   * The previous block ranges that were known before this message.
   */
  previous: Schema.Array(BlockRange),
  /**
   * The new incoming block ranges from the server.
   */
  incoming: Schema.Array(BlockRange),
  /**
   * The ranges that need to be invalidated due to the reorg.
   */
  invalidation: Schema.Array(InvalidationRange)
}).annotations({
  identifier: "ProtocolMessage.Reorg",
  description: "Chain reorganization detected"
})
export type ProtocolMessageReorg = typeof ProtocolMessageReorg.Type

/**
 * Represents a watermark indicating ranges are confirmed complete.
 *
 * Watermarks are emitted when `rangesComplete` is true and the batch
 * contains no data. They indicate that the specified block ranges
 * have been fully processed and can be considered stable.
 */
export const ProtocolMessageWatermark = Schema.TaggedStruct("Watermark", {
  /**
   * The block ranges that are confirmed complete.
   */
  ranges: Schema.Array(BlockRange)
}).annotations({
  identifier: "ProtocolMessage.Watermark",
  description: "Watermark indicating ranges are confirmed complete"
})
export type ProtocolMessageWatermark = typeof ProtocolMessageWatermark.Type

/**
 * Discriminated union of all protocol message types.
 *
 * Use pattern matching on the `_tag` field to handle each message type:
 *
 * ```typescript
 * switch (message._tag) {
 *   case "Data":
 *     // Process new data
 *     break;
 *   case "Reorg":
 *     // Handle chain reorganization
 *     break;
 *   case "Watermark":
 *     // Checkpoint progress
 *     break;
 * }
 * ```
 */
export const ProtocolMessage = Schema.Union(
  ProtocolMessageData,
  ProtocolMessageReorg,
  ProtocolMessageWatermark
).annotations({
  identifier: "ProtocolMessage",
  description: "A message from the protocol stream"
})
export type ProtocolMessage = typeof ProtocolMessage.Type

// =============================================================================
// Constructors
// =============================================================================

/**
 * Creates a Data protocol message.
 *
 * @param records - The decoded record batch data.
 * @param ranges - The block ranges covered by the data.
 * @returns A Data protocol message.
 */
export const data = (
  records: ReadonlyArray<Record<string, unknown>>,
  ranges: ReadonlyArray<typeof BlockRange.Type>
): ProtocolMessageData => ({
  _tag: "Data",
  data: records as Array<Record<string, unknown>>,
  ranges: ranges as Array<typeof BlockRange.Type>
})

/**
 * Creates a Reorg protocol message.
 *
 * @param previous - The previous known block ranges.
 * @param incoming - The new incoming block ranges.
 * @param invalidation - The ranges that need to be invalidated.
 * @returns A Reorg protocol message.
 */
export const reorg = (
  previous: ReadonlyArray<typeof BlockRange.Type>,
  incoming: ReadonlyArray<typeof BlockRange.Type>,
  invalidation: ReadonlyArray<InvalidationRange>
): ProtocolMessageReorg => ({
  _tag: "Reorg",
  previous: previous as Array<typeof BlockRange.Type>,
  incoming: incoming as Array<typeof BlockRange.Type>,
  invalidation: invalidation as Array<InvalidationRange>
})

/**
 * Creates a Watermark protocol message.
 *
 * @param ranges - The block ranges that are confirmed complete.
 * @returns A Watermark protocol message.
 */
export const watermark = (
  ranges: ReadonlyArray<typeof BlockRange.Type>
): ProtocolMessageWatermark => ({
  _tag: "Watermark",
  ranges: ranges as Array<typeof BlockRange.Type>
})
