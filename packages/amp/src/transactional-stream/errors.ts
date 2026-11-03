/**
 * Error types for the TransactionalStream.
 *
 * @module
 */
import * as Schema from "effect/Schema"
import type { ProtocolStreamError } from "../protocol-stream/errors.ts"
import { InvalidationRange } from "../protocol-stream/messages.ts"

// =============================================================================
// StateStore Errors
// =============================================================================

/**
 * Error from StateStore operations.
 */
export class StateStoreError extends Schema.TaggedError<StateStoreError>("Amp/TransactionalStream/StateStoreError")(
  "StateStoreError",
  {
    reason: Schema.String,
    operation: Schema.Literals(["advance", "commit", "truncate", "load"]),
    cause: Schema.optional(Schema.Defect())
  }
) {}

// =============================================================================
// Reorg Errors
// =============================================================================

/**
 * Unrecoverable reorg - all buffered watermarks are affected.
 * This occurs when a reorg is so deep that there's no valid recovery point.
 * The stream must be restarted with fresh state.
 */
export class UnrecoverableReorgError extends Schema.TaggedError<UnrecoverableReorgError>(
  "Amp/TransactionalStream/UnrecoverableReorgError"
)("UnrecoverableReorgError", {
  reason: Schema.String
}) {}

/**
 * Partial reorg - recovery point doesn't align with invalidation boundary.
 * This occurs when a reorg point falls in the middle of a watermark's block range.
 * TransactionalStream recovers from it by undoing back to an earlier watermark and reconnecting from there.
 * It only reaches consumers when no buffered watermark ends before the reorg. The stream must then be restarted
 * with fresh state.
 */
export class PartialReorgError extends Schema.TaggedError<PartialReorgError>(
  "Amp/TransactionalStream/PartialReorgError"
)("PartialReorgError", {
  reason: Schema.String,
  network: Schema.String,
  invalidation: Schema.Array(InvalidationRange)
}) {}

// =============================================================================
// Combined Error Type
// =============================================================================

/**
 * Union of all transactional stream errors.
 */
export type TransactionalStreamError =
  | StateStoreError
  | UnrecoverableReorgError
  | PartialReorgError
  | ProtocolStreamError
