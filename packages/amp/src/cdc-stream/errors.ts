/**
 * Error types for the CdcStream.
 *
 * @module
 */
import * as Schema from "effect/Schema"
import type { TransactionalStreamError } from "../transactional-stream/errors.ts"

// =============================================================================
// BatchStore Errors
// =============================================================================

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

// =============================================================================
// Combined Error Type
// =============================================================================

/**
 * Union of all CDC stream errors.
 */
export type CdcStreamError =
  | BatchStoreError
  | TransactionalStreamError
