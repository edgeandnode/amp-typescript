/**
 * This module contains error definitions for the ProtocolStream service.
 *
 * Errors are organized into two categories:
 * - ValidationError: Protocol invariant violations detected during stream processing
 * - ProtocolStreamError: Top-level errors that can occur in the ProtocolStream service
 *
 * @module
 */
import * as Schema from "effect/Schema"

// =============================================================================
// Validation Errors
// =============================================================================

/**
 * Represents an error that occurs when a batch contains duplicate networks.
 *
 * Each batch should contain at most one range per network.
 */
export class DuplicateNetworkError extends Schema.TaggedError<DuplicateNetworkError>(
  "Amp/ProtocolStream/DuplicateNetworkError"
)("DuplicateNetworkError", {
  /**
   * The network that appeared more than once in the batch.
   */
  network: Schema.String
}) {
  override get message(): string {
    return `Duplicate network in batch: ${this.network}`
  }
}

/**
 * Represents an error that occurs when the number of networks changes between batches.
 *
 * The network set must remain stable across all batches in a stream.
 */
export class NetworkCountChangedError extends Schema.TaggedError<NetworkCountChangedError>(
  "Amp/ProtocolStream/NetworkCountChangedError"
)("NetworkCountChangedError", {
  /**
   * The expected number of networks (from previous batch).
   */
  expected: Schema.Number,
  /**
   * The actual number of networks received.
   */
  actual: Schema.Number
}) {
  override get message(): string {
    return `Network count changed: expected ${this.expected}, got ${this.actual}`
  }
}

/**
 * Represents an error that occurs when an unexpected network appears in a batch.
 *
 * All networks must be established in the first batch and remain consistent.
 */
export class UnexpectedNetworkError extends Schema.TaggedError<UnexpectedNetworkError>(
  "Amp/ProtocolStream/UnexpectedNetworkError"
)("UnexpectedNetworkError", {
  /**
   * The unexpected network that was encountered.
   */
  network: Schema.String
}) {
  override get message(): string {
    return `Unexpected network in batch: ${this.network}`
  }
}

/**
 * Represents an error that occurs when a non-genesis block is missing its prevHash.
 *
 * Non-genesis blocks (start > 0) must have a prevHash to enable hash chain validation.
 */
export class MissingPrevHashError extends Schema.TaggedError<MissingPrevHashError>(
  "Amp/ProtocolStream/MissingPrevHashError"
)("MissingPrevHashError", {
  /**
   * The network where the missing prevHash was detected.
   */
  network: Schema.String,
  /**
   * The block number that is missing a prevHash.
   */
  block: Schema.Number
}) {
  override get message(): string {
    return `Missing prevHash for non-genesis block ${this.block} on network ${this.network}`
  }
}

/**
 * Represents an error that occurs when a genesis block has an invalid prevHash.
 *
 * Genesis blocks (start = 0) must have either no prevHash or a zero hash.
 */
export class InvalidPrevHashError extends Schema.TaggedError<InvalidPrevHashError>(
  "Amp/ProtocolStream/InvalidPrevHashError"
)("InvalidPrevHashError", {
  /**
   * The network where the invalid prevHash was detected.
   */
  network: Schema.String
}) {
  override get message(): string {
    return `Genesis block has invalid prevHash on network ${this.network}`
  }
}

/**
 * Represents an error that occurs when consecutive blocks have mismatched hashes.
 *
 * For consecutive blocks (incoming.start === prev.end + 1), the incoming prevHash
 * must match the previous block's hash.
 */
export class HashMismatchOnConsecutiveBlocksError extends Schema.TaggedError<HashMismatchOnConsecutiveBlocksError>(
  "Amp/ProtocolStream/HashMismatchOnConsecutiveBlocksError"
)("HashMismatchOnConsecutiveBlocksError", {
  /**
   * The network where the hash mismatch was detected.
   */
  network: Schema.String,
  /**
   * The expected hash (from the previous block).
   */
  expectedHash: Schema.String,
  /**
   * The actual prevHash from the incoming block.
   */
  actualPrevHash: Schema.String
}) {
  override get message(): string {
    return `Hash mismatch on consecutive blocks for network ${this.network}: expected ${this.expectedHash}, got ${this.actualPrevHash}`
  }
}

/**
 * Represents an error that occurs when a backwards jump has matching hashes.
 *
 * A backwards jump (incoming.start < prev.end + 1) indicates a reorg, which
 * requires different hashes. If hashes match, it's an invalid protocol state.
 */
export class InvalidReorgError extends Schema.TaggedError<InvalidReorgError>(
  "Amp/ProtocolStream/InvalidReorgError"
)("InvalidReorgError", {
  /**
   * The network where the invalid reorg was detected.
   */
  network: Schema.String
}) {
  override get message(): string {
    return `Invalid reorg detected on network ${this.network}: backwards jump with matching hashes`
  }
}

/**
 * Represents an error that occurs when there is a gap in block numbers.
 *
 * Forward gaps (incoming.start > prev.end + 1) are always protocol violations.
 */
export class GapError extends Schema.TaggedError<GapError>(
  "Amp/ProtocolStream/GapError"
)("GapError", {
  /**
   * The network where the gap was detected.
   */
  network: Schema.String,
  /**
   * The first missing block number (prev.end + 1).
   */
  missingStart: Schema.Number,
  /**
   * The last missing block number (incoming.start - 1).
   */
  missingEnd: Schema.Number
}) {
  override get message(): string {
    return `Gap in block numbers for network ${this.network}: missing blocks ${this.missingStart} to ${this.missingEnd}`
  }
}

/**
 * Union type representing all possible validation errors.
 */
export type ValidationError =
  | DuplicateNetworkError
  | NetworkCountChangedError
  | UnexpectedNetworkError
  | MissingPrevHashError
  | InvalidPrevHashError
  | HashMismatchOnConsecutiveBlocksError
  | InvalidReorgError
  | GapError

// =============================================================================
// Protocol Stream Errors
// =============================================================================

/**
 * Represents a validation error wrapped for the ProtocolStream service.
 */
export class ProtocolValidationError extends Schema.TaggedError<ProtocolValidationError>(
  "Amp/ProtocolStream/ProtocolValidationError"
)("ProtocolValidationError", {
  /**
   * The underlying validation error.
   */
  cause: Schema.Defect
}) {
  override get message(): string {
    const cause = this.cause as ValidationError
    return `Protocol validation failed: ${cause.message}`
  }
}

/**
 * Represents an error from the underlying Arrow Flight stream.
 */
export class ProtocolArrowFlightError extends Schema.TaggedError<ProtocolArrowFlightError>(
  "Amp/ProtocolStream/ProtocolArrowFlightError"
)("ProtocolArrowFlightError", {
  /**
   * The underlying Arrow Flight error.
   */
  cause: Schema.Defect
}) {}

/**
 * Union type representing all possible ProtocolStream errors.
 */
export type ProtocolStreamError =
  | ProtocolValidationError
  | ProtocolArrowFlightError
