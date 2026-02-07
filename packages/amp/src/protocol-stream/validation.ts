/**
 * This module contains validation functions for the Amp protocol.
 *
 * These functions validate protocol invariants to ensure the server is
 * sending well-formed data. The validation logic is ported from the Rust
 * implementation in `.repos/amp/crates/clients/flight/validation.rs`.
 *
 * Three validations are performed:
 * 1. validatePrevHash - Validates prevHash based on block position
 * 2. validateNetworks - Validates network consistency across batches
 * 3. validateConsecutiveness - Validates block range progression
 *
 * @module
 */
import * as Effect from "effect/Effect"
import type { BlockNumber, BlockRange } from "../core/domain.ts"
import {
  DuplicateNetworkError,
  GapError,
  HashMismatchOnConsecutiveBlocksError,
  InvalidPrevHashError,
  InvalidReorgError,
  MissingPrevHashError,
  NetworkCountChangedError,
  UnexpectedNetworkError,
  type ValidationError
} from "./errors.ts"

/**
 * The zero hash used to represent "no previous hash" for genesis blocks.
 */
const ZERO_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000"

/**
 * Checks if a hash is a zero hash (all zeros).
 */
const isZeroHash = (hash: string): boolean => hash === ZERO_HASH

/**
 * Validates the prevHash field of a block range.
 *
 * Rules:
 * - Genesis blocks (start = 0): prevHash must be undefined or zero hash
 * - Non-genesis blocks (start > 0): prevHash must be defined and non-zero
 *
 * @param range - The block range to validate.
 * @returns An Effect that succeeds if valid, or fails with a validation error.
 */
export const validatePrevHash = Effect.fnUntraced(
  function*(range: BlockRange): Effect.fn.Return<void, MissingPrevHashError | InvalidPrevHashError> {
    const isGenesis = range.numbers.start === 0

    if (isGenesis) {
      // Genesis blocks must have no prevHash or a zero hash
      if (range.prevHash !== undefined && !isZeroHash(range.prevHash)) {
        return yield* new InvalidPrevHashError({ network: range.network })
      }
    } else {
      // Non-genesis blocks must have a non-zero prevHash
      if (range.prevHash === undefined) {
        return yield* new MissingPrevHashError({
          network: range.network,
          block: range.numbers.start
        })
      }
      if (isZeroHash(range.prevHash)) {
        return yield* new MissingPrevHashError({
          network: range.network,
          block: range.numbers.start
        })
      }
    }
  }
)

/**
 * Validates network consistency between previous and incoming batches.
 *
 * Rules:
 * - No duplicate networks within a single batch
 * - Network set must remain stable across batches (count and names)
 *
 * @param previous - The previous batch's block ranges (empty for first batch).
 * @param incoming - The incoming batch's block ranges.
 * @returns An Effect that succeeds if valid, or fails with a validation error.
 */
export const validateNetworks = Effect.fnUntraced(
  function*(
    previous: ReadonlyArray<BlockRange>,
    incoming: ReadonlyArray<BlockRange>
  ): Effect.fn.Return<void, DuplicateNetworkError | NetworkCountChangedError | UnexpectedNetworkError> {
    // Check for duplicate networks in incoming batch
    const incomingNetworks = new Set<string>()
    for (const range of incoming) {
      if (incomingNetworks.has(range.network)) {
        return yield* new DuplicateNetworkError({ network: range.network })
      }
      incomingNetworks.add(range.network)
    }

    // If this is the first batch, no further validation needed
    if (previous.length === 0) {
      return
    }

    // Check network count matches
    if (previous.length !== incoming.length) {
      return yield* new NetworkCountChangedError({
        expected: previous.length,
        actual: incoming.length
      })
    }

    // Check all incoming networks were in previous batch
    const previousNetworks = new Set(previous.map((r) => r.network))
    for (const range of incoming) {
      if (!previousNetworks.has(range.network)) {
        return yield* new UnexpectedNetworkError({ network: range.network })
      }
    }
  }
)

/**
 * Checks if two block ranges are equal.
 */
export const blockRangeEquals = (a: BlockRange, b: BlockRange): boolean =>
  a.network === b.network &&
  a.numbers.start === b.numbers.start &&
  a.numbers.end === b.numbers.end &&
  a.hash === b.hash &&
  a.prevHash === b.prevHash

/**
 * Validates block range consecutiveness and hash chain integrity.
 *
 * For each network, validates based on block range position:
 * 1. Consecutive (start === prev.end + 1): Hash chain MUST match
 * 2. Backwards jump (start < prev.end + 1): Hash chain MUST mismatch (reorg)
 * 3. Forward gap (start > prev.end + 1): Always protocol violation
 * 4. Identical range: Allowed for watermark repeats
 *
 * @param previous - The previous batch's block ranges.
 * @param incoming - The incoming batch's block ranges.
 * @returns An Effect that succeeds if valid, or fails with a validation error.
 */
export const validateConsecutiveness = Effect.fnUntraced(
  function*(
    previous: ReadonlyArray<BlockRange>,
    incoming: ReadonlyArray<BlockRange>
  ): Effect.fn.Return<void, HashMismatchOnConsecutiveBlocksError | InvalidReorgError | GapError> {
    // If this is the first batch, no consecutiveness check needed
    if (previous.length === 0) {
      return
    }

    for (const incomingRange of incoming) {
      // Find the previous range for this network
      const prevRange = previous.find((p) => p.network === incomingRange.network)
      if (!prevRange) {
        // New network - should have been caught by validateNetworks
        continue
      }

      // Identical ranges are allowed (watermarks can repeat)
      if (blockRangeEquals(incomingRange, prevRange)) {
        continue
      }

      const incomingStart = incomingRange.numbers.start
      const prevEnd = prevRange.numbers.end
      const expectedNextStart = (prevEnd + 1) as BlockNumber

      if (incomingStart === expectedNextStart) {
        // Consecutive blocks: hash chain must match
        // incoming.prevHash must equal prev.hash
        if (incomingRange.prevHash !== prevRange.hash) {
          return yield* new HashMismatchOnConsecutiveBlocksError({
            network: incomingRange.network,
            expectedHash: prevRange.hash,
            actualPrevHash: incomingRange.prevHash ?? "undefined"
          })
        }
      } else if (incomingStart < expectedNextStart) {
        // Backwards jump: indicates a reorg
        // Hash chain MUST mismatch for a valid reorg
        if (incomingRange.prevHash === prevRange.hash) {
          return yield* new InvalidReorgError({ network: incomingRange.network })
        }
        // Hash mismatch is expected for reorg - this is valid
      } else {
        // Forward gap: always a protocol violation
        return yield* new GapError({
          network: incomingRange.network,
          missingStart: expectedNextStart,
          missingEnd: (incomingStart - 1) as BlockNumber
        })
      }
    }
  }
)

/**
 * Runs all validation checks on incoming block ranges.
 *
 * @param previous - The previous batch's block ranges.
 * @param incoming - The incoming batch's block ranges.
 * @returns An Effect that succeeds if all validations pass.
 */
export const validateAll = Effect.fnUntraced(
  function*(
    previous: ReadonlyArray<BlockRange>,
    incoming: ReadonlyArray<BlockRange>
  ): Effect.fn.Return<void, ValidationError> {
    // Validate prevHash for all incoming ranges
    for (const range of incoming) {
      yield* validatePrevHash(range)
    }

    // Validate network consistency
    yield* validateNetworks(previous, incoming)

    // Validate consecutiveness
    yield* validateConsecutiveness(previous, incoming)
  }
)
