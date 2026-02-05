/**
 * Protocol Stream Validation Tests
 *
 * Tests for the validation functions used by the ProtocolStream to ensure
 * protocol invariants are maintained.
 */
import {
  DuplicateNetworkError,
  GapError,
  HashMismatchOnConsecutiveBlocksError,
  InvalidPrevHashError,
  InvalidReorgError,
  MissingPrevHashError,
  NetworkCountChangedError,
  UnexpectedNetworkError,
  validateAll,
  validateConsecutiveness,
  validateNetworks,
  validatePrevHash
} from "@edgeandnode/amp/protocol-stream"
import { describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import type { BlockHash, BlockNumber, BlockRange, Network } from "@edgeandnode/amp/models"

// =============================================================================
// Test Helpers
// =============================================================================

const ZERO_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000" as BlockHash
const HASH_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" as BlockHash
const HASH_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" as BlockHash
const HASH_C = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" as BlockHash

const makeBlockRange = (
  network: string,
  start: number,
  end: number,
  hash: string,
  prevHash?: string
): BlockRange => ({
  network: network as Network,
  numbers: {
    start: start as BlockNumber,
    end: end as BlockNumber
  },
  hash: hash as BlockHash,
  prevHash: prevHash as BlockHash | undefined
})

// =============================================================================
// validatePrevHash Tests
// =============================================================================

describe("validatePrevHash", () => {
  it.effect("allows genesis block with no prevHash", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 0, 10, HASH_A)
      const result = yield* validatePrevHash(range).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("allows genesis block with zero prevHash", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 0, 10, HASH_A, ZERO_HASH)
      const result = yield* validatePrevHash(range).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("rejects genesis block with non-zero prevHash", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 0, 10, HASH_A, HASH_B)
      const result = yield* validatePrevHash(range).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(InvalidPrevHashError)
      }
    }))

  it.effect("allows non-genesis block with valid prevHash", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 100, 110, HASH_A, HASH_B)
      const result = yield* validatePrevHash(range).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("rejects non-genesis block with no prevHash", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 100, 110, HASH_A)
      const result = yield* validatePrevHash(range).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(MissingPrevHashError)
      }
    }))

  it.effect("rejects non-genesis block with zero prevHash", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 100, 110, HASH_A, ZERO_HASH)
      const result = yield* validatePrevHash(range).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(MissingPrevHashError)
      }
    }))
})

// =============================================================================
// validateNetworks Tests
// =============================================================================

describe("validateNetworks", () => {
  it.effect("allows first batch with any networks", ({ expect }) =>
    Effect.gen(function*() {
      const incoming = [
        makeBlockRange("eth", 0, 10, HASH_A),
        makeBlockRange("polygon", 0, 10, HASH_B)
      ]
      const result = yield* validateNetworks([], incoming).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("allows consistent networks across batches", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [
        makeBlockRange("eth", 0, 10, HASH_A),
        makeBlockRange("polygon", 0, 10, HASH_B)
      ]
      const incoming = [
        makeBlockRange("eth", 11, 20, HASH_B, HASH_A),
        makeBlockRange("polygon", 11, 20, HASH_C, HASH_B)
      ]
      const result = yield* validateNetworks(previous, incoming).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("rejects duplicate networks in batch", ({ expect }) =>
    Effect.gen(function*() {
      const incoming = [
        makeBlockRange("eth", 0, 10, HASH_A),
        makeBlockRange("eth", 0, 10, HASH_B)
      ]
      const result = yield* validateNetworks([], incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(DuplicateNetworkError)
      }
    }))

  it.effect("rejects network count change", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [
        makeBlockRange("eth", 0, 10, HASH_A)
      ]
      const incoming = [
        makeBlockRange("eth", 11, 20, HASH_B, HASH_A),
        makeBlockRange("polygon", 0, 10, HASH_C)
      ]
      const result = yield* validateNetworks(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(NetworkCountChangedError)
      }
    }))

  it.effect("rejects unexpected network", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [
        makeBlockRange("eth", 0, 10, HASH_A)
      ]
      const incoming = [
        makeBlockRange("polygon", 0, 10, HASH_B)
      ]
      const result = yield* validateNetworks(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(UnexpectedNetworkError)
      }
    }))
})

// =============================================================================
// validateConsecutiveness Tests
// =============================================================================

describe("validateConsecutiveness", () => {
  it.effect("allows first batch without validation", ({ expect }) =>
    Effect.gen(function*() {
      const incoming = [makeBlockRange("eth", 0, 10, HASH_A)]
      const result = yield* validateConsecutiveness([], incoming).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("allows consecutive blocks with matching hash chain", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, HASH_A)]
      const incoming = [makeBlockRange("eth", 11, 20, HASH_B, HASH_A)]
      const result = yield* validateConsecutiveness(previous, incoming).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("allows identical ranges (watermark repeat)", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 0, 10, HASH_A)
      const result = yield* validateConsecutiveness([range], [range]).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("allows backwards jump with hash mismatch (reorg)", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, HASH_A)]
      // Backwards jump (start=5 < prev.end+1=11) with different hash chain
      const incoming = [makeBlockRange("eth", 5, 12, HASH_C, HASH_B)]
      const result = yield* validateConsecutiveness(previous, incoming).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("rejects consecutive blocks with hash mismatch", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, HASH_A)]
      // Consecutive (start=11 == prev.end+1=11) but prevHash doesn't match
      const incoming = [makeBlockRange("eth", 11, 20, HASH_C, HASH_B)]
      const result = yield* validateConsecutiveness(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(HashMismatchOnConsecutiveBlocksError)
      }
    }))

  it.effect("rejects backwards jump with matching hash (invalid reorg)", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, HASH_A)]
      // Backwards jump but same hash chain - invalid
      const incoming = [makeBlockRange("eth", 5, 12, HASH_B, HASH_A)]
      const result = yield* validateConsecutiveness(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(InvalidReorgError)
      }
    }))

  it.effect("rejects forward gap", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, HASH_A)]
      // Gap: start=15 > prev.end+1=11
      const incoming = [makeBlockRange("eth", 15, 20, HASH_B, HASH_A)]
      const result = yield* validateConsecutiveness(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
      if (Either.isLeft(result)) {
        expect(result.left).toBeInstanceOf(GapError)
        expect((result.left as GapError).missingStart).toBe(11)
        expect((result.left as GapError).missingEnd).toBe(14)
      }
    }))
})

// =============================================================================
// validateAll Tests
// =============================================================================

describe("validateAll", () => {
  it.effect("passes all validations for valid consecutive batch", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 100, 110, HASH_A, HASH_B)]
      const incoming = [makeBlockRange("eth", 111, 120, HASH_B, HASH_A)]
      const result = yield* validateAll(previous, incoming).pipe(Effect.either)
      expect(Either.isRight(result)).toBe(true)
    }))

  it.effect("fails on prevHash validation", ({ expect }) =>
    Effect.gen(function*() {
      const incoming = [makeBlockRange("eth", 100, 110, HASH_A)] // Missing prevHash
      const result = yield* validateAll([], incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
    }))

  it.effect("fails on network validation", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 100, 110, HASH_A, HASH_B)]
      const incoming = [makeBlockRange("polygon", 100, 110, HASH_A, HASH_B)]
      const result = yield* validateAll(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
    }))

  it.effect("fails on consecutiveness validation", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 100, 110, HASH_A, HASH_B)]
      // Gap: start=115 > prev.end+1=111
      const incoming = [makeBlockRange("eth", 115, 120, HASH_B, HASH_A)]
      const result = yield* validateAll(previous, incoming).pipe(Effect.either)
      expect(Either.isLeft(result)).toBe(true)
    }))
})
