/**
 * Blockchain Reorg Detection Tests
 *
 * Ported from Rust tests in:
 * `.repos/amp/crates/clients/flight/src/tests/it_blockchain_reorg_test.rs`
 *
 * These tests verify the stateless reorg detection logic used by the protocol stream.
 * Unlike the Rust implementation which maintains transaction IDs and a buffer,
 * the TypeScript version is stateless and detects reorgs purely from consecutive
 * batch comparisons.
 *
 * Key behaviors tested:
 * - Reorg detection via backwards jumps with hash mismatches
 * - Invalidation range calculation
 * - Multi-network partial reorg detection
 * - Watermark and data message generation
 */
import type { BlockHash, BlockNumber, BlockRange, Network } from "@edgeandnode/amp/core"
import { data, invalidates, makeInvalidationRange, reorg, watermark } from "@edgeandnode/amp/protocol-stream"
import { describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"

// =============================================================================
// Test Helpers - Ported from Rust utils/response.rs
// =============================================================================

/**
 * Standard test hashes for different epochs.
 * Epoch 0 = HASH_A, Epoch 1 = HASH_B, etc.
 */

/**
 * Generates a deterministic hash for a block based on network, block number, and epoch.
 * This mirrors the Rust implementation for consistent hash generation.
 */
const makeHash = (network: string, block: number, epoch: number = 0): BlockHash => {
  // Use epoch as the main distinguisher, with network and block encoded
  const epochHex = epoch.toString(16).padStart(2, "0")
  const networkHash = Array.from(network).reduce((acc, c) => (acc * 31 + c.charCodeAt(0)) & 0xffffffff, 0)
  const networkHex = networkHash.toString(16).padStart(8, "0")
  const blockHex = block.toString(16).padStart(16, "0")
  return `0x${epochHex}${networkHex}${"0".repeat(38)}${blockHex}` as BlockHash
}

/**
 * Creates a BlockRange for testing.
 * Automatically generates hashes based on network, block numbers, and epoch.
 */
const makeBlockRange = (
  network: string,
  start: number,
  end: number,
  epoch: number = 0
): BlockRange => ({
  network: network as Network,
  numbers: {
    start: start as BlockNumber,
    end: end as BlockNumber
  },
  hash: makeHash(network, end, epoch),
  prevHash: start > 0 ? makeHash(network, start - 1, epoch) : undefined
})

/**
 * Creates a BlockRange with an explicit prevHash that doesn't match the epoch.
 * Used to simulate reorgs where the hash chain doesn't match.
 */
const makeBlockRangeWithReorg = (
  network: string,
  start: number,
  end: number,
  epoch: number,
  prevHashEpoch: number
): BlockRange => ({
  network: network as Network,
  numbers: {
    start: start as BlockNumber,
    end: end as BlockNumber
  },
  hash: makeHash(network, end, epoch),
  prevHash: start > 0 ? makeHash(network, start - 1, prevHashEpoch) : undefined
})

// =============================================================================
// Reorg Detection Helper
// =============================================================================

/**
 * Detects reorgs by comparing incoming ranges to previous ranges.
 * This is a copy of the detection logic from arrow-flight.ts for testing.
 */
const detectReorgs = (
  previous: ReadonlyArray<BlockRange>,
  incoming: ReadonlyArray<BlockRange>
): ReadonlyArray<ReturnType<typeof makeInvalidationRange>> => {
  const invalidations: Array<ReturnType<typeof makeInvalidationRange>> = []

  for (const incomingRange of incoming) {
    const prevRange = previous.find((p) => p.network === incomingRange.network)
    if (!prevRange) continue

    // Skip identical ranges (watermarks can repeat)
    if (
      incomingRange.network === prevRange.network &&
      incomingRange.numbers.start === prevRange.numbers.start &&
      incomingRange.numbers.end === prevRange.numbers.end &&
      incomingRange.hash === prevRange.hash &&
      incomingRange.prevHash === prevRange.prevHash
    ) {
      continue
    }

    const incomingStart = incomingRange.numbers.start
    const prevEnd = prevRange.numbers.end

    // Detect backwards jump (reorg indicator)
    if (incomingStart < prevEnd + 1) {
      invalidations.push(
        makeInvalidationRange(
          incomingRange.network,
          incomingStart,
          Math.max(incomingRange.numbers.end, prevEnd)
        )
      )
    }
  }

  return invalidations
}

// =============================================================================
// Reorg Detection Tests - Basic Scenarios
// =============================================================================

describe("detectReorgs", () => {
  /**
   * Tests basic reorg detection when a batch arrives with a backwards jump
   * and different hash chain. The reorg should create an invalidation range.
   *
   * Corresponds to Rust test: reorg_invalidates_affected_batches
   */
  it.effect("detects reorg when backwards jump with hash mismatch occurs", ({ expect }) =>
    Effect.gen(function*() {
      // Previous: blocks 0-10 in epoch 0
      const previous = [makeBlockRange("eth", 0, 10, 0)]

      // Incoming: blocks 5-12 in epoch 1 (different hash chain, backwards jump)
      const incoming = [makeBlockRangeWithReorg("eth", 5, 12, 1, 1)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.network).toBe("eth")
      expect(invalidations[0]!.start).toBe(5) // Start of reorg
      expect(invalidations[0]!.end).toBe(12) // End covers incoming range
    }))

  /**
   * Tests that consecutive blocks with matching hash chains don't trigger reorg.
   */
  it.effect("does not detect reorg for consecutive blocks with matching hashes", ({ expect }) =>
    Effect.gen(function*() {
      // Previous: blocks 0-10 in epoch 0
      const previous = [makeBlockRange("eth", 0, 10, 0)]

      // Incoming: blocks 11-20 in epoch 0 (consecutive, same hash chain)
      const incoming = [makeBlockRange("eth", 11, 20, 0)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(0)
    }))

  /**
   * Tests that watermarks don't invalidate previous batches when block ranges
   * don't overlap. This verifies the "protected by watermark" behavior.
   *
   * Corresponds to Rust test: reorg_does_not_invalidate_unaffected_batches
   */
  it.effect("does not detect reorg for forward progress", ({ expect }) =>
    Effect.gen(function*() {
      // Previous: blocks 0-10 (finalized by watermark)
      const previous = [makeBlockRange("eth", 0, 10, 0)]

      // Incoming: blocks 11-20 (consecutive, no overlap)
      const incoming = [makeBlockRange("eth", 11, 20, 0)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(0)
    }))

  /**
   * Tests identical range handling (watermark repeats).
   */
  it.effect("does not detect reorg for identical ranges (watermark repeat)", ({ expect }) =>
    Effect.gen(function*() {
      const range = makeBlockRange("eth", 0, 10, 0)

      const invalidations = detectReorgs([range], [range])

      expect(invalidations.length).toBe(0)
    }))
})

// =============================================================================
// Multi-Network Reorg Tests
// =============================================================================

describe("detectReorgs - multi-network", () => {
  /**
   * Tests partial reorg in multi-network scenarios where only some networks
   * experience a reorg while others continue normally.
   *
   * Corresponds to Rust test: multi_network_reorg_partial_invalidation
   */
  it.effect("detects partial reorg when only one network reorgs", ({ expect }) =>
    Effect.gen(function*() {
      // Previous: both networks at blocks 0-10
      const previous = [
        makeBlockRange("eth", 0, 10, 0),
        makeBlockRange("polygon", 0, 10, 0)
      ]

      // Incoming: eth reorgs back to block 5, polygon continues normally
      const incoming = [
        makeBlockRangeWithReorg("eth", 5, 12, 1, 1), // Reorg with different epoch
        makeBlockRange("polygon", 11, 20, 0) // Normal continuation
      ]

      const invalidations = detectReorgs(previous, incoming)

      // Only eth should have an invalidation
      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.network).toBe("eth")
      expect(invalidations[0]!.start).toBe(5)
    }))

  /**
   * Tests multi-network reorg where both networks reorg.
   */
  it.effect("detects reorg on both networks when both reorg", ({ expect }) =>
    Effect.gen(function*() {
      // Previous: both networks at blocks 0-10
      const previous = [
        makeBlockRange("eth", 0, 10, 0),
        makeBlockRange("polygon", 0, 10, 0)
      ]

      // Incoming: both networks reorg
      const incoming = [
        makeBlockRangeWithReorg("eth", 5, 12, 1, 1),
        makeBlockRangeWithReorg("polygon", 7, 15, 1, 1)
      ]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(2)
      expect(invalidations.find((i) => i.network === "eth")).toBeDefined()
      expect(invalidations.find((i) => i.network === "polygon")).toBeDefined()
    }))
})

// =============================================================================
// Invalidation Range Tests
// =============================================================================

describe("invalidates", () => {
  it.effect("returns true when block range overlaps with invalidation range", ({ expect }) =>
    Effect.gen(function*() {
      const invalidation = makeInvalidationRange("eth", 10, 20)
      const range = makeBlockRange("eth", 15, 25, 0)

      expect(invalidates(invalidation, range)).toBe(true)
    }))

  it.effect("returns false when block range is before invalidation range", ({ expect }) =>
    Effect.gen(function*() {
      const invalidation = makeInvalidationRange("eth", 20, 30)
      const range = makeBlockRange("eth", 5, 15, 0)

      expect(invalidates(invalidation, range)).toBe(false)
    }))

  it.effect("returns false when block range is after invalidation range", ({ expect }) =>
    Effect.gen(function*() {
      const invalidation = makeInvalidationRange("eth", 5, 15)
      const range = makeBlockRange("eth", 20, 30, 0)

      expect(invalidates(invalidation, range)).toBe(false)
    }))

  it.effect("returns false when networks don't match", ({ expect }) =>
    Effect.gen(function*() {
      const invalidation = makeInvalidationRange("eth", 10, 20)
      const range = makeBlockRange("polygon", 10, 20, 0)

      expect(invalidates(invalidation, range)).toBe(false)
    }))

  it.effect("returns true when ranges are identical", ({ expect }) =>
    Effect.gen(function*() {
      const invalidation = makeInvalidationRange("eth", 10, 20)
      const range = makeBlockRange("eth", 10, 20, 0)

      expect(invalidates(invalidation, range)).toBe(true)
    }))

  it.effect("returns true when invalidation range contains block range", ({ expect }) =>
    Effect.gen(function*() {
      const invalidation = makeInvalidationRange("eth", 5, 30)
      const range = makeBlockRange("eth", 10, 20, 0)

      expect(invalidates(invalidation, range)).toBe(true)
    }))
})

// =============================================================================
// Protocol Message Construction Tests
// =============================================================================

describe("protocol messages", () => {
  it.effect("creates Data message with records and ranges", ({ expect }) =>
    Effect.gen(function*() {
      const records = [{ id: 1 }, { id: 2 }]
      const ranges = [makeBlockRange("eth", 0, 10, 0)]

      const message = data(records, ranges)

      expect(message._tag).toBe("Data")
      expect(message.data.length).toBe(2)
      expect(message.ranges.length).toBe(1)
    }))

  it.effect("creates Watermark message with ranges", ({ expect }) =>
    Effect.gen(function*() {
      const ranges = [makeBlockRange("eth", 0, 10, 0)]

      const message = watermark(ranges)

      expect(message._tag).toBe("Watermark")
      expect(message.ranges.length).toBe(1)
    }))

  it.effect("creates Reorg message with previous, incoming, and invalidation", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, 0)]
      const incoming = [makeBlockRangeWithReorg("eth", 5, 12, 1, 1)]
      const invalidation = [makeInvalidationRange("eth", 5, 12)]

      const message = reorg(previous, incoming, invalidation)

      expect(message._tag).toBe("Reorg")
      expect(message.previous.length).toBe(1)
      expect(message.incoming.length).toBe(1)
      expect(message.invalidation.length).toBe(1)
    }))
})

// =============================================================================
// Deep Reorg Scenarios
// =============================================================================

describe("deep reorg scenarios", () => {
  /**
   * Tests that a deep reorg (going back many blocks) correctly identifies
   * the invalidation range. In stateless detection, we only see the
   * immediate previous batch, so the invalidation range is calculated
   * based on that comparison.
   *
   * Corresponds to Rust test: reorg_invalidates_multiple_consecutive_batches
   */
  it.effect("calculates invalidation range for deep reorg", ({ expect }) =>
    Effect.gen(function*() {
      // Simulate state after multiple data batches:
      // Previous state shows blocks 31-40 were the last received
      const previous = [makeBlockRange("eth", 31, 40, 0)]

      // Incoming: reorg back to block 15 (deep reorg)
      const incoming = [makeBlockRangeWithReorg("eth", 15, 25, 1, 1)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.network).toBe("eth")
      expect(invalidations[0]!.start).toBe(15) // Reorg point
      // End is max of incoming.end (25) and previous.end (40)
      expect(invalidations[0]!.end).toBe(40)
    }))

  /**
   * Tests consecutive reorgs - multiple reorgs in sequence.
   *
   * Corresponds to Rust test: consecutive_reorgs_cumulative_invalidation
   */
  it.effect("handles consecutive reorgs correctly", ({ expect }) =>
    Effect.gen(function*() {
      // First reorg scenario
      let previous = [makeBlockRange("eth", 21, 30, 0)]
      let incoming = [makeBlockRangeWithReorg("eth", 21, 25, 1, 1)]

      let invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.start).toBe(21)
      expect(invalidations[0]!.end).toBe(30)

      // Continue with new batch after first reorg
      previous = incoming
      incoming = [makeBlockRange("eth", 26, 30, 1)]

      invalidations = detectReorgs(previous, incoming)
      expect(invalidations.length).toBe(0) // Normal continuation

      // Second reorg occurs
      previous = incoming
      incoming = [makeBlockRangeWithReorg("eth", 21, 30, 2, 2)]

      invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.start).toBe(21)
      expect(invalidations[0]!.end).toBe(30)
    }))

  /**
   * Tests reorg with backwards jump that succeeds validation.
   *
   * Corresponds to Rust test: reorg_with_backwards_jump_succeeds
   */
  it.effect("detects reorg with backwards jump", ({ expect }) =>
    Effect.gen(function*() {
      // Previous: blocks 11-20
      const previous = [makeBlockRange("eth", 11, 20, 0)]

      // Incoming: reorg back to block 15 (backwards jump from end=20 to start=15)
      const incoming = [makeBlockRangeWithReorg("eth", 15, 25, 1, 1)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.network).toBe("eth")
      expect(invalidations[0]!.start).toBe(15)
      expect(invalidations[0]!.end).toBe(25) // max(incoming.end, previous.end)
    }))
})

// =============================================================================
// Edge Cases
// =============================================================================

describe("edge cases", () => {
  it.effect("handles empty previous ranges (first batch)", ({ expect }) =>
    Effect.gen(function*() {
      const previous: Array<BlockRange> = []
      const incoming = [makeBlockRange("eth", 0, 10, 0)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(0)
    }))

  it.effect("handles new network in incoming (no reorg)", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 0, 10, 0)]
      const incoming = [
        makeBlockRange("eth", 11, 20, 0),
        makeBlockRange("polygon", 0, 10, 0) // New network
      ]

      const invalidations = detectReorgs(previous, incoming)

      // New networks don't cause reorg detection (handled by validateNetworks)
      expect(invalidations.length).toBe(0)
    }))

  it.effect("handles single block ranges", ({ expect }) =>
    Effect.gen(function*() {
      const previous = [makeBlockRange("eth", 10, 10, 0)]
      const incoming = [makeBlockRangeWithReorg("eth", 10, 12, 1, 1)]

      const invalidations = detectReorgs(previous, incoming)

      expect(invalidations.length).toBe(1)
      expect(invalidations[0]!.start).toBe(10)
      expect(invalidations[0]!.end).toBe(12)
    }))
})
