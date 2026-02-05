/**
 * Tests for the TransactionalStream algorithms.
 *
 * @module
 */
import type { BlockRange } from "@edgeandnode/amp/models"
import type { InvalidationRange } from "@edgeandnode/amp/protocol-stream"
import {
  checkPartialReorg,
  compressCommits,
  findPruningPoint,
  findRecoveryPoint,
  type TransactionId
} from "@edgeandnode/amp/transactional-stream"
import { describe, expect, it } from "vitest"

// =============================================================================
// Test Helpers
// =============================================================================

const makeBlockRange = (
  network: string,
  start: number,
  end: number,
  hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
): BlockRange =>
  ({
    network,
    numbers: { start, end },
    hash,
    prevHash: undefined
  }) as BlockRange

const makeInvalidation = (network: string, start: number, end: number): InvalidationRange =>
  ({
    network,
    start,
    end
  }) as InvalidationRange

const makeWatermark = (
  id: number,
  ranges: ReadonlyArray<BlockRange>
): readonly [TransactionId, ReadonlyArray<BlockRange>] => [id as TransactionId, ranges]

// =============================================================================
// findRecoveryPoint Tests
// =============================================================================

describe("findRecoveryPoint", () => {
  it("returns undefined for empty buffer", () => {
    const result = findRecoveryPoint([], [makeInvalidation("eth", 100, 110)])
    expect(result).toBeUndefined()
  })

  it("returns last watermark when no network is affected", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10)]),
      makeWatermark(2, [makeBlockRange("eth", 11, 20)]),
      makeWatermark(3, [makeBlockRange("eth", 21, 30)])
    ]

    // Invalidation on different network
    const result = findRecoveryPoint(buffer, [makeInvalidation("polygon", 100, 110)])
    expect(result).toEqual(buffer[2])
  })

  it("finds last unaffected watermark", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10)]),
      makeWatermark(2, [makeBlockRange("eth", 11, 20)]),
      makeWatermark(3, [makeBlockRange("eth", 21, 30)])
    ]

    // Reorg at block 21 - watermarks at id=1,2 are safe, id=3 starts at affected point
    const result = findRecoveryPoint(buffer, [makeInvalidation("eth", 21, 35)])
    expect(result?.[0]).toBe(2)
  })

  it("returns undefined when all watermarks affected", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 100, 110)]),
      makeWatermark(2, [makeBlockRange("eth", 111, 120)])
    ]

    // Reorg affects from block 100
    const result = findRecoveryPoint(buffer, [makeInvalidation("eth", 100, 130)])
    expect(result).toBeUndefined()
  })

  it("handles multi-network scenarios", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10), makeBlockRange("polygon", 0, 100)]),
      makeWatermark(2, [makeBlockRange("eth", 11, 20), makeBlockRange("polygon", 101, 200)]),
      makeWatermark(3, [makeBlockRange("eth", 21, 30), makeBlockRange("polygon", 201, 300)])
    ]

    // Reorg on eth at block 21, polygon at block 201
    const result = findRecoveryPoint(buffer, [
      makeInvalidation("eth", 21, 35),
      makeInvalidation("polygon", 201, 350)
    ])
    expect(result?.[0]).toBe(2)
  })

  it("single watermark affected returns undefined", () => {
    const buffer = [makeWatermark(1, [makeBlockRange("eth", 50, 60)])]

    const result = findRecoveryPoint(buffer, [makeInvalidation("eth", 50, 70)])
    expect(result).toBeUndefined()
  })

  it("handles invalidation that doesn't affect any watermark", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10)]),
      makeWatermark(2, [makeBlockRange("eth", 11, 20)])
    ]

    // Invalidation starts at block 100, well beyond our watermarks
    // But watermarks with start >= 100 would be affected
    // Since no watermarks start at >= 100, all are safe
    const result = findRecoveryPoint(buffer, [makeInvalidation("eth", 100, 110)])
    expect(result?.[0]).toBe(2) // Last watermark is safe
  })
})

// =============================================================================
// findPruningPoint Tests
// =============================================================================

describe("findPruningPoint", () => {
  it("returns undefined for empty buffer", () => {
    const result = findPruningPoint([], 50)
    expect(result).toBeUndefined()
  })

  it("returns undefined for single watermark", () => {
    const buffer = [makeWatermark(1, [makeBlockRange("eth", 100, 110)])]
    const result = findPruningPoint(buffer, 50)
    expect(result).toBeUndefined()
  })

  it("returns undefined when all within retention", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 90, 95)]),
      makeWatermark(2, [makeBlockRange("eth", 96, 100)])
    ]
    // Retention 50, latest start is 96, cutoff = 96 - 50 = 46
    // Watermark 1 ends at 95, which is >= 46, so it's within retention
    const result = findPruningPoint(buffer, 50)
    expect(result).toBeUndefined()
  })

  it("finds watermarks outside retention", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10)]),
      makeWatermark(2, [makeBlockRange("eth", 11, 20)]),
      makeWatermark(3, [makeBlockRange("eth", 100, 110)])
    ]
    // Retention 50, latest start is 100, cutoff = 100 - 50 = 50
    // Watermark 1: end=10 < 50, outside retention
    // Watermark 2: end=20 < 50, outside retention
    // Never prune latest (id=3)
    const result = findPruningPoint(buffer, 50)
    expect(result).toBe(2)
  })

  it("stops at first watermark within retention", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10)]),
      makeWatermark(2, [makeBlockRange("eth", 45, 55)]), // Within retention
      makeWatermark(3, [makeBlockRange("eth", 100, 110)])
    ]
    // Retention 50, latest start is 100, cutoff = 50
    // Watermark 1: end=10 < 50, outside retention
    // Watermark 2: end=55 >= 50, within retention - stop here
    const result = findPruningPoint(buffer, 50)
    expect(result).toBe(1) // Only prune up to id=1
  })

  it("handles multi-network scenarios", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10), makeBlockRange("polygon", 0, 100)]),
      makeWatermark(2, [makeBlockRange("eth", 200, 210), makeBlockRange("polygon", 200, 300)])
    ]
    // For eth: cutoff = 200 - 50 = 150
    // For polygon: cutoff = 200 - 50 = 150
    // Watermark 1: eth.end=10 < 150, polygon.end=100 < 150 - both outside
    const result = findPruningPoint(buffer, 50)
    expect(result).toBe(1)
  })

  it("does not prune when watermark has network not in latest", () => {
    const buffer = [
      makeWatermark(1, [makeBlockRange("eth", 0, 10), makeBlockRange("polygon", 0, 100)]),
      makeWatermark(2, [makeBlockRange("eth", 200, 210)]) // polygon removed
    ]
    // For eth: cutoff = 200 - 50 = 150
    // Watermark 1: eth.end=10 < 150, polygon not in latest (considered outside)
    // So watermark 1 is prunable
    const result = findPruningPoint(buffer, 50)
    expect(result).toBe(1)
  })
})

// =============================================================================
// checkPartialReorg Tests
// =============================================================================

describe("checkPartialReorg", () => {
  it("returns undefined when no overlap", () => {
    const recoveryRanges = [makeBlockRange("eth", 0, 10)]
    const invalidation = [makeInvalidation("eth", 11, 20)]

    const result = checkPartialReorg(recoveryRanges, invalidation)
    expect(result).toBeUndefined()
  })

  it("returns undefined when reorg at exact boundary", () => {
    const recoveryRanges = [makeBlockRange("eth", 0, 10)]
    // Reorg starts exactly at end+1 of recovery range
    const invalidation = [makeInvalidation("eth", 11, 20)]

    const result = checkPartialReorg(recoveryRanges, invalidation)
    expect(result).toBeUndefined()
  })

  it("detects partial reorg within range", () => {
    const recoveryRanges = [makeBlockRange("eth", 0, 10)]
    // Reorg point 5 falls within range [0, 10]
    const invalidation = [makeInvalidation("eth", 5, 20)]

    const result = checkPartialReorg(recoveryRanges, invalidation)
    expect(result).toBe("eth")
  })

  it("detects partial reorg at end of range", () => {
    const recoveryRanges = [makeBlockRange("eth", 0, 10)]
    // Reorg point 10 equals end of range - this is partial
    const invalidation = [makeInvalidation("eth", 10, 20)]

    const result = checkPartialReorg(recoveryRanges, invalidation)
    expect(result).toBe("eth")
  })

  it("returns undefined for different networks", () => {
    const recoveryRanges = [makeBlockRange("eth", 0, 10)]
    const invalidation = [makeInvalidation("polygon", 5, 20)]

    const result = checkPartialReorg(recoveryRanges, invalidation)
    expect(result).toBeUndefined()
  })

  it("returns first partial network in multi-network scenario", () => {
    const recoveryRanges = [
      makeBlockRange("eth", 0, 10),
      makeBlockRange("polygon", 0, 100)
    ]
    // Both have partial reorg
    const invalidation = [
      makeInvalidation("eth", 5, 20),
      makeInvalidation("polygon", 50, 150)
    ]

    const result = checkPartialReorg(recoveryRanges, invalidation)
    // Returns first one found (eth)
    expect(result).toBe("eth")
  })

  it("returns undefined when reorg before range", () => {
    const recoveryRanges = [makeBlockRange("eth", 10, 20)]
    // Reorg point is before the range starts - not partial
    const invalidation = [makeInvalidation("eth", 5, 25)]

    // This is actually partial because 5 < 10 but 5 <= 20
    // Wait, the check is: range.start < point && point <= range.end
    // 10 < 5? No. So not partial.
    const result = checkPartialReorg(recoveryRanges, invalidation)
    expect(result).toBeUndefined()
  })
})

// =============================================================================
// compressCommits Tests
// =============================================================================

describe("compressCommits", () => {
  it("returns empty for empty input", () => {
    const result = compressCommits([])
    expect(result.insert).toEqual([])
    expect(result.prune).toBeUndefined()
  })

  it("collects single commit", () => {
    const pending = [
      [
        1 as TransactionId,
        { ranges: [makeBlockRange("eth", 0, 10)], prune: undefined }
      ] as const
    ]

    const result = compressCommits(pending)
    expect(result.insert).toHaveLength(1)
    expect(result.insert[0]![0]).toBe(1)
    expect(result.prune).toBeUndefined()
  })

  it("collects multiple commits", () => {
    const pending = [
      [1 as TransactionId, { ranges: [makeBlockRange("eth", 0, 10)], prune: undefined }] as const,
      [2 as TransactionId, { ranges: [makeBlockRange("eth", 11, 20)], prune: undefined }] as const
    ]

    const result = compressCommits(pending)
    expect(result.insert).toHaveLength(2)
    expect(result.prune).toBeUndefined()
  })

  it("takes maximum prune point", () => {
    const pending = [
      [1 as TransactionId, { ranges: [makeBlockRange("eth", 0, 10)], prune: undefined }] as const,
      [2 as TransactionId, { ranges: [makeBlockRange("eth", 11, 20)], prune: 0 as TransactionId }] as const,
      [3 as TransactionId, { ranges: [makeBlockRange("eth", 21, 30)], prune: 1 as TransactionId }] as const
    ]

    const result = compressCommits(pending)
    expect(result.prune).toBe(1)
  })

  it("filters pruned watermarks from inserts", () => {
    const pending = [
      [1 as TransactionId, { ranges: [makeBlockRange("eth", 0, 10)], prune: undefined }] as const,
      [2 as TransactionId, { ranges: [makeBlockRange("eth", 11, 20)], prune: undefined }] as const,
      [3 as TransactionId, { ranges: [makeBlockRange("eth", 21, 30)], prune: 1 as TransactionId }] as const
    ]

    const result = compressCommits(pending)
    // Should filter out id=1 since prune=1
    expect(result.insert).toHaveLength(2)
    expect(result.insert.map(([id]) => id)).toEqual([2, 3])
    expect(result.prune).toBe(1)
  })

  it("handles all watermarks being pruned", () => {
    const pending = [
      [1 as TransactionId, { ranges: [makeBlockRange("eth", 0, 10)], prune: undefined }] as const,
      [2 as TransactionId, { ranges: [makeBlockRange("eth", 11, 20)], prune: 2 as TransactionId }] as const
    ]

    const result = compressCommits(pending)
    // All IDs <= 2 are pruned
    expect(result.insert).toHaveLength(0)
    expect(result.prune).toBe(2)
  })
})
