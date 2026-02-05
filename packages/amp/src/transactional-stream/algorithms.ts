/**
 * Core algorithms for the TransactionalStream.
 *
 * These are pure functions that implement the buffer management logic
 * for recovery point calculation and retention-based pruning.
 *
 * Ported from Rust implementation in:
 * `.repos/amp/crates/clients/flight/src/transactional.rs`
 *
 * @module
 */
import type { BlockRange } from "../models.ts"
import type { InvalidationRange } from "../protocol-stream/messages.ts"
import type { TransactionId } from "./types.ts"

// =============================================================================
// Recovery Point Algorithm
// =============================================================================

/**
 * Find the recovery point watermark for a reorg.
 *
 * Walks backwards through watermarks to find the last unaffected watermark.
 * This watermark represents the safe recovery point - everything after it
 * needs to be invalidated.
 *
 * A watermark is affected if ANY of its networks have a block range that
 * starts at or after the reorg point for that network.
 *
 * @param buffer - Watermark buffer (oldest to newest)
 * @param invalidation - Invalidation ranges from the reorg
 * @returns Last unaffected watermark [id, ranges], or undefined if all affected
 *
 * @example
 * ```typescript
 * const buffer = [
 *   [1, [{ network: "eth", numbers: { start: 0, end: 10 }, ... }]],
 *   [3, [{ network: "eth", numbers: { start: 11, end: 20 }, ... }]],
 *   [5, [{ network: "eth", numbers: { start: 21, end: 30 }, ... }]]
 * ]
 * const invalidation = [{ network: "eth", start: 25, end: 35 }]
 *
 * // Walks backwards: id=5 is affected (21 >= 25? no, but 25 is within range)
 * // Actually: affected if range.start >= reorg_point
 * // id=5: start=21 >= 25? No -> not affected by this criteria
 * // But the Rust impl checks if range.start >= point, which means
 * // "this watermark's data starts after or at the reorg point"
 *
 * const recovery = findRecoveryPoint(buffer, invalidation)
 * // Returns [3, [...]] - last unaffected watermark
 * ```
 */
export const findRecoveryPoint = (
  buffer: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>,
  invalidation: ReadonlyArray<InvalidationRange>
): readonly [TransactionId, ReadonlyArray<BlockRange>] | undefined => {
  if (buffer.length === 0) {
    return undefined
  }

  // Build reorg points map: network -> first invalid block
  const points = new Map<string, number>()
  for (const inv of invalidation) {
    points.set(inv.network, inv.start)
  }

  // Walk backwards through watermarks (newest to oldest)
  for (let i = buffer.length - 1; i >= 0; i--) {
    const entry = buffer[i]!
    const [id, ranges] = entry

    // Check if ANY network in this watermark is affected
    // A watermark is affected if its range starts at or after the reorg point
    const affected = ranges.some((range) => {
      const point = points.get(range.network)
      // Only check networks that are in the invalidation list
      if (point === undefined) {
        return false
      }
      // Watermark is affected if range.start >= reorg_point
      return range.numbers.start >= point
    })

    if (!affected) {
      // Found last unaffected watermark
      return [id, ranges]
    }
  }

  // All watermarks are affected
  return undefined
}

// =============================================================================
// Pruning Point Algorithm
// =============================================================================

/**
 * Compute the last transaction ID that should be pruned based on retention window.
 *
 * Walks through watermarks from oldest to newest and identifies the last one
 * outside the retention window. A watermark is outside the retention window
 * if ALL its networks have ranges that end before the cutoff.
 *
 * The cutoff for each network is: `latest_start - retention`
 *
 * @param buffer - Watermark buffer (oldest to newest)
 * @param retention - Retention window in blocks
 * @returns Last transaction ID to prune (all IDs <= this are removed), or undefined if no pruning needed
 *
 * @example
 * ```typescript
 * const buffer = [
 *   [1, [{ network: "eth", numbers: { start: 0, end: 10 }, ... }]],
 *   [3, [{ network: "eth", numbers: { start: 11, end: 20 }, ... }]],
 *   [5, [{ network: "eth", numbers: { start: 100, end: 110 }, ... }]]
 * ]
 * const retention = 50
 *
 * // Latest start = 100, cutoff = 100 - 50 = 50
 * // id=1: end=10 < 50? Yes -> outside retention
 * // id=3: end=20 < 50? Yes -> outside retention
 * // id=5: skip (always keep latest)
 *
 * const prune = findPruningPoint(buffer, retention)
 * // Returns 3 - prune all IDs <= 3
 * ```
 */
export const findPruningPoint = (
  buffer: ReadonlyArray<readonly [TransactionId, ReadonlyArray<BlockRange>]>,
  retention: number
): TransactionId | undefined => {
  if (buffer.length === 0) {
    return undefined
  }

  // Get latest ranges from buffer (last entry)
  const latestEntry = buffer[buffer.length - 1]!
  const [, latestRanges] = latestEntry

  if (latestRanges.length === 0) {
    return undefined
  }

  // Calculate cutoff block for each network
  // Cutoff = latest_start - retention (minimum 0)
  const cutoffs = new Map<string, number>()
  for (const range of latestRanges) {
    const cutoff = Math.max(0, range.numbers.start - retention)
    cutoffs.set(range.network, cutoff)
  }

  let last: TransactionId | undefined = undefined

  // Walk from front (oldest to newest), skipping the last watermark
  // We never prune the most recent watermark
  for (let i = 0; i < buffer.length - 1; i++) {
    const entry = buffer[i]!
    const [id, ranges] = entry

    // Check if this watermark is ENTIRELY outside retention window
    // ALL networks must have ranges that end before their cutoff
    const outside = ranges.every((range) => {
      const cutoff = cutoffs.get(range.network)
      // If network isn't in latest, consider it outside retention
      if (cutoff === undefined) {
        return true
      }
      return range.numbers.end < cutoff
    })

    if (outside) {
      last = id
    } else {
      // First watermark within retention - stop searching
      break
    }
  }

  return last
}

// =============================================================================
// Partial Reorg Detection
// =============================================================================

/**
 * Check if a reorg is partial (unrecoverable).
 *
 * A partial reorg occurs when the recovery point watermark has a range
 * that partially overlaps with the invalidation - meaning the reorg point
 * falls within a watermark's range, not at its boundary.
 *
 * If we were to ignore this, we would end up with a data gap because we're
 * telling the consumer to invalidate data that won't be replayed.
 *
 * @param recoveryRanges - Block ranges from the recovery point watermark
 * @param invalidation - Invalidation ranges from the reorg
 * @returns The network with partial reorg, or undefined if no partial reorg
 */
export const checkPartialReorg = (
  recoveryRanges: ReadonlyArray<BlockRange>,
  invalidation: ReadonlyArray<InvalidationRange>
): string | undefined => {
  for (const range of recoveryRanges) {
    const inv = invalidation.find((i) => i.network === range.network)
    if (inv !== undefined) {
      const point = inv.start
      // Check: recovery.start < reorg_point <= recovery.end
      // This means the reorg point falls within the recovery watermark's range
      if (range.numbers.start < point && point <= range.numbers.end) {
        return range.network
      }
    }
  }
  return undefined
}

// =============================================================================
// Commit Compression
// =============================================================================

/**
 * Compress multiple pending commits into a single atomic update.
 *
 * Takes the maximum pruning point from all pending commits and
 * removes any watermarks that would be pruned from the insert list.
 *
 * @param pendingCommits - Array of [transactionId, { ranges, prune }] tuples
 * @returns Compressed commit with combined inserts and maximum prune point
 */
export const compressCommits = (
  pendingCommits: ReadonlyArray<
    readonly [TransactionId, { readonly ranges: ReadonlyArray<BlockRange>; readonly prune: TransactionId | undefined }]
  >
): { insert: Array<readonly [TransactionId, ReadonlyArray<BlockRange>]>; prune: TransactionId | undefined } => {
  const insert: Array<readonly [TransactionId, ReadonlyArray<BlockRange>]> = []
  let maxPrune: TransactionId | undefined = undefined

  // Collect watermarks and find maximum prune point
  for (const [id, commit] of pendingCommits) {
    insert.push([id, commit.ranges])

    // Take the maximum prune point
    if (commit.prune !== undefined) {
      if (maxPrune === undefined || commit.prune > maxPrune) {
        maxPrune = commit.prune
      }
    }
  }

  // Remove any watermarks that were pruned
  const filteredInsert =
    maxPrune !== undefined
      ? insert.filter(([id]) => id > maxPrune!)
      : insert

  return {
    insert: filteredInsert,
    prune: maxPrune
  }
}
