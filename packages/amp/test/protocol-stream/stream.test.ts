/**
 * ProtocolStream end-to-end tests.
 *
 * `reorg.test.ts` checks a copy of the reorg detection logic. These tests run
 * the real `ProtocolStream` service over a mock `ArrowFlight` that replays the
 * batches Amp sends, to check the messages a client actually receives.
 */
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import type { QueryOptions, QueryResult } from "@edgeandnode/amp/arrow-flight"
import type { BlockHash, BlockNumber, BlockRange, Network } from "@edgeandnode/amp/core"
import { layer as protocolStreamLayer, type ProtocolMessage, ProtocolStream } from "@edgeandnode/amp/protocol-stream"
import { describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

// =============================================================================
// Helpers
// =============================================================================

const NETWORK = "anvil"

/**
 * A deterministic block hash. Blocks of different epochs, like the blocks of
 * two forks, get different hashes. The `ab` marker keeps block 0's hash from
 * being the zero hash, which the protocol reserves for "no parent".
 */
const makeHash = (block: number, epoch: number): BlockHash =>
  `0x${epoch.toString(16).padStart(2, "0")}ab${"0".repeat(44)}${block.toString(16).padStart(16, "0")}` as BlockHash

/**
 * A block range from `start` to `end` on the fork `epoch`, whose parent is on
 * the fork `parentEpoch` (the same fork unless the range starts a new one).
 */
const range = (start: number, end: number, epoch: number, parentEpoch: number = epoch): BlockRange => ({
  network: NETWORK as Network,
  numbers: { start: start as BlockNumber, end: end as BlockNumber },
  hash: makeHash(end, epoch),
  prevHash: start > 0 ? makeHash(start - 1, parentEpoch) : undefined
})

const rows = (start: number, end: number): Array<Record<string, unknown>> =>
  Array.from({ length: end - start + 1 }, (_, i) => ({ block_num: start + i }))

/** A batch of rows for a range, as Amp sends it before the range is complete. */
const dataBatch = (r: BlockRange): QueryResult<Record<string, unknown>> => ({
  data: rows(r.numbers.start, r.numbers.end),
  metadata: { ranges: [r], rangesComplete: false }
})

/** The empty batch that marks a range complete. */
const completeBatch = (r: BlockRange): QueryResult<Record<string, unknown>> => ({
  data: [],
  metadata: { ranges: [r], rangesComplete: true }
})

/**
 * A mock ArrowFlight whose `streamQuery` replays `batches`, and records the
 * options it was called with.
 */
const mockArrowFlightLayer = (
  batches: ReadonlyArray<QueryResult<Record<string, unknown>>>,
  calls: Array<QueryOptions | undefined>
): Layer.Layer<ArrowFlight.ArrowFlight> =>
  Layer.succeed(ArrowFlight.ArrowFlight, {
    client: undefined as never,
    query: () => Effect.die("query is not used by ProtocolStream"),
    explain: () => Effect.die("explain is not used by ProtocolStream"),
    streamQuery: ((_sql: string, options?: QueryOptions) => {
      calls.push(options)
      return Stream.fromIterable(batches)
    }) as never
  })

/** Runs a protocol stream over `batches`, and collects its messages. */
const collect = (
  batches: ReadonlyArray<QueryResult<Record<string, unknown>>>,
  resumeWatermark?: ReadonlyArray<BlockRange>
) => {
  const calls: Array<QueryOptions | undefined> = []
  return Effect.gen(function* () {
    const protocolStream = yield* ProtocolStream
    const messages = yield* Stream.runCollect(
      protocolStream.stream("SELECT block_num FROM blocks", resumeWatermark ? { resumeWatermark } : undefined)
    )
    return { messages: Array.from(messages) as Array<ProtocolMessage>, calls }
  }).pipe(Effect.provide(protocolStreamLayer.pipe(Layer.provide(mockArrowFlightLayer(batches, calls)))))
}

const summarize = (messages: ReadonlyArray<ProtocolMessage>) =>
  messages.map((message) => {
    switch (message._tag) {
      case "Data":
        return `Data ${message.data.map((row) => row.block_num).join(",")}`
      case "Watermark":
        return `Watermark ${message.ranges.map((r) => `${r.numbers.start}-${r.numbers.end}`).join(",")}`
      case "Reorg":
        return `Reorg ${message.invalidation.map((r) => `${r.start}-${r.end}`).join(",")}`
    }
  })

// =============================================================================
// Reorgs within a stream
// =============================================================================

describe("ProtocolStream reorgs", () => {
  it.effect("emits the replacement rows that arrive with a reorg", ({ expect }) =>
    Effect.gen(function* () {
      // Blocks 0-2, then a reorg replaces 1-2 and adds 3: the new range starts
      // at block 1, and its parent is block 0, the last block both forks share.
      const original = range(0, 2, 0)
      const replacement = range(1, 3, 1, 0)

      const { messages } = yield* collect([
        dataBatch(original),
        completeBatch(original),
        dataBatch(replacement),
        completeBatch(replacement)
      ])

      expect(summarize(messages)).toEqual(["Data 0,1,2", "Watermark 0-2", "Reorg 1-3", "Data 1,2,3", "Watermark 1-3"])
    })
  )

  it.effect("emits the watermark of a reorg range that has no rows", ({ expect }) =>
    Effect.gen(function* () {
      const original = range(0, 2, 0)
      const replacement = range(1, 3, 1, 0)

      const { messages } = yield* collect([dataBatch(original), completeBatch(original), completeBatch(replacement)])

      expect(summarize(messages)).toEqual(["Data 0,1,2", "Watermark 0-2", "Reorg 1-3", "Watermark 1-3"])
    })
  )

  it.effect("emits a plain data message when the chain continues", ({ expect }) =>
    Effect.gen(function* () {
      const first = range(0, 2, 0)
      const next = range(3, 4, 0)

      const { messages } = yield* collect([
        dataBatch(first),
        completeBatch(first),
        dataBatch(next),
        completeBatch(next)
      ])

      expect(summarize(messages)).toEqual(["Data 0,1,2", "Watermark 0-2", "Data 3,4", "Watermark 3-4"])
    })
  )
})

// =============================================================================
// Reorgs while the stream was stopped
// =============================================================================

describe("ProtocolStream resume", () => {
  it.effect("detects a reorg that happened while the stream was stopped", ({ expect }) =>
    Effect.gen(function* () {
      // The client saved its cursor at block 2. Meanwhile, a reorg replaced
      // blocks 1-2, so Amp re-sends from block 1 on the new fork.
      const cursor = range(0, 2, 0)
      const replacement = range(1, 3, 1, 0)

      const { calls, messages } = yield* collect([dataBatch(replacement), completeBatch(replacement)], [cursor])

      expect(calls[0]?.resumeWatermark).toEqual([cursor])
      expect(summarize(messages)).toEqual(["Reorg 1-3", "Data 1,2,3", "Watermark 1-3"])
    })
  )

  it.effect("resumes without a reorg when the chain continued", ({ expect }) =>
    Effect.gen(function* () {
      const cursor = range(0, 2, 0)
      const next = range(3, 4, 0)

      const { messages } = yield* collect([dataBatch(next), completeBatch(next)], [cursor])

      expect(summarize(messages)).toEqual(["Data 3,4", "Watermark 3-4"])
    })
  )
})
