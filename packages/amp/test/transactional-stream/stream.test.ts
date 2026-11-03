/**
 * Tests for TransactionalStream - reconnecting after a partial reorg.
 *
 * @module
 */
import type { BlockNumber, BlockRange, Network } from "@edgeandnode/amp/core"
import { makeInvalidationRange, ProtocolStream, type ProtocolMessage } from "@edgeandnode/amp/protocol-stream"
import {
  InMemoryStateStore,
  layer as transactionalStreamLayer,
  type StateSnapshot,
  TransactionalStream,
  type TransactionId
} from "@edgeandnode/amp/transactional-stream"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

// =============================================================================
// Test Helpers
// =============================================================================

const makeBlockRange = (start: number, end: number): BlockRange =>
  ({
    network: "eth" as Network,
    numbers: { start: start as BlockNumber, end: end as BlockNumber },
    hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    prevHash: undefined
  }) as BlockRange

const data = (start: number, end: number): ProtocolMessage => ({
  _tag: "Data",
  data: [{ start, end }],
  ranges: [makeBlockRange(start, end)]
})

const watermark = (start: number, end: number): ProtocolMessage => ({
  _tag: "Watermark",
  ranges: [makeBlockRange(start, end)]
})

const reorg = (start: number, end: number): ProtocolMessage => ({
  _tag: "Reorg",
  previous: [],
  incoming: [makeBlockRange(start, end)],
  invalidation: [makeInvalidationRange("eth", start, end)]
})

/**
 * Runs a transactional stream over a mock ProtocolStream that serves one list of messages per connection,
 * and reports what each connection resumed from and when it closed.
 */
const run = (snapshot: StateSnapshot, connections: ReadonlyArray<ReadonlyArray<ProtocolMessage>>) => {
  const log: Array<string> = []
  let connection = 0

  const layer = transactionalStreamLayer.pipe(
    Layer.provide(InMemoryStateStore.layerWithState(snapshot)),
    Layer.provide(
      Layer.succeed(ProtocolStream, {
        stream: (_sql, options) => {
          const id = connection++
          const resume = options?.resumeWatermark?.[0]?.numbers
          log.push(`open ${id} from ${resume === undefined ? "start" : `${resume.start}-${resume.end}`}`)
          return Stream.fromIterable(connections[id] ?? []).pipe(
            Stream.ensuring(Effect.sync(() => log.push(`close ${id}`)))
          )
        }
      } as ProtocolStream["Service"])
    )
  )

  return Effect.gen(function* () {
    const transactionalStream = yield* TransactionalStream
    const events = yield* transactionalStream.streamTransactional("SELECT * FROM eth.logs").pipe(Stream.runCollect)
    return { events: events.map(([event]) => event), log }
  }).pipe(Effect.provide(layer))
}

// =============================================================================
// Tests
// =============================================================================

describe("TransactionalStream.streamTransactional", () => {
  it.effect("reconnects from an earlier watermark when a reorg starts inside the last one", () =>
    Effect.gen(function* () {
      const { events, log } = yield* run(
        {
          buffer: [
            [1 as TransactionId, [makeBlockRange(0, 34)]],
            [3 as TransactionId, [makeBlockRange(35, 37)]]
          ],
          next: 4 as TransactionId
        },
        [
          // The batch that reveals the reorg only covers blocks from the reorg point on
          [reorg(36, 38), data(36, 38), watermark(36, 38)],
          [data(35, 38), watermark(35, 38)]
        ]
      )

      expect(log).toEqual(["open 0 from 35-37", "close 0", "open 1 from 0-34", "close 1"])
      expect(events.map((event) => event._tag)).toEqual(["Undo", "Data", "Watermark"])
      const undo = events[0]!
      expect(undo._tag === "Undo" && undo.invalidate).toEqual({ start: 2, end: 4 })
      const replacement = events[1]!
      expect(replacement._tag === "Data" && replacement.ranges).toEqual([makeBlockRange(35, 38)])
    })
  )

  it.effect("starts over when a reorg starts inside the only watermark", () =>
    Effect.gen(function* () {
      const { events, log } = yield* run(
        {
          buffer: [[1 as TransactionId, [makeBlockRange(0, 37)]]],
          next: 2 as TransactionId
        },
        [[reorg(36, 38), data(36, 38)], [data(0, 38)]]
      )

      expect(log).toEqual(["open 0 from 0-37", "close 0", "open 1 from start", "close 1"])
      expect(events.map((event) => event._tag)).toEqual(["Undo", "Data"])
      const undo = events[0]!
      expect(undo._tag === "Undo" && undo.invalidate).toEqual({ start: 0, end: 2 })
    })
  )

  it.effect("stays connected when a reorg starts at a watermark's first block", () =>
    Effect.gen(function* () {
      const { events, log } = yield* run(
        {
          buffer: [
            [1 as TransactionId, [makeBlockRange(0, 34)]],
            [3 as TransactionId, [makeBlockRange(35, 37)]]
          ],
          next: 4 as TransactionId
        },
        [[reorg(35, 38), data(35, 38), watermark(35, 38)]]
      )

      expect(log).toEqual(["open 0 from 35-37", "close 0"])
      expect(events.map((event) => event._tag)).toEqual(["Undo", "Data", "Watermark"])
    })
  )
})
