/**
 * Tests for CdcStream service.
 *
 * @module
 */
import { BatchStore, CdcStream, InMemoryBatchStore, layer as cdcStreamLayer } from "@edgeandnode/amp/cdc-stream"
import type { BlockRange } from "@edgeandnode/amp/core"
import { makeInvalidationRange, ProtocolStream, type ProtocolMessage } from "@edgeandnode/amp/protocol-stream"
import {
  type CommitHandle,
  dataEvent,
  InMemoryStateStore,
  layer as transactionalStreamLayer,
  rewindCause,
  TransactionalStream,
  type TransactionalStreamService,
  type TransactionEvent,
  type TransactionId,
  undoEvent,
  watermarkEvent
} from "@edgeandnode/amp/transactional-stream"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { identity } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"

// =============================================================================
// Test Helpers
// =============================================================================

const makeBlockRange = (network: string, start: number, end: number): BlockRange =>
  ({
    network,
    numbers: { start, end },
    hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    prevHash: undefined
  }) as BlockRange

/**
 * Create a mock CommitHandle that tracks whether commit was called.
 */
const makeMockCommitHandle = (id: TransactionId, commitRef: Ref.Ref<ReadonlyArray<TransactionId>>): CommitHandle => ({
  id,
  commit: Ref.update(commitRef, (ids) => [...ids, id])
})

/**
 * Create a mock TransactionalStream layer from a list of events.
 */
const mockTransactionalStreamLayer = (
  events: ReadonlyArray<readonly [TransactionEvent, CommitHandle]>
): Layer.Layer<TransactionalStream> =>
  Layer.succeed(TransactionalStream, {
    streamTransactional: () => Stream.fromIterable(events),
    forEach: () => Effect.void
  } satisfies TransactionalStreamService)

/**
 * Standard test layer: provides CdcStream only.
 */
const makeTestLayer = (events: ReadonlyArray<readonly [TransactionEvent, CommitHandle]>) =>
  cdcStreamLayer.pipe(Layer.provide(mockTransactionalStreamLayer(events)), Layer.provide(InMemoryBatchStore.layer))

/**
 * Test layer that exposes both CdcStream AND BatchStore (for tests needing store access).
 * Uses Layer.merge so both services are available in the test context.
 */
const makeTestLayerWithBatchStore = (events: ReadonlyArray<readonly [TransactionEvent, CommitHandle]>) => {
  const batchStoreLayer = InMemoryBatchStore.layer
  return Layer.merge(
    cdcStreamLayer.pipe(Layer.provide(mockTransactionalStreamLayer(events)), Layer.provide(batchStoreLayer)),
    batchStoreLayer
  )
}

// =============================================================================
// Insert Tests
// =============================================================================

describe("CdcStream - Insert", () => {
  it.effect("Data events become Insert events with same data", () =>
    Effect.gen(function* () {
      const data = [{ block: 1, address: "0xabc" }]
      const ranges = [makeBlockRange("eth", 0, 10)]

      const cdc = yield* CdcStream
      const results = yield* cdc.streamCdc("SELECT * FROM eth.logs").pipe(Stream.runCollect)

      expect(results.length).toBe(1)
      const [cdcEvent] = results[0]!
      expect(cdcEvent._tag).toBe("Insert")
      if (cdcEvent._tag === "Insert") {
        expect(cdcEvent.id).toBe(1)
        expect(cdcEvent.data).toEqual(data)
        expect(cdcEvent.ranges).toEqual(ranges)
      }
    }).pipe(
      Effect.provide(
        makeTestLayer([
          [
            dataEvent(1 as TransactionId, [{ block: 1, address: "0xabc" }], [makeBlockRange("eth", 0, 10)]),
            {
              id: 1 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )

  it.effect("Insert stores batch before emitting", () =>
    Effect.gen(function* () {
      const data = [{ block: 1 }]

      const cdc = yield* CdcStream
      const batchStore = yield* BatchStore

      // Consume the stream
      yield* cdc.streamCdc("SELECT 1").pipe(Stream.runDrain)

      // Verify batch was stored
      const stored = yield* batchStore.load(1 as TransactionId)
      expect(stored).toEqual(data)
    }).pipe(
      Effect.provide(
        makeTestLayerWithBatchStore([
          [
            dataEvent(1 as TransactionId, [{ block: 1 }], [makeBlockRange("eth", 0, 10)]),
            {
              id: 1 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )
})

// =============================================================================
// Delete Tests
// =============================================================================

describe("CdcStream - Delete", () => {
  it.effect("Undo events become Delete events with batch data", () =>
    Effect.gen(function* () {
      const data = [{ block: 1 }]

      const cdc = yield* CdcStream
      const results = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)

      // Should have Insert then Delete
      expect(results.length).toBe(2)

      const [insertEvent] = results[0]!
      expect(insertEvent._tag).toBe("Insert")

      const [deleteEvent] = results[1]!
      expect(deleteEvent._tag).toBe("Delete")
      if (deleteEvent._tag === "Delete") {
        const batch = yield* deleteEvent.batches.next
        expect(batch).toBeDefined()
        const [id, batchData] = batch!
        expect(id).toBe(1)
        expect(batchData).toEqual(data)

        // No more batches
        const next = yield* deleteEvent.batches.next
        expect(next).toBeUndefined()
      }
    }).pipe(
      Effect.provide(
        makeTestLayer([
          // First: Data event (stores batch)
          [
            dataEvent(1 as TransactionId, [{ block: 1 }], [makeBlockRange("eth", 0, 10)]),
            {
              id: 1 as TransactionId,
              commit: Effect.void
            }
          ],
          // Then: Undo event (should produce Delete with stored batch)
          [
            undoEvent(2 as TransactionId, rewindCause(), { start: 1 as TransactionId, end: 1 as TransactionId }),
            {
              id: 2 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )

  it.effect("Undo with no matching batches is skipped", () =>
    Effect.gen(function* () {
      const cdc = yield* CdcStream
      const results = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)

      // Undo for range with no stored batches should be filtered out
      expect(results.length).toBe(0)
    }).pipe(
      Effect.provide(
        makeTestLayer([
          [
            undoEvent(1 as TransactionId, rewindCause(), { start: 10 as TransactionId, end: 20 as TransactionId }),
            {
              id: 1 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )

  it.effect("Delete iterator loads lazily and skips missing", () =>
    Effect.gen(function* () {
      const cdc = yield* CdcStream
      const results = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)

      // Data events for IDs 1 and 3, then undo covering 1-3 (2 is missing)
      expect(results.length).toBe(3) // 2 Inserts + 1 Delete

      const [deleteEvent] = results[2]!
      expect(deleteEvent._tag).toBe("Delete")
      if (deleteEvent._tag === "Delete") {
        // First batch
        const first = yield* deleteEvent.batches.next
        expect(first).toBeDefined()
        expect(first![0]).toBe(1)

        // Second batch (ID 2 is missing, should skip to 3)
        const second = yield* deleteEvent.batches.next
        expect(second).toBeDefined()
        expect(second![0]).toBe(3)

        // No more
        const third = yield* deleteEvent.batches.next
        expect(third).toBeUndefined()
      }
    }).pipe(
      Effect.provide(
        makeTestLayer([
          [
            dataEvent(1 as TransactionId, [{ a: 1 }], [makeBlockRange("eth", 0, 5)]),
            {
              id: 1 as TransactionId,
              commit: Effect.void
            }
          ],
          [
            dataEvent(3 as TransactionId, [{ a: 3 }], [makeBlockRange("eth", 6, 10)]),
            {
              id: 3 as TransactionId,
              commit: Effect.void
            }
          ],
          [
            undoEvent(4 as TransactionId, rewindCause(), { start: 1 as TransactionId, end: 3 as TransactionId }),
            {
              id: 4 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )
})

// =============================================================================
// Watermark Tests
// =============================================================================

describe("CdcStream - Watermark", () => {
  it.effect("Watermark events are not exposed to consumer", () =>
    Effect.gen(function* () {
      const cdc = yield* CdcStream
      const results = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)

      // Watermark should be filtered out
      expect(results.length).toBe(0)
    }).pipe(
      Effect.provide(
        makeTestLayer([
          [
            watermarkEvent(1 as TransactionId, [makeBlockRange("eth", 0, 10)], null),
            {
              id: 1 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )

  it.effect("Watermark auto-commits", () =>
    Effect.gen(function* () {
      // Create a shared commitRef that both the mock and test can access
      const commitRef = yield* Ref.make<ReadonlyArray<TransactionId>>([])
      const commit = makeMockCommitHandle(1 as TransactionId, commitRef)

      const testLayer = cdcStreamLayer.pipe(
        Layer.provide(
          Layer.succeed(TransactionalStream, {
            streamTransactional: () =>
              Stream.make([watermarkEvent(1 as TransactionId, [makeBlockRange("eth", 0, 10)], null), commit] as const),
            forEach: () => Effect.void
          } satisfies TransactionalStreamService)
        ),
        Layer.provide(InMemoryBatchStore.layer)
      )

      yield* Effect.gen(function* () {
        const cdc = yield* CdcStream
        yield* cdc.streamCdc("SELECT 1").pipe(Stream.runDrain)
      }).pipe(Effect.provide(testLayer))

      const committed = yield* Ref.get(commitRef)
      expect(committed).toEqual([1])
    })
  )

  it.effect("Watermark triggers prune when prune point present", () =>
    Effect.gen(function* () {
      const batchStore = yield* BatchStore
      const cdc = yield* CdcStream

      // Pre-populate batch store
      yield* batchStore.append([{ a: 1 }], 1 as TransactionId)
      yield* batchStore.append([{ a: 2 }], 2 as TransactionId)
      yield* batchStore.append([{ a: 3 }], 3 as TransactionId)

      // Watermark with prune point at 2 should prune IDs 1 and 2
      yield* cdc.streamCdc("SELECT 1").pipe(Stream.runDrain)

      expect(yield* batchStore.load(1 as TransactionId)).toBeUndefined()
      expect(yield* batchStore.load(2 as TransactionId)).toBeUndefined()
      expect(yield* batchStore.load(3 as TransactionId)).toEqual([{ a: 3 }])
    }).pipe(
      Effect.provide(
        makeTestLayerWithBatchStore([
          [
            watermarkEvent(5 as TransactionId, [makeBlockRange("eth", 0, 10)], 2 as TransactionId),
            {
              id: 5 as TransactionId,
              commit: Effect.void
            }
          ]
        ])
      )
    )
  )
})

// =============================================================================
// forEach Tests
// =============================================================================

describe("CdcStream.forEach", () => {
  it.effect("auto-commits after handler succeeds", () =>
    Effect.gen(function* () {
      const commitRef = yield* Ref.make<ReadonlyArray<TransactionId>>([])
      const commit = makeMockCommitHandle(1 as TransactionId, commitRef)
      const handlerEvents = yield* Ref.make<ReadonlyArray<string>>([])

      const testLayer = cdcStreamLayer.pipe(
        Layer.provide(
          Layer.succeed(TransactionalStream, {
            streamTransactional: () =>
              Stream.make([
                dataEvent(1 as TransactionId, [{ block: 1 }], [makeBlockRange("eth", 0, 10)]),
                commit
              ] as const),
            forEach: () => Effect.void
          } satisfies TransactionalStreamService)
        ),
        Layer.provide(InMemoryBatchStore.layer)
      )

      yield* Effect.gen(function* () {
        const cdc = yield* CdcStream
        yield* cdc.forEach("SELECT 1", { retention: 128 }, (event) =>
          Ref.update(handlerEvents, (events) => [...events, event._tag])
        )
      }).pipe(Effect.provide(testLayer))

      const events = yield* Ref.get(handlerEvents)
      expect(events).toEqual(["Insert"])

      // Verify commit was called (forEach auto-commits)
      const committed = yield* Ref.get(commitRef)
      expect(committed).toEqual([1])
    })
  )
})

// =============================================================================
// Partial Reorg Tests
// =============================================================================

describe("CdcStream - partial reorg", () => {
  const batch = (start: number, end: number): Array<ProtocolMessage> => [
    {
      _tag: "Data",
      data: Array.from({ length: end - start + 1 }, (_, i) => ({ block: start + i })),
      ranges: [makeBlockRange("eth", start, end)]
    },
    { _tag: "Watermark", ranges: [makeBlockRange("eth", start, end)] }
  ]

  const reorg = (start: number, end: number): Array<ProtocolMessage> => [
    {
      _tag: "Reorg",
      previous: [],
      incoming: [makeBlockRange("eth", start, end)],
      invalidation: [makeInvalidationRange("eth", start, end)]
    }
  ]

  /**
   * Streams the first connection's messages through the real transactional and CDC layers, then resumes with the
   * state it committed, serving the remaining connections. Returns the blocks a consumer that applies each Insert
   * and Delete ends up with, how the resumed session ended, and how many connections it opened.
   */
  const run = (retention: number, connections: ReadonlyArray<ReadonlyArray<ProtocolMessage>>) => {
    let connection = 0
    const layer = cdcStreamLayer.pipe(
      Layer.provide(InMemoryBatchStore.layer),
      Layer.provide(transactionalStreamLayer),
      Layer.provide(InMemoryStateStore.layer),
      Layer.provide(
        Layer.succeed(ProtocolStream, {
          stream: () => Stream.fromIterable(connections[connection++] ?? [])
        } as ProtocolStream["Service"])
      )
    )

    return Effect.gen(function* () {
      const cdc = yield* CdcStream
      const rows = new Map<TransactionId, ReadonlyArray<Record<string, unknown>>>()
      const consume = cdc.forEach("SELECT * FROM eth.blocks", { retention }, (event) =>
        Effect.gen(function* () {
          if (event._tag === "Insert") {
            rows.set(event.id, event.data)
            return
          }
          let next = yield* event.batches.next
          while (next !== undefined) {
            rows.delete(next[0])
            next = yield* event.batches.next
          }
        })
      )

      yield* consume
      const error = yield* consume.pipe(Effect.match({ onFailure: identity, onSuccess: () => undefined }))

      return {
        blocks: [...rows.values()]
          .flat()
          .map((row) => row.block as number)
          .sort((a, b) => a - b),
        connections: connection,
        error
      }
    }).pipe(Effect.provide(layer))
  }

  const blocks = (start: number, end: number) => Array.from({ length: end - start + 1 }, (_, i) => start + i)

  it.effect("fails instead of replaying pruned batches when retention is zero", () =>
    Effect.gen(function* () {
      const result = yield* run(0, [[...batch(0, 34), ...batch(35, 37)], reorg(36, 38), batch(0, 38)])

      expect(result.error?._tag).toBe("PartialReorgError")
      expect(result.connections).toBe(2)
      expect(result.blocks).toEqual(blocks(0, 37))
    })
  )

  it.effect("fails instead of replaying pruned batches when the watermark before the reorg was pruned", () =>
    Effect.gen(function* () {
      const result = yield* run(128, [
        [...batch(0, 99), ...batch(100, 199), ...batch(200, 299), ...batch(300, 399)],
        reorg(150, 400),
        batch(0, 400)
      ])

      expect(result.error?._tag).toBe("PartialReorgError")
      expect(result.connections).toBe(2)
      expect(result.blocks).toEqual(blocks(0, 399))
    })
  )

  it.effect("recovers from a retained watermark", () =>
    Effect.gen(function* () {
      const result = yield* run(128, [[...batch(0, 34), ...batch(35, 37)], reorg(36, 38), batch(35, 38)])

      expect(result.error).toBeUndefined()
      expect(result.connections).toBe(3)
      expect(result.blocks).toEqual(blocks(0, 38))
    })
  )

  it.effect("recovers from a retained watermark after older ones were pruned", () =>
    Effect.gen(function* () {
      const result = yield* run(128, [
        [...batch(0, 99), ...batch(100, 199), ...batch(200, 299), ...batch(300, 399)],
        reorg(350, 400),
        batch(300, 400)
      ])

      expect(result.error).toBeUndefined()
      expect(result.connections).toBe(3)
      expect(result.blocks).toEqual(blocks(0, 400))
    })
  )
})
