/**
 * Tests for CdcStream service.
 *
 * @module
 */
import { BatchStore, CdcStream, InMemoryBatchStore, layer as cdcStreamLayer } from "@edgeandnode/amp/cdc-stream"
import type { BlockRange } from "@edgeandnode/amp/core"
import {
  type CommitHandle,
  dataEvent,
  rewindCause,
  TransactionalStream,
  type TransactionalStreamService,
  type TransactionEvent,
  type TransactionId,
  undoEvent,
  watermarkEvent
} from "@edgeandnode/amp/transactional-stream"
import { describe, expect, it } from "@effect/vitest"
import * as Chunk from "effect/Chunk"
import * as Effect from "effect/Effect"
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
const makeMockCommitHandle = (
  id: TransactionId,
  commitRef: Ref.Ref<ReadonlyArray<TransactionId>>
): CommitHandle => ({
  id,
  commit: Ref.update(commitRef, (ids) => [...ids, id])
})

/**
 * Create a mock TransactionalStream layer from a list of events.
 */
const mockTransactionalStreamLayer = (
  events: ReadonlyArray<readonly [TransactionEvent, CommitHandle]>
): Layer.Layer<TransactionalStream> =>
  Layer.succeed(
    TransactionalStream,
    {
      streamTransactional: () => Stream.fromIterable(events),
      forEach: () => Effect.void
    } satisfies TransactionalStreamService
  )

/**
 * Standard test layer: provides CdcStream only.
 */
const makeTestLayer = (
  events: ReadonlyArray<readonly [TransactionEvent, CommitHandle]>
) =>
  cdcStreamLayer.pipe(
    Layer.provide(mockTransactionalStreamLayer(events)),
    Layer.provide(InMemoryBatchStore.layer)
  )

/**
 * Test layer that exposes both CdcStream AND BatchStore (for tests needing store access).
 * Uses Layer.merge so both services are available in the test context.
 */
const makeTestLayerWithBatchStore = (
  events: ReadonlyArray<readonly [TransactionEvent, CommitHandle]>
) => {
  const batchStoreLayer = InMemoryBatchStore.layer
  return Layer.merge(
    cdcStreamLayer.pipe(
      Layer.provide(mockTransactionalStreamLayer(events)),
      Layer.provide(batchStoreLayer)
    ),
    batchStoreLayer
  )
}

// =============================================================================
// Insert Tests
// =============================================================================

describe("CdcStream - Insert", () => {
  it.effect("Data events become Insert events with same data", () =>
    Effect.gen(function*() {
      const data = [{ block: 1, address: "0xabc" }]
      const ranges = [makeBlockRange("eth", 0, 10)]

      const cdc = yield* CdcStream
      const chunk = yield* cdc.streamCdc("SELECT * FROM eth.logs").pipe(Stream.runCollect)
      const results = Chunk.toReadonlyArray(chunk)

      expect(results.length).toBe(1)
      const [cdcEvent] = results[0]!
      expect(cdcEvent._tag).toBe("Insert")
      if (cdcEvent._tag === "Insert") {
        expect(cdcEvent.id).toBe(1)
        expect(cdcEvent.data).toEqual(data)
        expect(cdcEvent.ranges).toEqual(ranges)
      }
    }).pipe(Effect.provide(
      makeTestLayer([
        [dataEvent(1 as TransactionId, [{ block: 1, address: "0xabc" }], [makeBlockRange("eth", 0, 10)]), {
          id: 1 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))

  it.effect("Insert stores batch before emitting", () =>
    Effect.gen(function*() {
      const data = [{ block: 1 }]

      const cdc = yield* CdcStream
      const batchStore = yield* BatchStore

      // Consume the stream
      yield* cdc.streamCdc("SELECT 1").pipe(Stream.runDrain)

      // Verify batch was stored
      const stored = yield* batchStore.load(1 as TransactionId)
      expect(stored).toEqual(data)
    }).pipe(Effect.provide(
      makeTestLayerWithBatchStore([
        [dataEvent(1 as TransactionId, [{ block: 1 }], [makeBlockRange("eth", 0, 10)]), {
          id: 1 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))
})

// =============================================================================
// Delete Tests
// =============================================================================

describe("CdcStream - Delete", () => {
  it.effect("Undo events become Delete events with batch data", () =>
    Effect.gen(function*() {
      const data = [{ block: 1 }]

      const cdc = yield* CdcStream
      const chunk = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)
      const results = Chunk.toReadonlyArray(chunk)

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
    }).pipe(Effect.provide(
      makeTestLayer([
        // First: Data event (stores batch)
        [dataEvent(1 as TransactionId, [{ block: 1 }], [makeBlockRange("eth", 0, 10)]), {
          id: 1 as TransactionId,
          commit: Effect.void
        }],
        // Then: Undo event (should produce Delete with stored batch)
        [undoEvent(2 as TransactionId, rewindCause(), { start: 1 as TransactionId, end: 1 as TransactionId }), {
          id: 2 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))

  it.effect("Undo with no matching batches is skipped", () =>
    Effect.gen(function*() {
      const cdc = yield* CdcStream
      const chunk = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)
      const results = Chunk.toReadonlyArray(chunk)

      // Undo for range with no stored batches should be filtered out
      expect(results.length).toBe(0)
    }).pipe(Effect.provide(
      makeTestLayer([
        [undoEvent(1 as TransactionId, rewindCause(), { start: 10 as TransactionId, end: 20 as TransactionId }), {
          id: 1 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))

  it.effect("Delete iterator loads lazily and skips missing", () =>
    Effect.gen(function*() {
      const cdc = yield* CdcStream
      const chunk = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)
      const results = Chunk.toReadonlyArray(chunk)

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
    }).pipe(Effect.provide(
      makeTestLayer([
        [dataEvent(1 as TransactionId, [{ a: 1 }], [makeBlockRange("eth", 0, 5)]), {
          id: 1 as TransactionId,
          commit: Effect.void
        }],
        [dataEvent(3 as TransactionId, [{ a: 3 }], [makeBlockRange("eth", 6, 10)]), {
          id: 3 as TransactionId,
          commit: Effect.void
        }],
        [undoEvent(4 as TransactionId, rewindCause(), { start: 1 as TransactionId, end: 3 as TransactionId }), {
          id: 4 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))
})

// =============================================================================
// Watermark Tests
// =============================================================================

describe("CdcStream - Watermark", () => {
  it.effect("Watermark events are not exposed to consumer", () =>
    Effect.gen(function*() {
      const cdc = yield* CdcStream
      const chunk = yield* cdc.streamCdc("SELECT 1").pipe(Stream.runCollect)
      const results = Chunk.toReadonlyArray(chunk)

      // Watermark should be filtered out
      expect(results.length).toBe(0)
    }).pipe(Effect.provide(
      makeTestLayer([
        [watermarkEvent(1 as TransactionId, [makeBlockRange("eth", 0, 10)], null), {
          id: 1 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))

  it.effect("Watermark auto-commits", () =>
    Effect.gen(function*() {
      // Create a shared commitRef that both the mock and test can access
      const commitRef = yield* Ref.make<ReadonlyArray<TransactionId>>([])
      const commit = makeMockCommitHandle(1 as TransactionId, commitRef)

      const testLayer = cdcStreamLayer.pipe(
        Layer.provide(Layer.succeed(
          TransactionalStream,
          {
            streamTransactional: () =>
              Stream.make(
                [watermarkEvent(1 as TransactionId, [makeBlockRange("eth", 0, 10)], null), commit] as const
              ),
            forEach: () => Effect.void
          } satisfies TransactionalStreamService
        )),
        Layer.provide(InMemoryBatchStore.layer)
      )

      yield* Effect.gen(function*() {
        const cdc = yield* CdcStream
        yield* cdc.streamCdc("SELECT 1").pipe(Stream.runDrain)
      }).pipe(Effect.provide(testLayer))

      const committed = yield* Ref.get(commitRef)
      expect(committed).toEqual([1])
    }))

  it.effect("Watermark triggers prune when prune point present", () =>
    Effect.gen(function*() {
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
    }).pipe(Effect.provide(
      makeTestLayerWithBatchStore([
        [watermarkEvent(5 as TransactionId, [makeBlockRange("eth", 0, 10)], 2 as TransactionId), {
          id: 5 as TransactionId,
          commit: Effect.void
        }]
      ])
    )))
})

// =============================================================================
// forEach Tests
// =============================================================================

describe("CdcStream.forEach", () => {
  it.effect("auto-commits after handler succeeds", () =>
    Effect.gen(function*() {
      const commitRef = yield* Ref.make<ReadonlyArray<TransactionId>>([])
      const commit = makeMockCommitHandle(1 as TransactionId, commitRef)
      const handlerEvents = yield* Ref.make<ReadonlyArray<string>>([])

      const testLayer = cdcStreamLayer.pipe(
        Layer.provide(Layer.succeed(
          TransactionalStream,
          {
            streamTransactional: () =>
              Stream.make(
                [dataEvent(1 as TransactionId, [{ block: 1 }], [makeBlockRange("eth", 0, 10)]), commit] as const
              ),
            forEach: () => Effect.void
          } satisfies TransactionalStreamService
        )),
        Layer.provide(InMemoryBatchStore.layer)
      )

      yield* Effect.gen(function*() {
        const cdc = yield* CdcStream
        yield* cdc.forEach(
          "SELECT 1",
          { retention: 128 },
          (event) => Ref.update(handlerEvents, (events) => [...events, event._tag])
        )
      }).pipe(Effect.provide(testLayer))

      const events = yield* Ref.get(handlerEvents)
      expect(events).toEqual(["Insert"])

      // Verify commit was called (forEach auto-commits)
      const committed = yield* Ref.get(commitRef)
      expect(committed).toEqual([1])
    }))
})
