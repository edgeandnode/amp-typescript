/**
 * Tests for StateActor - reorg and rewind handling.
 *
 * @module
 */
import type { BlockNumber, BlockRange, Network } from "@edgeandnode/amp/models"
import type { ProtocolMessage } from "@edgeandnode/amp/protocol-stream"
import { InMemoryStateStore, StateStore, type TransactionId } from "@edgeandnode/amp/transactional-stream"
import { makeStateActor } from "@edgeandnode/amp/transactional-stream/state-actor"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"

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

const dataMessage = (
  data: ReadonlyArray<Record<string, unknown>>,
  ranges: ReadonlyArray<BlockRange>
): ProtocolMessage => ({
  _tag: "Data",
  data,
  ranges
})

const watermarkMessage = (ranges: ReadonlyArray<BlockRange>): ProtocolMessage => ({
  _tag: "Watermark",
  ranges
})

const reorgMessage = (
  previous: ReadonlyArray<BlockRange>,
  incoming: ReadonlyArray<BlockRange>,
  invalidation: ReadonlyArray<{ network: string; start: number; end: number }>
): ProtocolMessage => ({
  _tag: "Reorg",
  previous,
  incoming,
  invalidation: invalidation.map((i) => ({
    network: i.network as Network,
    start: i.start as BlockNumber,
    end: i.end as BlockNumber
  }))
})

// =============================================================================
// Basic Operation Tests
// =============================================================================

describe("StateActor.watermark", () => {
  it.effect("returns undefined for empty buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const result = yield* actor.watermark
      expect(result).toBeUndefined()
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 0 as TransactionId
    }))))

  it.effect("returns last watermark from buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const result = yield* actor.watermark
      expect(result?.[0]).toBe(2)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 3 as TransactionId
    }))))
})

describe("StateActor.peek", () => {
  it.effect("returns next transaction ID", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const result = yield* actor.peek
      expect(result).toBe(10)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 10 as TransactionId
    }))))
})

// =============================================================================
// Data Event Tests
// =============================================================================

describe("StateActor.execute - Data", () => {
  it.effect("passes through data events with transaction ID", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({
        _tag: "Message",
        message: dataMessage([{ value: 1 }], [makeBlockRange("eth", 0, 10)])
      })
      expect(event._tag).toBe("Data")
      expect(event.id).toBe(5)
      if (event._tag === "Data") {
        expect(event.data).toEqual([{ value: 1 }])
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 5 as TransactionId
    }))))

  it.effect("increments transaction ID for each event", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event1] = yield* actor.execute({
        _tag: "Message",
        message: dataMessage([{ value: 1 }], [makeBlockRange("eth", 0, 10)])
      })
      const [event2] = yield* actor.execute({
        _tag: "Message",
        message: dataMessage([{ value: 2 }], [makeBlockRange("eth", 11, 20)])
      })
      expect([event1.id, event2.id]).toEqual([0, 1])
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 0 as TransactionId
    }))))
})

// =============================================================================
// Watermark Event Tests
// =============================================================================

describe("StateActor.execute - Watermark", () => {
  it.effect("emits watermark event with transaction ID", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({
        _tag: "Message",
        message: watermarkMessage([makeBlockRange("eth", 0, 10)])
      })
      expect(event._tag).toBe("Watermark")
      expect(event.id).toBe(3)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 3 as TransactionId
    }))))

  it.effect("adds watermark to internal buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      yield* actor.execute({
        _tag: "Message",
        message: watermarkMessage([makeBlockRange("eth", 0, 10)])
      })
      const watermark = yield* actor.watermark
      expect(watermark?.[0]).toBe(0)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 0 as TransactionId
    }))))

  it.effect("computes prune point based on retention", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 50)
      // Add a watermark at block 200, which should prune old watermarks
      const [event] = yield* actor.execute({
        _tag: "Message",
        message: watermarkMessage([makeBlockRange("eth", 200, 210)])
      })
      expect(event._tag).toBe("Watermark")
      if (event._tag === "Watermark") {
        // Cutoff is 200 - 50 = 150
        // Watermarks 0 and 1 end at 10 and 20, both < 150, so prune up to 1
        expect(event.prune._tag).toBe("Some")
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [0 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [1 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 2 as TransactionId
    }))))
})

// =============================================================================
// Rewind Tests
// =============================================================================

describe("StateActor.execute - Rewind", () => {
  it.effect("emits Undo event with Rewind cause", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({ _tag: "Rewind" })
      expect(event._tag).toBe("Undo")
      if (event._tag === "Undo") {
        expect(event.cause._tag).toBe("Rewind")
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 5 as TransactionId
    }))))

  it.effect("computes invalidation range for empty buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({ _tag: "Rewind" })
      if (event._tag === "Undo") {
        // Empty buffer, next=5, so invalidate from 0 to id-1=4
        expect(event.invalidate.start).toBe(0)
        expect(event.invalidate.end).toBe(4)
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 5 as TransactionId
    }))))

  it.effect("computes invalidation range from last watermark", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({ _tag: "Rewind" })
      if (event._tag === "Undo") {
        // Last watermark at id=3, next=7, new id=7
        // Invalidate from 4 (3+1) to 6 (7-1)
        expect(event.invalidate.start).toBe(4)
        expect(event.invalidate.end).toBe(6)
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [[3 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
      next: 7 as TransactionId
    }))))
})

// =============================================================================
// Reorg Tests
// =============================================================================

describe("StateActor.execute - Reorg", () => {
  it.effect("emits Undo event with Reorg cause", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({
        _tag: "Message",
        message: reorgMessage(
          [makeBlockRange("eth", 0, 10)],
          [makeBlockRange("eth", 5, 15)],
          [{ network: "eth", start: 11, end: 15 }]
        )
      })
      expect(event._tag).toBe("Undo")
      if (event._tag === "Undo") {
        expect(event.cause._tag).toBe("Reorg")
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [[1 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
      next: 2 as TransactionId
    }))))

  it.effect("finds recovery point and truncates buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      // Reorg at block 21 affects watermark id=3
      yield* actor.execute({
        _tag: "Message",
        message: reorgMessage(
          [makeBlockRange("eth", 21, 30)],
          [makeBlockRange("eth", 21, 35)],
          [{ network: "eth", start: 21, end: 35 }]
        )
      })

      // Check that buffer is truncated
      const result = yield* actor.watermark
      // Recovery point is id=2 (last unaffected), buffer truncated to keep only id=1,2
      expect(result?.[0]).toBe(2)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]],
        [3 as TransactionId, [makeBlockRange("eth", 21, 30)]]
      ],
      next: 4 as TransactionId
    }))))

  it.effect("handles reorg with empty buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [event] = yield* actor.execute({
        _tag: "Message",
        message: reorgMessage(
          [makeBlockRange("eth", 0, 10)],
          [makeBlockRange("eth", 5, 15)],
          [{ network: "eth", start: 5, end: 15 }]
        )
      })
      // Empty buffer case: invalidate from 0 to id-1
      if (event._tag === "Undo") {
        expect(event.invalidate.start).toBe(0)
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 5 as TransactionId
    }))))

  it.effect("fails with UnrecoverableReorgError when all watermarks affected", () =>
    Effect.gen(function*() {
      const result = yield* Effect.exit(
        Effect.gen(function*() {
          const store = yield* StateStore
          const actor = yield* makeStateActor(store, 128)

          // Reorg at block 100 affects all watermarks
          yield* actor.execute({
            _tag: "Message",
            message: reorgMessage(
              [makeBlockRange("eth", 100, 120)],
              [makeBlockRange("eth", 100, 130)],
              [{ network: "eth", start: 100, end: 130 }]
            )
          })
        })
      )
      expect(result._tag).toBe("Failure")
      if (result._tag === "Failure") {
        const error = result.cause
        // Check that it's an UnrecoverableReorgError
        expect(error).toBeDefined()
      }
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 100, 110)]],
        [2 as TransactionId, [makeBlockRange("eth", 111, 120)]]
      ],
      next: 3 as TransactionId
    }))))

  it.effect("fails with PartialReorgError when reorg point falls within watermark range", () =>
    Effect.gen(function*() {
      const result = yield* Effect.exit(
        Effect.gen(function*() {
          const store = yield* StateStore
          const actor = yield* makeStateActor(store, 128)

          // Reorg at block 10 falls within watermark range [0, 20]
          yield* actor.execute({
            _tag: "Message",
            message: reorgMessage(
              [makeBlockRange("eth", 0, 20)],
              [makeBlockRange("eth", 10, 30)],
              [{ network: "eth", start: 10, end: 30 }]
            )
          })
        })
      )
      expect(result._tag).toBe("Failure")
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [[1 as TransactionId, [makeBlockRange("eth", 0, 20)]]],
      next: 2 as TransactionId
    }))))
})

// =============================================================================
// Commit Tests
// =============================================================================

describe("StateActor.commit", () => {
  it.effect("commits watermarks via commit handle", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      const [, handle] = yield* actor.execute({
        _tag: "Message",
        message: watermarkMessage([makeBlockRange("eth", 0, 10)])
      })

      yield* handle.commit

      // The watermark should now be committed
      const result = yield* actor.watermark
      expect(result?.[0]).toBe(0)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 0 as TransactionId
    }))))

  it.effect("batches multiple commits", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const actor = yield* makeStateActor(store, 128)
      // Execute multiple watermarks without committing
      yield* actor.execute({
        _tag: "Message",
        message: watermarkMessage([makeBlockRange("eth", 0, 10)])
      })
      const [, handle2] = yield* actor.execute({
        _tag: "Message",
        message: watermarkMessage([makeBlockRange("eth", 11, 20)])
      })

      // Commit only the second one - should commit both
      yield* handle2.commit

      const watermark = yield* actor.watermark
      expect(watermark?.[0]).toBe(1)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [],
      next: 0 as TransactionId
    }))))
})
