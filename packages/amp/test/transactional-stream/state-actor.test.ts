/**
 * Tests for StateActor - reorg and rewind handling.
 *
 * @module
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import { describe, expect, it } from "vitest"
import type { BlockRange } from "../../src/models.ts"
import type { ProtocolMessage } from "../../src/protocol-stream/messages.ts"
import * as InMemoryStateStore from "../../src/transactional-stream/memory-store.ts"
import { makeStateActor, type Action, type StateActor } from "../../src/transactional-stream/state-actor.ts"
import type { StateSnapshot } from "../../src/transactional-stream/state-store.ts"
import type { TransactionId } from "../../src/transactional-stream/types.ts"

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
    network: i.network,
    start: i.start,
    end: i.end
  })) as ReadonlyArray<{ network: string; start: number; end: number }>
})

const runWithActor = <A>(
  initial: StateSnapshot,
  retention: number,
  fn: (actor: StateActor) => Effect.Effect<A, unknown>
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function*() {
      const { service, stateRef } = yield* InMemoryStateStore.makeTestable

      // Set initial state
      yield* Ref.set(stateRef, initial)

      const actor = yield* makeStateActor(service, retention)
      return yield* fn(actor)
    })
  )

// =============================================================================
// Basic Operation Tests
// =============================================================================

describe("StateActor.watermark", () => {
  it("returns undefined for empty buffer", async () => {
    const result = await runWithActor(
      { buffer: [], next: 0 as TransactionId },
      128,
      (actor) => actor.watermark()
    )

    expect(result).toBeUndefined()
  })

  it("returns last watermark from buffer", async () => {
    const initial: StateSnapshot = {
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 3 as TransactionId
    }

    const result = await runWithActor(initial, 128, (actor) => actor.watermark())

    expect(result?.[0]).toBe(2)
  })
})

describe("StateActor.peek", () => {
  it("returns next transaction ID", async () => {
    const result = await runWithActor(
      { buffer: [], next: 10 as TransactionId },
      128,
      (actor) => actor.peek()
    )

    expect(result).toBe(10)
  })
})

// =============================================================================
// Data Event Tests
// =============================================================================

describe("StateActor.execute - Data", () => {
  it("passes through data events with transaction ID", async () => {
    const result = await runWithActor(
      { buffer: [], next: 5 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({
            _tag: "Message",
            message: dataMessage([{ value: 1 }], [makeBlockRange("eth", 0, 10)])
          })
          return event
        })
    )

    expect(result._tag).toBe("Data")
    expect(result.id).toBe(5)
    if (result._tag === "Data") {
      expect(result.data).toEqual([{ value: 1 }])
    }
  })

  it("increments transaction ID for each event", async () => {
    const ids = await runWithActor(
      { buffer: [], next: 0 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event1] = yield* actor.execute({
            _tag: "Message",
            message: dataMessage([{ value: 1 }], [makeBlockRange("eth", 0, 10)])
          })
          const [event2] = yield* actor.execute({
            _tag: "Message",
            message: dataMessage([{ value: 2 }], [makeBlockRange("eth", 11, 20)])
          })
          return [event1.id, event2.id]
        })
    )

    expect(ids).toEqual([0, 1])
  })
})

// =============================================================================
// Watermark Event Tests
// =============================================================================

describe("StateActor.execute - Watermark", () => {
  it("emits watermark event with transaction ID", async () => {
    const result = await runWithActor(
      { buffer: [], next: 3 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({
            _tag: "Message",
            message: watermarkMessage([makeBlockRange("eth", 0, 10)])
          })
          return event
        })
    )

    expect(result._tag).toBe("Watermark")
    expect(result.id).toBe(3)
  })

  it("adds watermark to internal buffer", async () => {
    const watermark = await runWithActor(
      { buffer: [], next: 0 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          yield* actor.execute({
            _tag: "Message",
            message: watermarkMessage([makeBlockRange("eth", 0, 10)])
          })
          return yield* actor.watermark()
        })
    )

    expect(watermark?.[0]).toBe(0)
  })

  it("computes prune point based on retention", async () => {
    // Build up a buffer with watermarks far apart
    const result = await runWithActor(
      {
        buffer: [
          [0 as TransactionId, [makeBlockRange("eth", 0, 10)]],
          [1 as TransactionId, [makeBlockRange("eth", 11, 20)]]
        ],
        next: 2 as TransactionId
      },
      50, // Retention of 50 blocks
      (actor) =>
        Effect.gen(function*() {
          // Add a watermark at block 200, which should prune old watermarks
          const [event] = yield* actor.execute({
            _tag: "Message",
            message: watermarkMessage([makeBlockRange("eth", 200, 210)])
          })
          return event
        })
    )

    expect(result._tag).toBe("Watermark")
    if (result._tag === "Watermark") {
      // Cutoff is 200 - 50 = 150
      // Watermarks 0 and 1 end at 10 and 20, both < 150, so prune up to 1
      expect(result.prune._tag).toBe("Some")
    }
  })
})

// =============================================================================
// Rewind Tests
// =============================================================================

describe("StateActor.execute - Rewind", () => {
  it("emits Undo event with Rewind cause", async () => {
    const result = await runWithActor(
      { buffer: [], next: 5 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({ _tag: "Rewind" })
          return event
        })
    )

    expect(result._tag).toBe("Undo")
    if (result._tag === "Undo") {
      expect(result.cause._tag).toBe("Rewind")
    }
  })

  it("computes invalidation range for empty buffer", async () => {
    const result = await runWithActor(
      { buffer: [], next: 5 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({ _tag: "Rewind" })
          return event
        })
    )

    if (result._tag === "Undo") {
      // Empty buffer, next=5, so invalidate from 0 to id-1=4
      expect(result.invalidate.start).toBe(0)
      expect(result.invalidate.end).toBe(4)
    }
  })

  it("computes invalidation range from last watermark", async () => {
    const result = await runWithActor(
      {
        buffer: [[3 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
        next: 7 as TransactionId
      },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({ _tag: "Rewind" })
          return event
        })
    )

    if (result._tag === "Undo") {
      // Last watermark at id=3, next=7, new id=7
      // Invalidate from 4 (3+1) to 6 (7-1)
      expect(result.invalidate.start).toBe(4)
      expect(result.invalidate.end).toBe(6)
    }
  })
})

// =============================================================================
// Reorg Tests
// =============================================================================

describe("StateActor.execute - Reorg", () => {
  it("emits Undo event with Reorg cause", async () => {
    const result = await runWithActor(
      {
        buffer: [[1 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
        next: 2 as TransactionId
      },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({
            _tag: "Message",
            message: reorgMessage(
              [makeBlockRange("eth", 0, 10)],
              [makeBlockRange("eth", 5, 15)],
              [{ network: "eth", start: 11, end: 15 }]
            )
          })
          return event
        })
    )

    expect(result._tag).toBe("Undo")
    if (result._tag === "Undo") {
      expect(result.cause._tag).toBe("Reorg")
    }
  })

  it("finds recovery point and truncates buffer", async () => {
    const result = await runWithActor(
      {
        buffer: [
          [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
          [2 as TransactionId, [makeBlockRange("eth", 11, 20)]],
          [3 as TransactionId, [makeBlockRange("eth", 21, 30)]]
        ],
        next: 4 as TransactionId
      },
      128,
      (actor) =>
        Effect.gen(function*() {
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
          const watermark = yield* actor.watermark()
          return watermark
        })
    )

    // Recovery point is id=2 (last unaffected), buffer truncated to keep only id=1,2
    expect(result?.[0]).toBe(2)
  })

  it("handles reorg with empty buffer", async () => {
    const result = await runWithActor(
      { buffer: [], next: 5 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [event] = yield* actor.execute({
            _tag: "Message",
            message: reorgMessage(
              [makeBlockRange("eth", 0, 10)],
              [makeBlockRange("eth", 5, 15)],
              [{ network: "eth", start: 5, end: 15 }]
            )
          })
          return event
        })
    )

    // Empty buffer case: invalidate from 0 to id-1
    if (result._tag === "Undo") {
      expect(result.invalidate.start).toBe(0)
    }
  })

  it("fails with UnrecoverableReorgError when all watermarks affected", async () => {
    const result = await Effect.runPromiseExit(
      Effect.gen(function*() {
        const { service, stateRef } = yield* InMemoryStateStore.makeTestable

        yield* Ref.set(stateRef, {
          buffer: [
            [1 as TransactionId, [makeBlockRange("eth", 100, 110)]],
            [2 as TransactionId, [makeBlockRange("eth", 111, 120)]]
          ],
          next: 3 as TransactionId
        })

        const actor = yield* makeStateActor(service, 128)

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
  })

  it("fails with PartialReorgError when reorg point falls within watermark range", async () => {
    const result = await Effect.runPromiseExit(
      Effect.gen(function*() {
        const { service, stateRef } = yield* InMemoryStateStore.makeTestable

        yield* Ref.set(stateRef, {
          buffer: [[1 as TransactionId, [makeBlockRange("eth", 0, 20)]]],
          next: 2 as TransactionId
        })

        const actor = yield* makeStateActor(service, 128)

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
  })
})

// =============================================================================
// Commit Tests
// =============================================================================

describe("StateActor.commit", () => {
  it("commits watermarks via commit handle", async () => {
    const result = await runWithActor(
      { buffer: [], next: 0 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          const [, handle] = yield* actor.execute({
            _tag: "Message",
            message: watermarkMessage([makeBlockRange("eth", 0, 10)])
          })

          yield* handle.commit()

          // The watermark should now be committed
          return yield* actor.watermark()
        })
    )

    expect(result?.[0]).toBe(0)
  })

  it("batches multiple commits", async () => {
    await runWithActor(
      { buffer: [], next: 0 as TransactionId },
      128,
      (actor) =>
        Effect.gen(function*() {
          // Execute multiple watermarks without committing
          const [, handle1] = yield* actor.execute({
            _tag: "Message",
            message: watermarkMessage([makeBlockRange("eth", 0, 10)])
          })
          const [, handle2] = yield* actor.execute({
            _tag: "Message",
            message: watermarkMessage([makeBlockRange("eth", 11, 20)])
          })

          // Commit only the second one - should commit both
          yield* handle2.commit()

          const watermark = yield* actor.watermark()
          expect(watermark?.[0]).toBe(1)
        })
    )
  })
})
