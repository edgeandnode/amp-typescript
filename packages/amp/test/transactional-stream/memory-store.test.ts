/**
 * Tests for InMemoryStateStore.
 *
 * @module
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import { describe, expect, it } from "vitest"
import type { BlockRange } from "../../src/models.ts"
import * as InMemoryStateStore from "../../src/transactional-stream/memory-store.ts"
import { StateStore, emptySnapshot, type StateSnapshot } from "../../src/transactional-stream/state-store.ts"
import type { TransactionId } from "../../src/transactional-stream/types.ts"

// =============================================================================
// Test Helpers
// =============================================================================

const makeBlockRange = (
  network: string,
  start: number,
  end: number
): BlockRange =>
  ({
    network,
    numbers: { start, end },
    hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    prevHash: undefined
  }) as BlockRange

const runWithStore = <A, E>(
  effect: Effect.Effect<A, E, StateStore>
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(Effect.provide(InMemoryStateStore.layer))
  )

const runWithState = <A, E>(
  initial: StateSnapshot,
  effect: Effect.Effect<A, E, StateStore>
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(Effect.provide(InMemoryStateStore.layerWithState(initial)))
  )

// =============================================================================
// Layer Tests
// =============================================================================

describe("InMemoryStateStore.layer", () => {
  it("provides empty initial state", async () => {
    const result = await runWithStore(
      Effect.gen(function*() {
        const store = yield* StateStore
        return yield* store.load()
      })
    )

    expect(result).toEqual(emptySnapshot)
  })
})

describe("InMemoryStateStore.layerWithState", () => {
  it("provides custom initial state", async () => {
    const initial: StateSnapshot = {
      buffer: [[5 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
      next: 10 as TransactionId
    }

    const result = await runWithState(
      initial,
      Effect.gen(function*() {
        const store = yield* StateStore
        return yield* store.load()
      })
    )

    expect(result).toEqual(initial)
  })
})

// =============================================================================
// StateStore Operations Tests
// =============================================================================

describe("StateStore.advance", () => {
  it("updates the next transaction ID", async () => {
    const result = await runWithStore(
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.advance(5 as TransactionId)

        const snapshot = yield* store.load()
        return snapshot.next
      })
    )

    expect(result).toBe(5)
  })

  it("can be called multiple times", async () => {
    const result = await runWithStore(
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.advance(1 as TransactionId)
        yield* store.advance(2 as TransactionId)
        yield* store.advance(10 as TransactionId)

        const snapshot = yield* store.load()
        return snapshot.next
      })
    )

    expect(result).toBe(10)
  })
})

describe("StateStore.commit", () => {
  it("inserts watermarks to buffer", async () => {
    const result = await runWithStore(
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.commit({
          insert: [
            [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
            [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
          ],
          prune: undefined
        })

        const snapshot = yield* store.load()
        return snapshot.buffer
      })
    )

    expect(result).toHaveLength(2)
    expect(result[0]![0]).toBe(1)
    expect(result[1]![0]).toBe(2)
  })

  it("prunes watermarks with ID <= prune point", async () => {
    const initial: StateSnapshot = {
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]],
        [3 as TransactionId, [makeBlockRange("eth", 21, 30)]]
      ],
      next: 4 as TransactionId
    }

    const result = await runWithState(
      initial,
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.commit({
          insert: [],
          prune: 2 as TransactionId
        })

        const snapshot = yield* store.load()
        return snapshot.buffer
      })
    )

    expect(result).toHaveLength(1)
    expect(result[0]![0]).toBe(3)
  })

  it("can insert and prune atomically", async () => {
    const initial: StateSnapshot = {
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 3 as TransactionId
    }

    const result = await runWithState(
      initial,
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.commit({
          insert: [[3 as TransactionId, [makeBlockRange("eth", 21, 30)]]],
          prune: 1 as TransactionId
        })

        const snapshot = yield* store.load()
        return snapshot.buffer
      })
    )

    expect(result).toHaveLength(2)
    expect(result.map(([id]) => id)).toEqual([2, 3])
  })
})

describe("StateStore.truncate", () => {
  it("removes watermarks with ID >= from", async () => {
    const initial: StateSnapshot = {
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]],
        [3 as TransactionId, [makeBlockRange("eth", 21, 30)]]
      ],
      next: 4 as TransactionId
    }

    const result = await runWithState(
      initial,
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.truncate(2 as TransactionId)

        const snapshot = yield* store.load()
        return snapshot.buffer
      })
    )

    expect(result).toHaveLength(1)
    expect(result[0]![0]).toBe(1)
  })

  it("removes all watermarks when truncating from 0", async () => {
    const initial: StateSnapshot = {
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 3 as TransactionId
    }

    const result = await runWithState(
      initial,
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.truncate(0 as TransactionId)

        const snapshot = yield* store.load()
        return snapshot.buffer
      })
    )

    expect(result).toHaveLength(0)
  })

  it("preserves next ID", async () => {
    const initial: StateSnapshot = {
      buffer: [[1 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
      next: 10 as TransactionId
    }

    const result = await runWithState(
      initial,
      Effect.gen(function*() {
        const store = yield* StateStore

        yield* store.truncate(1 as TransactionId)

        const snapshot = yield* store.load()
        return snapshot.next
      })
    )

    expect(result).toBe(10)
  })
})

// =============================================================================
// makeTestable Tests
// =============================================================================

describe("InMemoryStateStore.makeTestable", () => {
  it("exposes internal state ref for inspection", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const { service, stateRef } = yield* InMemoryStateStore.makeTestable

        yield* service.advance(5 as TransactionId)
        yield* service.commit({
          insert: [[1 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
          prune: undefined
        })

        const state = yield* Ref.get(stateRef)
        return state
      })
    )

    expect(result.next).toBe(5)
    expect(result.buffer).toHaveLength(1)
  })
})

// =============================================================================
// testLayer Tests
// =============================================================================

describe("InMemoryStateStore.testLayer", () => {
  it("provides both StateStore and TestStateRef", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const store = yield* StateStore
        const stateRef = yield* InMemoryStateStore.TestStateRef

        yield* store.advance(5 as TransactionId)

        const state = yield* Ref.get(stateRef)
        return state.next
      }).pipe(Effect.provide(InMemoryStateStore.testLayer))
    )

    expect(result).toBe(5)
  })
})
