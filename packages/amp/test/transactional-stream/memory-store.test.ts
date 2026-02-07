/**
 * Tests for InMemoryStateStore.
 *
 * @module
 */
import type { BlockRange } from "@edgeandnode/amp/core"
import {
  emptySnapshot,
  InMemoryStateStore,
  type StateSnapshot,
  StateStore,
  type TransactionId
} from "@edgeandnode/amp/transactional-stream"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"

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

// =============================================================================
// Layer Tests
// =============================================================================

describe("InMemoryStateStore.layer", () => {
  it.effect("provides empty initial state", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const result = yield* store.load
      expect(result).toEqual(emptySnapshot)
    }).pipe(Effect.provide(InMemoryStateStore.layer)))
})

describe("InMemoryStateStore.layerWithState", () => {
  it.effect("provides custom initial state", () =>
    Effect.gen(function*() {
      const initial: StateSnapshot = {
        buffer: [[5 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
        next: 10 as TransactionId
      }

      const store = yield* StateStore
      const result = yield* store.load
      expect(result).toEqual(initial)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [[5 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
      next: 10 as TransactionId
    }))))
})

// =============================================================================
// StateStore Operations Tests
// =============================================================================

describe("StateStore.advance", () => {
  it.effect("updates the next transaction ID", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.advance(5 as TransactionId)

      const snapshot = yield* store.load
      expect(snapshot.next).toBe(5)
    }).pipe(Effect.provide(InMemoryStateStore.layer)))

  it.effect("can be called multiple times", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.advance(1 as TransactionId)
      yield* store.advance(2 as TransactionId)
      yield* store.advance(10 as TransactionId)

      const snapshot = yield* store.load
      expect(snapshot.next).toBe(10)
    }).pipe(Effect.provide(InMemoryStateStore.layer)))
})

describe("StateStore.commit", () => {
  it.effect("inserts watermarks to buffer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.commit({
        insert: [
          [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
          [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
        ],
        prune: undefined
      })

      const snapshot = yield* store.load
      const result = snapshot.buffer
      expect(result).toHaveLength(2)
      expect(result[0]![0]).toBe(1)
      expect(result[1]![0]).toBe(2)
    }).pipe(Effect.provide(InMemoryStateStore.layer)))

  it.effect("prunes watermarks with ID <= prune point", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.commit({
        insert: [],
        prune: 2 as TransactionId
      })

      const snapshot = yield* store.load
      const result = snapshot.buffer
      expect(result).toHaveLength(1)
      expect(result[0]![0]).toBe(3)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]],
        [3 as TransactionId, [makeBlockRange("eth", 21, 30)]]
      ],
      next: 4 as TransactionId
    }))))

  it.effect("can insert and prune atomically", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.commit({
        insert: [[3 as TransactionId, [makeBlockRange("eth", 21, 30)]]],
        prune: 1 as TransactionId
      })

      const snapshot = yield* store.load
      const result = snapshot.buffer
      expect(result).toHaveLength(2)
      expect(result.map(([id]) => id)).toEqual([2, 3])
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 3 as TransactionId
    }))))
})

describe("StateStore.truncate", () => {
  it.effect("removes watermarks with ID >= from", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.truncate(2 as TransactionId)

      const snapshot = yield* store.load
      const result = snapshot.buffer
      expect(result).toHaveLength(1)
      expect(result[0]![0]).toBe(1)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]],
        [3 as TransactionId, [makeBlockRange("eth", 21, 30)]]
      ],
      next: 4 as TransactionId
    }))))

  it.effect("removes all watermarks when truncating from 0", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.truncate(0 as TransactionId)

      const snapshot = yield* store.load
      const result = snapshot.buffer
      expect(result).toHaveLength(0)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [
        [1 as TransactionId, [makeBlockRange("eth", 0, 10)]],
        [2 as TransactionId, [makeBlockRange("eth", 11, 20)]]
      ],
      next: 3 as TransactionId
    }))))

  it.effect("preserves next ID", () =>
    Effect.gen(function*() {
      const store = yield* StateStore

      yield* store.truncate(1 as TransactionId)

      const snapshot = yield* store.load
      expect(snapshot.next).toBe(10)
    }).pipe(Effect.provide(InMemoryStateStore.layerWithState({
      buffer: [[1 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
      next: 10 as TransactionId
    }))))
})

// =============================================================================
// layerTest Tests
// =============================================================================

describe("InMemoryStateStore.layerTest", () => {
  it.effect("exposes internal state via TestState", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const testState = yield* InMemoryStateStore.TestState

      yield* store.advance(5 as TransactionId)
      yield* store.commit({
        insert: [[1 as TransactionId, [makeBlockRange("eth", 0, 10)]]],
        prune: undefined
      })

      const result = yield* testState.get
      expect(result.next).toBe(5)
      expect(result.buffer).toHaveLength(1)
    }).pipe(Effect.provide(InMemoryStateStore.layerTest)))

  it.effect("provides StateStore via layer", () =>
    Effect.gen(function*() {
      const store = yield* StateStore
      const testState = yield* InMemoryStateStore.TestState

      yield* store.advance(5 as TransactionId)

      const state = yield* testState.get
      expect(state.next).toBe(5)
    }).pipe(Effect.provide(InMemoryStateStore.layerTest)))
})
