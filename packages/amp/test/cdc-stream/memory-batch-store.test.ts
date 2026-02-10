/**
 * Tests for InMemoryBatchStore.
 *
 * @module
 */
import {
  BatchStore,
  InMemoryBatchStore,
  type TransactionId,
  type TransactionIdRange
} from "@edgeandnode/amp/cdc-stream"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"

// =============================================================================
// append + load Tests
// =============================================================================

describe("BatchStore.append + load", () => {
  it.effect("stores data retrievable by load", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore
      const data = [{ block: 1, address: "0xabc" }]

      yield* store.append(data, 1 as TransactionId)

      const result = yield* store.load(1 as TransactionId)
      expect(result).toEqual(data)
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))

  it.effect("load returns undefined for missing IDs", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore

      const result = yield* store.load(999 as TransactionId)
      expect(result).toBeUndefined()
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))

  it.effect("stores multiple batches independently", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore
      const data1 = [{ block: 1 }]
      const data2 = [{ block: 2 }]

      yield* store.append(data1, 1 as TransactionId)
      yield* store.append(data2, 2 as TransactionId)

      expect(yield* store.load(1 as TransactionId)).toEqual(data1)
      expect(yield* store.load(2 as TransactionId)).toEqual(data2)
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))
})

// =============================================================================
// seek Tests
// =============================================================================

describe("BatchStore.seek", () => {
  it.effect("returns sorted IDs within range", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore

      yield* store.append([{ a: 1 }], 3 as TransactionId)
      yield* store.append([{ a: 2 }], 1 as TransactionId)
      yield* store.append([{ a: 3 }], 5 as TransactionId)
      yield* store.append([{ a: 4 }], 2 as TransactionId)

      const range: TransactionIdRange = {
        start: 1 as TransactionId,
        end: 3 as TransactionId
      }
      const result = yield* store.seek(range)
      expect(result).toEqual([1, 2, 3])
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))

  it.effect("returns empty array for empty range", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore

      yield* store.append([{ a: 1 }], 1 as TransactionId)

      const range: TransactionIdRange = {
        start: 10 as TransactionId,
        end: 20 as TransactionId
      }
      const result = yield* store.seek(range)
      expect(result).toEqual([])
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))

  it.effect("includes boundary IDs", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore

      yield* store.append([{ a: 1 }], 1 as TransactionId)
      yield* store.append([{ a: 2 }], 5 as TransactionId)
      yield* store.append([{ a: 3 }], 10 as TransactionId)

      const range: TransactionIdRange = {
        start: 1 as TransactionId,
        end: 10 as TransactionId
      }
      const result = yield* store.seek(range)
      expect(result).toEqual([1, 5, 10])
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))
})

// =============================================================================
// prune Tests
// =============================================================================

describe("BatchStore.prune", () => {
  it.effect("removes batches up to cutoff (inclusive)", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore

      yield* store.append([{ a: 1 }], 1 as TransactionId)
      yield* store.append([{ a: 2 }], 2 as TransactionId)
      yield* store.append([{ a: 3 }], 3 as TransactionId)

      yield* store.prune(2 as TransactionId)

      expect(yield* store.load(1 as TransactionId)).toBeUndefined()
      expect(yield* store.load(2 as TransactionId)).toBeUndefined()
      expect(yield* store.load(3 as TransactionId)).toEqual([{ a: 3 }])
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))

  it.effect("is idempotent", () =>
    Effect.gen(function*() {
      const store = yield* BatchStore

      yield* store.append([{ a: 1 }], 1 as TransactionId)
      yield* store.append([{ a: 2 }], 2 as TransactionId)

      yield* store.prune(1 as TransactionId)
      yield* store.prune(1 as TransactionId)

      expect(yield* store.load(1 as TransactionId)).toBeUndefined()
      expect(yield* store.load(2 as TransactionId)).toEqual([{ a: 2 }])
    }).pipe(Effect.provide(InMemoryBatchStore.layer)))
})
