/**
 * InMemoryStateStore - reference implementation of StateStore.
 *
 * Uses Effect Ref for in-memory state management. Not crash-safe but
 * suitable for development, testing, and ephemeral use cases.
 *
 * @module
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import {
  StateStore,
  type StateSnapshot,
  type StateStoreService,
  type Commit,
  emptySnapshot
} from "./state-store.ts"
import type { TransactionId } from "./types.ts"

// =============================================================================
// Implementation
// =============================================================================

/**
 * Create InMemoryStateStore service implementation.
 */
const makeWithInitialState = (initial: StateSnapshot) =>
  Effect.gen(function*() {
    const stateRef = yield* Ref.make<StateSnapshot>(initial)

    const advance = (next: TransactionId) =>
      Ref.update(stateRef, (state) => ({
        ...state,
        next
      }))

    const commit = (commitData: Commit) =>
      Ref.update(stateRef, (state) => {
        let buffer = [...state.buffer]

        // Remove pruned watermarks (all IDs <= prune)
        if (commitData.prune !== undefined) {
          buffer = buffer.filter(([id]) => id > commitData.prune!)
        }

        // Add new watermarks
        for (const entry of commitData.insert) {
          buffer.push(entry)
        }

        return { ...state, buffer }
      })

    const truncate = (from: TransactionId) =>
      Ref.update(stateRef, (state) => ({
        ...state,
        buffer: state.buffer.filter(([id]) => id < from)
      }))

    const load = () => Ref.get(stateRef)

    return {
      advance,
      commit,
      truncate,
      load
    } satisfies StateStoreService
  })

/**
 * Create InMemoryStateStore service with empty initial state.
 */
const make = makeWithInitialState(emptySnapshot)

// =============================================================================
// Layers
// =============================================================================

/**
 * Layer providing InMemoryStateStore with empty initial state.
 *
 * @example
 * ```typescript
 * const program = Effect.gen(function*() {
 *   const store = yield* StateStore
 *   const snapshot = yield* store.load()
 *   console.log(snapshot.next) // 0
 * })
 *
 * Effect.runPromise(program.pipe(Effect.provide(InMemoryStateStore.layer)))
 * ```
 */
export const layer: Layer.Layer<StateStore> = Layer.effect(StateStore, make)

/**
 * Create a layer with pre-populated initial state.
 *
 * Useful for testing scenarios that require specific initial conditions.
 *
 * @example
 * ```typescript
 * const testState: StateSnapshot = {
 *   buffer: [[5 as TransactionId, [{ network: "eth", ... }]]],
 *   next: 10 as TransactionId
 * }
 *
 * const TestLayer = InMemoryStateStore.layerWithState(testState)
 *
 * Effect.runPromise(
 *   program.pipe(Effect.provide(TestLayer))
 * )
 * ```
 */
export const layerWithState = (initial: StateSnapshot): Layer.Layer<StateStore> =>
  Layer.effect(StateStore, makeWithInitialState(initial))

// =============================================================================
// Testing Utilities
// =============================================================================

/**
 * Create an InMemoryStateStore service directly (for testing).
 *
 * Returns both the service and a reference to inspect internal state.
 *
 * @example
 * ```typescript
 * const { service, stateRef } = yield* InMemoryStateStore.makeTestable()
 *
 * yield* service.advance(5 as TransactionId)
 *
 * const state = yield* Ref.get(stateRef)
 * expect(state.next).toBe(5)
 * ```
 */
export const makeTestable = Effect.gen(function*() {
  const stateRef = yield* Ref.make<StateSnapshot>(emptySnapshot)

  const advance = (next: TransactionId) =>
    Ref.update(stateRef, (state) => ({
      ...state,
      next
    }))

  const commit = (commitData: Commit) =>
    Ref.update(stateRef, (state) => {
      let buffer = [...state.buffer]

      if (commitData.prune !== undefined) {
        buffer = buffer.filter(([id]) => id > commitData.prune!)
      }

      for (const entry of commitData.insert) {
        buffer.push(entry)
      }

      return { ...state, buffer }
    })

  const truncate = (from: TransactionId) =>
    Ref.update(stateRef, (state) => ({
      ...state,
      buffer: state.buffer.filter(([id]) => id < from)
    }))

  const load = () => Ref.get(stateRef)

  const service: StateStoreService = {
    advance,
    commit,
    truncate,
    load
  }

  return { service, stateRef }
})

/**
 * Create a layer that also exposes the internal state ref for testing.
 */
export class TestStateRef extends Effect.Service<TestStateRef>()("TestStateRef", {
  effect: Effect.gen(function*() {
    return yield* Ref.make<StateSnapshot>(emptySnapshot)
  })
}) {}

export const testLayer: Layer.Layer<StateStore | TestStateRef> = Layer.effect(
  StateStore,
  Effect.gen(function*() {
    const stateRef = yield* TestStateRef

    const advance = (next: TransactionId) =>
      Ref.update(stateRef, (state) => ({ ...state, next }))

    const commit = (commitData: Commit) =>
      Ref.update(stateRef, (state) => {
        let buffer = [...state.buffer]
        if (commitData.prune !== undefined) {
          buffer = buffer.filter(([id]) => id > commitData.prune!)
        }
        for (const entry of commitData.insert) {
          buffer.push(entry)
        }
        return { ...state, buffer }
      })

    const truncate = (from: TransactionId) =>
      Ref.update(stateRef, (state) => ({
        ...state,
        buffer: state.buffer.filter(([id]) => id < from)
      }))

    const load = () => Ref.get(stateRef)

    return { advance, commit, truncate, load } satisfies StateStoreService
  })
).pipe(Layer.provideMerge(TestStateRef.Default))
