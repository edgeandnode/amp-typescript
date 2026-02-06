/**
 * InMemoryStateStore - reference implementation of StateStore.
 *
 * Uses Effect Ref for in-memory state management. Not crash-safe but
 * suitable for development, testing, and ephemeral use cases.
 *
 * @module
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import { emptySnapshot, type StateSnapshot, StateStore, type StateStoreService } from "./state-store.ts"

// =============================================================================
// Implementation
// =============================================================================

/**
 * Build a StateStoreService backed by a single Ref.
 */
const makeServiceFromRef = (stateRef: Ref.Ref<StateSnapshot>): StateStoreService => ({
  advance: (next) =>
    Ref.update(stateRef, (state) => ({
      ...state,
      next
    })),

  commit: (commitData) =>
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
    }),

  truncate: (from) =>
    Ref.update(stateRef, (state) => ({
      ...state,
      buffer: state.buffer.filter(([id]) => id < from)
    })),

  load: Ref.get(stateRef)
})

/**
 * Create InMemoryStateStore service implementation.
 */
const makeWithInitialState = Effect.fnUntraced(function*(initial: StateSnapshot): Effect.fn.Return<StateStoreService> {
  const stateRef = yield* Ref.make<StateSnapshot>(initial)
  return makeServiceFromRef(stateRef)
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
 *   const snapshot = yield* store.load
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
 * Service tag exposing the internal state of a test store for inspection.
 *
 * Provided alongside `StateStore` by {@link layerTest}.
 *
 * @example
 * ```typescript
 * const testState = yield* InMemoryStateStore.TestState
 * const snapshot = yield* testState.get
 * expect(snapshot.next).toBe(5)
 * ```
 */
export class TestState extends Context.Tag("Amp/TransactionalStream/TestState")<
  TestState,
  { readonly get: Effect.Effect<StateSnapshot> }
>() {}

/**
 * Test layer providing both `StateStore` and `TestState`.
 *
 * Creates a fresh in-memory store and exposes its internal state
 * via the `TestState` tag, allowing tests to inspect snapshots
 * without needing a raw Ref.
 *
 * @example
 * ```typescript
 * const program = Effect.gen(function*() {
 *   const store = yield* StateStore
 *   const testState = yield* InMemoryStateStore.TestState
 *
 *   yield* store.advance(5 as TransactionId)
 *
 *   const snapshot = yield* testState.get
 *   expect(snapshot.next).toBe(5)
 * })
 *
 * Effect.runPromise(program.pipe(Effect.provide(InMemoryStateStore.layerTest)))
 * ```
 */
export const layerTest: Layer.Layer<TestState | StateStore> = Layer.effectContext(
  Effect.gen(function*() {
    const ref = yield* Ref.make(emptySnapshot)

    const state = TestState.of({
      get: Ref.get(ref)
    })

    const store = StateStore.of(makeServiceFromRef(ref))

    return Context.mergeAll(
      Context.make(StateStore, store),
      Context.make(TestState, state)
    )
  })
)
