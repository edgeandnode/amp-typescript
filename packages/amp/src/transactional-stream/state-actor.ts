/**
 * StateActor - internal state management for TransactionalStream.
 *
 * Wraps a StateStore with a Ref for concurrent-safe in-memory state management.
 * Created once per stream instance. Not exported publicly.
 *
 * @module
 * @internal
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import type { BlockRange } from "../core/domain.ts"
import type { ProtocolMessage } from "../protocol-stream/messages.ts"
import {
  checkPartialReorg,
  compressCommits,
  findPruningPoint,
  findRecoveryPoint,
  type PendingCommit
} from "./algorithms.ts"
import { type CommitHandle, makeCommitHandle } from "./commit-handle.ts"
import { PartialReorgError, type StateStoreError, UnrecoverableReorgError } from "./errors.ts"
import type { StateStoreService } from "./state-store.ts"
import {
  dataEvent,
  reorgCause,
  rewindCause,
  type TransactionEvent,
  type TransactionId,
  type TransactionIdRange,
  undoEvent,
  watermarkEvent
} from "./types.ts"

// =============================================================================
// Types
// =============================================================================

/**
 * Internal state container - in-memory copy of persisted state.
 */
interface StateContainer {
  /** Next transaction ID to assign */
  next: TransactionId
  /** Watermark buffer (oldest to newest) */
  buffer: Array<readonly [TransactionId, ReadonlyArray<BlockRange>]>
  /** Uncommitted watermarks awaiting user commit */
  uncommitted: Array<readonly [TransactionId, PendingCommit]>
}

/**
 * Action union for execute().
 */
export type Action =
  | { readonly _tag: "Message"; readonly message: ProtocolMessage }
  | { readonly _tag: "Rewind" }

/**
 * StateActor interface - manages transactional stream state.
 */
export interface StateActor {
  /** Get last watermark from buffer */
  readonly watermark: Effect.Effect<readonly [TransactionId, ReadonlyArray<BlockRange>] | undefined>

  /** Get next transaction ID without incrementing */
  readonly peek: Effect.Effect<TransactionId>

  /** Execute an action and return event with commit handle */
  readonly execute: (
    action: Action
  ) => Effect.Effect<
    readonly [TransactionEvent, CommitHandle],
    StateStoreError | UnrecoverableReorgError | PartialReorgError
  >

  /** Commit pending changes up to and including this ID */
  readonly commit: (id: TransactionId) => Effect.Effect<void, StateStoreError>
}

// =============================================================================
// Factory
// =============================================================================

/**
 * Create a StateActor from a StateStore service.
 *
 * @param store - The StateStore service to wrap
 * @param retention - Retention window in blocks for pruning
 * @returns Effect that creates a StateActor
 */
export const makeStateActor = Effect.fnUntraced(
  function*(store: StateStoreService, retention: number): Effect.fn.Return<StateActor, StateStoreError> {
    // Load initial state from store
    const snapshot = yield* store.load

    // Create mutable state container wrapped in Ref
    const containerRef = yield* Ref.make<StateContainer>({
      next: snapshot.next,
      buffer: [...snapshot.buffer],
      uncommitted: []
    })

    // =========================================================================
    // watermark()
    // =========================================================================

    const watermark = Ref.get(containerRef).pipe(
      Effect.map((state) =>
        state.buffer.length > 0
          ? state.buffer[state.buffer.length - 1]
          : undefined
      )
    )

    // =========================================================================
    // peek()
    // =========================================================================

    const peek = Ref.get(containerRef).pipe(Effect.map((state) => state.next))

    // =========================================================================
    // execute()
    // =========================================================================

    const execute = Effect.fnUntraced(function*(action: Action): Effect.fn.Return<
      readonly [TransactionEvent, CommitHandle],
      StateStoreError | UnrecoverableReorgError | PartialReorgError
    > {
      // 1. Pre-allocate monotonic ID
      const id = yield* Ref.getAndUpdate(containerRef, (state) => ({
        ...state,
        next: (state.next + 1) as TransactionId
      })).pipe(Effect.map((state) => state.next))

      const nextId = (id + 1) as TransactionId

      // Persist the new next ID immediately (ensures monotonicity survives crashes)
      yield* store.advance(nextId)

      // 2. Execute action based on type
      const event: TransactionEvent = yield* (() => {
        switch (action._tag) {
          case "Rewind":
            return executeRewind(id, containerRef)

          case "Message":
            return executeMessage(id, action.message, containerRef, store, retention)
        }
      })()

      // 3. Return event with commit handle
      const handle = makeCommitHandle(id, commit)

      return [event, handle] as const
    })

    // =========================================================================
    // commit()
    // =========================================================================

    const commit = Effect.fnUntraced(function*(id: TransactionId): Effect.fn.Return<void, StateStoreError> {
      const state = yield* Ref.get(containerRef)

      // Find position where IDs become > id (all before this are <= id)
      const pos = state.uncommitted.findIndex(([currentId]) => currentId > id)
      const endIndex = pos === -1 ? state.uncommitted.length : pos

      if (endIndex === 0) {
        // Nothing to commit
        return
      }

      // Collect commits [0..endIndex)
      const pending = state.uncommitted.slice(0, endIndex)

      // Compress and persist
      const compressed = compressCommits(pending)

      if (compressed.insert.length > 0 || compressed.prune !== undefined) {
        // Apply pruning to in-memory buffer
        yield* Ref.update(containerRef, (s) => {
          let buffer = s.buffer
          if (compressed.prune !== undefined) {
            buffer = buffer.filter(([bufferId]) => bufferId > compressed.prune!)
          }
          return { ...s, buffer }
        })

        // Persist to store
        yield* store.commit({
          insert: compressed.insert,
          prune: compressed.prune
        })
      }

      // Remove committed from uncommitted queue
      yield* Ref.update(containerRef, (s) => ({
        ...s,
        uncommitted: s.uncommitted.slice(endIndex)
      }))
    })

    return { watermark, peek, execute, commit }
  }
)

// =============================================================================
// Helpers
// =============================================================================

/**
 * Compute the invalidation range for an undo event.
 *
 * @param afterId - The anchor transaction ID (invalidation starts at afterId + 1), or undefined to start from 0
 * @param currentId - The current transaction ID being assigned to the undo event
 */
const computeInvalidationRange = (
  afterId: TransactionId | undefined,
  currentId: TransactionId
): TransactionIdRange => {
  const start = (afterId !== undefined ? afterId + 1 : 0) as TransactionId
  const end = Math.max(start, currentId - 1) as TransactionId
  return { start, end }
}

// =============================================================================
// Action Handlers
// =============================================================================

/**
 * Execute a Rewind action.
 */
const executeRewind = Effect.fnUntraced(function*(
  id: TransactionId,
  containerRef: Ref.Ref<StateContainer>
): Effect.fn.Return<TransactionEvent> {
  const state = yield* Ref.get(containerRef)

  const anchor = state.buffer.length > 0
    ? state.buffer[state.buffer.length - 1]![0]
    : undefined

  return undoEvent(id, rewindCause(), computeInvalidationRange(anchor, id))
})

/**
 * Execute a Message action.
 */
const executeMessage = Effect.fnUntraced(function*(
  id: TransactionId,
  message: ProtocolMessage,
  containerRef: Ref.Ref<StateContainer>,
  store: StateStoreService,
  retention: number
): Effect.fn.Return<TransactionEvent, StateStoreError | UnrecoverableReorgError | PartialReorgError> {
  switch (message._tag) {
    case "Data":
      // Data events just pass through - no buffer mutation
      return dataEvent(id, message.data, message.ranges)

    case "Watermark":
      return yield* executeWatermark(id, message.ranges, containerRef, retention)

    case "Reorg":
      return yield* executeReorg(id, message, containerRef, store)
  }
})

/**
 * Execute a Watermark message.
 */
const executeWatermark = Effect.fnUntraced(function*(
  id: TransactionId,
  ranges: ReadonlyArray<BlockRange>,
  containerRef: Ref.Ref<StateContainer>,
  retention: number
): Effect.fn.Return<TransactionEvent> {
  // Add watermark to buffer
  yield* Ref.update(containerRef, (state) => ({
    ...state,
    buffer: [...state.buffer, [id, ranges] as const]
  }))

  // Compute pruning point based on current buffer state
  const state = yield* Ref.get(containerRef)
  const prune = findPruningPoint(state.buffer, retention)

  // Record in uncommitted queue
  yield* Ref.update(containerRef, (s) => ({
    ...s,
    uncommitted: [...s.uncommitted, [id, { ranges, prune }] as const]
  }))

  return watermarkEvent(id, ranges, prune ?? null)
})

/**
 * Execute a Reorg message.
 */
const executeReorg = Effect.fnUntraced(function*(
  id: TransactionId,
  message: Extract<ProtocolMessage, { _tag: "Reorg" }>,
  containerRef: Ref.Ref<StateContainer>,
  store: StateStoreService
): Effect.fn.Return<TransactionEvent, StateStoreError | UnrecoverableReorgError | PartialReorgError> {
  const state = yield* Ref.get(containerRef)
  const { invalidation } = message

  // 1. Find recovery point
  const recovery = findRecoveryPoint(state.buffer, invalidation)

  // 2. Determine anchor ID for invalidation range
  let anchor: TransactionId | undefined

  if (recovery === undefined) {
    if (state.buffer.length === 0) {
      // Empty buffer: invalidate everything from 0
      anchor = undefined
    } else {
      // No recovery point with a non-empty buffer means all buffered watermarks
      // are affected by the reorg. This is not recoverable.
      return yield* Effect.fail(
        new UnrecoverableReorgError({
          reason: "All buffered watermarks are affected by the reorg"
        })
      )
    }
  } else {
    const [recoveryId, recoveryRanges] = recovery

    // 3. Check for partial reorg
    const partialNetwork = checkPartialReorg(recoveryRanges, invalidation)
    if (partialNetwork !== undefined) {
      return yield* Effect.fail(
        new PartialReorgError({
          reason: "Recovery point doesn't align with reorg boundary",
          network: partialNetwork
        })
      )
    }

    anchor = recoveryId
  }

  // 4. Compute invalidation range and truncate both in-memory and store
  const range = computeInvalidationRange(anchor, id)

  yield* Ref.update(containerRef, (s) => ({
    ...s,
    buffer: s.buffer.filter(([bufferId]) => bufferId < range.start),
    uncommitted: s.uncommitted.filter(([uncommittedId]) => uncommittedId < range.start)
  }))

  yield* store.truncate(range.start)

  // 5. Emit Undo with Cause::Reorg
  return undoEvent(id, reorgCause(invalidation), range)
})
