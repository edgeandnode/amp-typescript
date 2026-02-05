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
import type { BlockRange } from "../models.ts"
import type { ProtocolMessage } from "../protocol-stream/messages.ts"
import { findRecoveryPoint, findPruningPoint, checkPartialReorg, compressCommits } from "./algorithms.ts"
import { makeCommitHandle, type CommitHandle } from "./commit-handle.ts"
import { PartialReorgError, type StateStoreError, UnrecoverableReorgError } from "./errors.ts"
import type { StateStoreService } from "./state-store.ts"
import {
  type TransactionId,
  type TransactionEvent,
  dataEvent,
  undoEvent,
  watermarkEvent,
  reorgCause,
  rewindCause
} from "./types.ts"

// =============================================================================
// Types
// =============================================================================

/**
 * Pending commit waiting for user to call commit handle.
 */
interface PendingCommit {
  readonly ranges: ReadonlyArray<BlockRange>
  readonly prune: TransactionId | undefined
}

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
  readonly watermark: () => Effect.Effect<readonly [TransactionId, ReadonlyArray<BlockRange>] | undefined>

  /** Get next transaction ID without incrementing */
  readonly peek: () => Effect.Effect<TransactionId>

  /** Execute an action and return event with commit handle */
  readonly execute: (
    action: Action
  ) => Effect.Effect<readonly [TransactionEvent, CommitHandle], StateStoreError | UnrecoverableReorgError | PartialReorgError>

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
export const makeStateActor = (
  store: StateStoreService,
  retention: number
): Effect.Effect<StateActor, StateStoreError> =>
  Effect.gen(function*() {
    // Load initial state from store
    const snapshot = yield* store.load()

    // Create mutable state container wrapped in Ref
    const containerRef = yield* Ref.make<StateContainer>({
      next: snapshot.next,
      buffer: [...snapshot.buffer],
      uncommitted: []
    })

    // =========================================================================
    // watermark()
    // =========================================================================

    const watermark = () =>
      Ref.get(containerRef).pipe(
        Effect.map((state) =>
          state.buffer.length > 0
            ? state.buffer[state.buffer.length - 1]
            : undefined
        )
      )

    // =========================================================================
    // peek()
    // =========================================================================

    const peek = () => Ref.get(containerRef).pipe(Effect.map((state) => state.next))

    // =========================================================================
    // execute()
    // =========================================================================

    const execute = (action: Action) =>
      Effect.gen(function*() {
        // 1. Pre-allocate monotonic ID
        const id = yield* Ref.getAndUpdate(containerRef, (state) => ({
          ...state,
          next: (state.next + 1) as TransactionId
        })).pipe(Effect.map((state) => state.next))

        const nextId = (id + 1) as TransactionId

        // Persist the new next ID immediately (ensures monotonicity survives crashes)
        yield* store.advance(nextId)

        // 2. Execute action based on type
        const event: TransactionEvent = yield* ((): Effect.Effect<
          TransactionEvent,
          StateStoreError | UnrecoverableReorgError | PartialReorgError
        > => {
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

    const commit = (id: TransactionId): Effect.Effect<void, StateStoreError> =>
      Effect.gen(function*() {
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
  })

// =============================================================================
// Action Handlers
// =============================================================================

/**
 * Execute a Rewind action.
 */
const executeRewind = (
  id: TransactionId,
  containerRef: Ref.Ref<StateContainer>
): Effect.Effect<TransactionEvent> =>
  Effect.gen(function*() {
    const state = yield* Ref.get(containerRef)

    // Compute invalidation range based on buffer state
    let invalidateStart: TransactionId
    let invalidateEnd: TransactionId

    if (state.buffer.length === 0) {
      // Empty buffer (early crash before any watermark): invalidate from the beginning
      invalidateStart = 0 as TransactionId
      invalidateEnd = Math.max(0, id - 1) as TransactionId
    } else {
      // Normal rewind: invalidate after last watermark
      const lastWatermarkId = state.buffer[state.buffer.length - 1]![0]
      invalidateStart = (lastWatermarkId + 1) as TransactionId
      invalidateEnd = Math.max(invalidateStart, id - 1) as TransactionId
    }

    return undoEvent(id, rewindCause(), {
      start: invalidateStart,
      end: invalidateEnd
    })
  })

/**
 * Execute a Message action.
 */
const executeMessage = (
  id: TransactionId,
  message: ProtocolMessage,
  containerRef: Ref.Ref<StateContainer>,
  store: StateStoreService,
  retention: number
): Effect.Effect<TransactionEvent, StateStoreError | UnrecoverableReorgError | PartialReorgError> =>
  Effect.gen(function*() {
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
const executeWatermark = (
  id: TransactionId,
  ranges: ReadonlyArray<BlockRange>,
  containerRef: Ref.Ref<StateContainer>,
  retention: number
): Effect.Effect<TransactionEvent> =>
  Effect.gen(function*() {
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
const executeReorg = (
  id: TransactionId,
  message: Extract<ProtocolMessage, { _tag: "Reorg" }>,
  containerRef: Ref.Ref<StateContainer>,
  store: StateStoreService
): Effect.Effect<TransactionEvent, StateStoreError | UnrecoverableReorgError | PartialReorgError> =>
  Effect.gen(function*() {
    const state = yield* Ref.get(containerRef)
    const { invalidation } = message

    // 1. Find recovery point
    const recovery = findRecoveryPoint(state.buffer, invalidation)

    // 2. Compute invalidation range
    let invalidateStart: TransactionId
    let invalidateEnd: TransactionId

    if (recovery === undefined) {
      if (state.buffer.length === 0) {
        // If the buffer is empty, invalidate everything up to before the current event
        invalidateStart = 0 as TransactionId
        invalidateEnd = Math.max(0, id - 1) as TransactionId
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

      invalidateStart = (recoveryId + 1) as TransactionId
      invalidateEnd = Math.max(invalidateStart, id - 1) as TransactionId
    }

    // 4. Truncate both in-memory and store
    const truncateFrom = recovery !== undefined ? (recovery[0] + 1) as TransactionId : 0 as TransactionId

    yield* Ref.update(containerRef, (s) => ({
      ...s,
      buffer: s.buffer.filter(([bufferId]) => bufferId < truncateFrom),
      uncommitted: s.uncommitted.filter(([uncommittedId]) => uncommittedId < truncateFrom)
    }))

    yield* store.truncate(truncateFrom)

    // 5. Emit Undo with Cause::Reorg
    return undoEvent(id, reorgCause(invalidation), {
      start: invalidateStart,
      end: invalidateEnd
    })
  })
