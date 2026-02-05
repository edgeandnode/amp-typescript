/**
 * CommitHandle - handle for committing state changes.
 *
 * @module
 */
import type * as Effect from "effect/Effect"
import type { StateStoreError } from "./errors.ts"
import type { TransactionId } from "./types.ts"

// =============================================================================
// CommitHandle Interface
// =============================================================================

/**
 * Handle for committing state changes.
 *
 * Returned with each event from the transactional stream. The user must
 * call `commit()` to persist the state change. If the user doesn't commit
 * and the process crashes, the event will be replayed via Rewind on restart.
 *
 * Commits are idempotent - calling `commit()` multiple times is safe.
 * Subsequent calls after the first are no-ops.
 *
 * @example
 * ```typescript
 * yield* txStream.pipe(
 *   Stream.runForEach(Effect.fnUntraced(function*([event, commitHandle]) {
 *       // Process the event
 *       yield* processEvent(event)
 *
 *       // Commit the state change
 *       yield* commitHandle.commit()
 *     })
 *   )
 * )
 * ```
 */
export interface CommitHandle {
  /**
   * The transaction ID associated with this commit.
   */
  readonly id: TransactionId

  /**
   * Commit all pending changes up to and including this transaction ID.
   *
   * Safe to call multiple times - subsequent calls are no-ops if already committed.
   *
   * Multiple commits can be batched together for efficiency - if you don't commit
   * after every event, the next commit will include all previous uncommitted events.
   */
  readonly commit: () => Effect.Effect<void, StateStoreError>
}

// =============================================================================
// Factory
// =============================================================================

/**
 * Create a CommitHandle.
 *
 * @internal This is used internally by the StateActor.
 */
export const makeCommitHandle = (
  id: TransactionId,
  commitFn: (id: TransactionId) => Effect.Effect<void, StateStoreError>
): CommitHandle => ({
  id,
  commit: () => commitFn(id)
})
