/**
 * CdcStream service - provides CDC (Change Data Capture) semantics with
 * Insert/Delete events and at-least-once delivery.
 *
 * Wraps TransactionalStream with a BatchStore for persisting batch content,
 * enabling Delete events to replay original data during reorgs and rewinds.
 *
 * @module
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import type { CommitHandle } from "../transactional-stream/commit-handle.ts"
import { TransactionalStream, type TransactionalStreamOptions } from "../transactional-stream/stream.ts"
import type { TransactionId } from "../transactional-stream/types.ts"
import { BatchStore, type BatchStoreService } from "./batch-store.ts"
import type { BatchStoreError, CdcStreamError } from "./errors.ts"
import type { CdcEvent, DeleteBatchIterator } from "./types.ts"

// =============================================================================
// Options
// =============================================================================

export interface CdcStreamOptions extends TransactionalStreamOptions {}

// =============================================================================
// Service Interface
// =============================================================================

export interface CdcStreamService {
  /**
   * Create a CDC stream from a SQL query.
   *
   * Returns tuples of [CdcEvent, CommitHandle]. The consumer must call
   * commit() after processing each event to advance the stream state.
   */
  readonly streamCdc: (
    sql: string,
    options?: CdcStreamOptions
  ) => Stream.Stream<readonly [CdcEvent, CommitHandle], CdcStreamError>

  /**
   * High-level consumer: auto-commit after callback succeeds.
   */
  readonly forEach: <E, R>(
    sql: string,
    options: CdcStreamOptions,
    handler: (event: CdcEvent) => Effect.Effect<void, E, R>
  ) => Effect.Effect<void, CdcStreamError | E, R>
}

// =============================================================================
// Context.Tag
// =============================================================================

export class CdcStream extends Context.Tag("Amp/CdcStream")<
  CdcStream,
  CdcStreamService
>() {}

// =============================================================================
// DeleteBatchIterator
// =============================================================================

/**
 * Create a lazy batch iterator that loads batches one-by-one from the store.
 * Skips missing batches (watermark-only transactions).
 */
const makeDeleteBatchIterator = (
  store: BatchStoreService,
  ids: ReadonlyArray<TransactionId>
): DeleteBatchIterator => {
  let cursor = 0

  return {
    next: Effect.suspend(() => {
      const loop = (): Effect.Effect<
        readonly [TransactionId, ReadonlyArray<Record<string, unknown>>] | undefined,
        BatchStoreError
      > => {
        if (cursor >= ids.length) return Effect.succeed(undefined)
        const id = ids[cursor]!
        cursor++
        return store.load(id).pipe(
          Effect.flatMap((batch) =>
            batch !== undefined
              ? Effect.succeed([id, batch] as const)
              : loop()
          )
        )
      }
      return loop()
    })
  }
}

// =============================================================================
// Implementation
// =============================================================================

const make = Effect.gen(function*() {
  const txStream = yield* TransactionalStream
  const batchStore = yield* BatchStore

  const streamCdc = (sql: string, options?: CdcStreamOptions) => {
    return txStream.streamTransactional(sql, options).pipe(
      Stream.mapEffect(
        Effect.fnUntraced(function*([event, commit]): Effect.fn.Return<
          Option.Option<readonly [CdcEvent, CommitHandle]>,
          CdcStreamError
        > {
          switch (event._tag) {
            case "Data": {
              // Store batch BEFORE emitting — guarantees availability for Delete
              yield* batchStore.append(event.data, event.id)
              const insert: CdcEvent = {
                _tag: "Insert",
                id: event.id,
                data: event.data,
                ranges: event.ranges
              }
              return Option.some([insert, commit] as const)
            }

            case "Undo": {
              // Find batch IDs in the invalidation range
              const ids = yield* batchStore.seek(event.invalidate)

              if (ids.length === 0) {
                // No batches to delete — skip (don't emit empty Delete)
                return Option.none()
              }

              // Build lazy iterator
              const iterator = makeDeleteBatchIterator(batchStore, ids)
              const del: CdcEvent = {
                _tag: "Delete",
                id: event.id,
                batches: iterator
              }
              return Option.some([del, commit] as const)
            }

            case "Watermark": {
              // Handle batch pruning when retention window moves
              if (Option.isSome(event.prune)) {
                // Best-effort pruning
                yield* batchStore.prune(event.prune.value).pipe(
                  Effect.catchAll((error) =>
                    Effect.logWarning("Batch pruning failed (will retry on next watermark)", error)
                  )
                )
              }
              // Watermarks are not exposed to CDC consumers
              // Still need to commit so the underlying TransactionalStream advances
              yield* commit.commit
              return Option.none()
            }
          }
        })
      ),
      Stream.filterMap((x) => x),
      Stream.withSpan("CdcStream.streamCdc")
    )
  }

  // forEach auto-commits after handler succeeds
  const forEach = <E, R>(
    sql: string,
    options: CdcStreamOptions,
    handler: (event: CdcEvent) => Effect.Effect<void, E, R>
  ): Effect.Effect<void, CdcStreamError | E, R> =>
    streamCdc(sql, options).pipe(
      Stream.runForEach(
        Effect.fnUntraced(function*([event, commitHandle]) {
          yield* handler(event)
          yield* commitHandle.commit
        })
      ),
      Effect.withSpan("CdcStream.forEach")
    )

  return { streamCdc, forEach } satisfies CdcStreamService
})

// =============================================================================
// Layer
// =============================================================================

/**
 * Layer providing CdcStream.
 *
 * Requires TransactionalStream and BatchStore in context.
 */
export const layer: Layer.Layer<CdcStream, never, TransactionalStream | BatchStore> = Layer.effect(CdcStream, make)
