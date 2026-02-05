/**
 * TransactionalStream service - provides exactly-once semantics for data processing.
 *
 * The TransactionalStream wraps ProtocolStream with:
 * - Transaction IDs for each event
 * - Crash recovery via persistent state
 * - Rewind detection for uncommitted transactions
 * - CommitHandle for explicit commit control
 *
 * @module
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import type { QueryOptions } from "../arrow-flight.ts"
import type { BlockRange } from "../models.ts"
import type { ProtocolStreamError } from "../protocol-stream/errors.ts"
import { ProtocolStream, type ProtocolStreamOptions } from "../protocol-stream/service.ts"
import type { CommitHandle } from "./commit-handle.ts"
import type { StateStoreError, TransactionalStreamError, UnrecoverableReorgError, PartialReorgError } from "./errors.ts"
import { type Action, makeStateActor, type StateActor } from "./state-actor.ts"
import { StateStore } from "./state-store.ts"
import type { TransactionEvent, TransactionId } from "./types.ts"

// =============================================================================
// Options
// =============================================================================

/**
 * Options for creating a transactional stream.
 * Note: StateStore is NOT passed here - it comes from the Layer context.
 */
export interface TransactionalStreamOptions {
  /**
   * Retention window in blocks for pruning old watermarks.
   * Watermarks older than this will be pruned to save memory.
   * @default 128
   */
  readonly retention?: number

  /**
   * Optional schema for data validation.
   * If provided, data will be validated and decoded using this schema.
   */
  readonly schema?: QueryOptions["schema"]
}

// =============================================================================
// Service Interface
// =============================================================================

/**
 * TransactionalStream service interface.
 *
 * Provides transactional semantics on top of the protocol stream:
 * - Each event has a unique, monotonically increasing transaction ID
 * - Events can be committed explicitly via CommitHandle
 * - Uncommitted events are replayed on restart via Rewind
 * - Reorgs emit Undo events with invalidation ranges
 */
export interface TransactionalStreamService {
  /**
   * Create a transactional stream from a SQL query.
   *
   * Returns tuples of [event, commitHandle] for manual commit control.
   * If you don't call commit() and the process crashes, events will be
   * replayed via a Rewind event on restart.
   *
   * @example
   * ```typescript
   * const txStream = yield* TransactionalStream
   *
   * yield* txStream.streamTransactional("SELECT * FROM eth.logs", { retention: 128 }).pipe(
   *   Stream.runForEach(([event, commitHandle]) =>
   *     Effect.gen(function*() {
   *       yield* processEvent(event)
   *       yield* commitHandle.commit()
   *     })
   *   )
   * )
   * ```
   */
  readonly streamTransactional: (
    sql: string,
    options?: TransactionalStreamOptions
  ) => Stream.Stream<
    readonly [TransactionEvent, CommitHandle],
    TransactionalStreamError
  >

  /**
   * High-level consumer: auto-commit after callback succeeds.
   *
   * If callback fails, stream stops and pending batch remains uncommitted.
   * On restart, uncommitted batch triggers Rewind.
   *
   * @example
   * ```typescript
   * const txStream = yield* TransactionalStream
   *
   * yield* txStream.forEach(
   *   "SELECT * FROM eth.logs",
   *   { retention: 128 },
   *   (event) => Effect.gen(function*() {
   *     switch (event._tag) {
   *       case "Data":
   *         yield* processData(event.data)
   *         break
   *       case "Undo":
   *         yield* rollback(event.invalidate)
   *         break
   *       case "Watermark":
   *         yield* checkpoint(event.ranges)
   *         break
   *     }
   *   })
   * )
   * ```
   */
  readonly forEach: <E, R>(
    sql: string,
    options: TransactionalStreamOptions,
    handler: (event: TransactionEvent) => Effect.Effect<void, E, R>
  ) => Effect.Effect<void, TransactionalStreamError | E, R>
}

// =============================================================================
// Context.Tag
// =============================================================================

/**
 * TransactionalStream Context.Tag - use this to depend on TransactionalStream in Effects.
 *
 * @example
 * ```typescript
 * const program = Effect.gen(function*() {
 *   const txStream = yield* TransactionalStream
 *   yield* txStream.forEach("SELECT * FROM eth.logs", {}, processEvent)
 * })
 *
 * Effect.runPromise(program.pipe(
 *   Effect.provide(TransactionalStream.layer),
 *   Effect.provide(InMemoryStateStore.layer),
 *   Effect.provide(ProtocolStream.layer),
 *   Effect.provide(ArrowFlight.layer),
 *   Effect.provide(Transport.layer)
 * ))
 * ```
 */
export class TransactionalStream extends Context.Tag("Amp/TransactionalStream")<
  TransactionalStream,
  TransactionalStreamService
>() {}

// =============================================================================
// Implementation
// =============================================================================

const DEFAULT_RETENTION = 128

/**
 * Check if a rewind is needed based on persisted state.
 *
 * A rewind is needed when:
 * - Buffer is empty but next > 0 (crash before any watermark committed)
 * - Buffer has watermarks but next > last_watermark_id + 1 (crash after processing but before commit)
 */
const needsRewind = (
  next: TransactionId,
  lastWatermarkId: TransactionId | undefined
): boolean => {
  if (lastWatermarkId === undefined) {
    // No watermarks - rewind if we've processed anything (next > 0)
    return next > 0
  }
  // Rewind if next ID is beyond what we've committed
  return next > (lastWatermarkId + 1)
}

/**
 * Create TransactionalStream service implementation.
 */
const make = Effect.gen(function*() {
  const protocolStreamService = yield* ProtocolStream
  const storeService = yield* StateStore

  const streamTransactional = (
    sql: string,
    options?: TransactionalStreamOptions
  ): Stream.Stream<
    readonly [TransactionEvent, CommitHandle],
    TransactionalStreamError
  > => {
    const retention = options?.retention ?? DEFAULT_RETENTION

    // Create the stream with proper scoping
    return Stream.unwrapScoped(
      Effect.gen(function*() {
        // 1. Create StateActor
        const actor: StateActor = yield* makeStateActor(storeService, retention)

        // 2. Get current watermark and next ID
        const watermark = yield* actor.watermark()
        const nextId = yield* actor.peek()

        // 3. Determine resume cursor
        const resumeWatermark: ReadonlyArray<BlockRange> | undefined = watermark !== undefined
          ? watermark[1]
          : undefined

        // 4. Check if rewind is needed
        const lastWatermarkId = watermark?.[0]
        const shouldRewind = needsRewind(nextId, lastWatermarkId)

        // 5. Get protocol stream with resume cursor
        const protocolOptions: ProtocolStreamOptions = resumeWatermark !== undefined
          ? { schema: options?.schema, resumeWatermark }
          : { schema: options?.schema }
        const protocolStream = protocolStreamService.stream(sql, protocolOptions)

        // 6. Build transactional stream
        // First emit Rewind if needed, then map protocol messages through actor
        const rewindStream: Stream.Stream<
          readonly [TransactionEvent, CommitHandle],
          StateStoreError | UnrecoverableReorgError | PartialReorgError
        > = shouldRewind
          ? Stream.fromEffect(
              actor.execute({ _tag: "Rewind" })
            )
          : Stream.empty

        const messageStream: Stream.Stream<
          readonly [TransactionEvent, CommitHandle],
          ProtocolStreamError | StateStoreError | UnrecoverableReorgError | PartialReorgError
        > = protocolStream.pipe(
          Stream.mapEffect((message) =>
            actor.execute({ _tag: "Message", message } as Action)
          )
        )

        return Stream.concat(rewindStream, messageStream)
      })
    ).pipe(
      Stream.withSpan("TransactionalStream.streamTransactional")
    )
  }

  const forEach = <E, R>(
    sql: string,
    options: TransactionalStreamOptions,
    handler: (event: TransactionEvent) => Effect.Effect<void, E, R>
  ): Effect.Effect<void, TransactionalStreamError | E, R> =>
    streamTransactional(sql, options).pipe(
      Stream.runForEach(([event, commitHandle]) =>
        Effect.gen(function*() {
          // Process the event
          yield* handler(event)
          // Auto-commit after successful processing
          yield* commitHandle.commit()
        })
      ),
      Effect.withSpan("TransactionalStream.forEach")
    )

  return {
    streamTransactional,
    forEach
  } satisfies TransactionalStreamService
})

// =============================================================================
// Layer
// =============================================================================

/**
 * Layer providing TransactionalStream.
 *
 * Requires ProtocolStream and StateStore in context.
 *
 * @example
 * ```typescript
 * // Development/Testing with InMemoryStateStore
 * const DevLayer = TransactionalStream.layer.pipe(
 *   Layer.provide(InMemoryStateStore.layer),
 *   Layer.provide(ProtocolStream.layer),
 *   Layer.provide(ArrowFlight.layer),
 *   Layer.provide(Transport.layer)
 * )
 *
 * // Production with persistent store (future)
 * const ProdLayer = TransactionalStream.layer.pipe(
 *   Layer.provide(IndexedDBStateStore.layer),
 *   Layer.provide(ProtocolStream.layer),
 *   Layer.provide(ArrowFlight.layer),
 *   Layer.provide(Transport.layer)
 * )
 * ```
 */
export const layer: Layer.Layer<TransactionalStream, never, ProtocolStream | StateStore> =
  Layer.effect(TransactionalStream, make)
