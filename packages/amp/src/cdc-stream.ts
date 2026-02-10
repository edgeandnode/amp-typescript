/**
 * CdcStream module - CDC (Change Data Capture) with at-least-once delivery.
 *
 * Wraps TransactionalStream with a BatchStore to provide Insert/Delete events.
 * Watermarks are handled internally; consumers only see Insert and Delete.
 *
 * @example
 * ```typescript
 * import {
 *   BatchStore,
 *   CdcStream,
 *   type CdcEvent,
 *   InMemoryBatchStore
 * } from "@edgeandnode/amp/cdc-stream"
 *
 * const program = Effect.gen(function*() {
 *   const cdc = yield* CdcStream
 *
 *   yield* cdc.forEach(
 *     "SELECT * FROM eth.logs WHERE address = '0x...'",
 *     { retention: 128 },
 *     Effect.fnUntraced(function*(event) {
 *       switch (event._tag) {
 *         case "Insert":
 *           yield* forwardInsert(event.id, event.data)
 *           break
 *         case "Delete": {
 *           let batch = yield* event.batches.next
 *           while (batch !== undefined) {
 *             const [id, data] = batch
 *             yield* forwardDelete(id, data)
 *             batch = yield* event.batches.next
 *           }
 *           break
 *         }
 *       }
 *     })
 *   )
 * })
 *
 * const AppLayer = CdcStream.layer.pipe(
 *   Layer.provide(InMemoryBatchStore.layer),
 *   Layer.provide(TransactionalStream.layer),
 *   Layer.provide(InMemoryStateStore.layer),
 *   Layer.provide(ProtocolStream.layer),
 *   Layer.provide(ArrowFlight.layer),
 *   Layer.provide(Transport.layer)
 * )
 *
 * Effect.runPromise(program.pipe(Effect.provide(AppLayer)))
 * ```
 *
 * @module
 */

// =============================================================================
// Events
// =============================================================================

export {
  type CdcEvent,
  type CdcEventDelete,
  type CdcEventInsert,
  type DeleteBatchIterator
} from "./cdc-stream/types.ts"

// =============================================================================
// Errors
// =============================================================================

export { BatchStoreError, type CdcStreamError } from "./cdc-stream/errors.ts"

// =============================================================================
// BatchStore Service
// =============================================================================

export { BatchStore, type BatchStoreService } from "./cdc-stream/batch-store.ts"

// =============================================================================
// InMemoryBatchStore Layer
// =============================================================================

export * as InMemoryBatchStore from "./cdc-stream/memory-batch-store.ts"

// =============================================================================
// CdcStream Service
// =============================================================================

export { CdcStream, type CdcStreamOptions, type CdcStreamService, layer } from "./cdc-stream/stream.ts"
