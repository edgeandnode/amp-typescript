/**
 * InMemoryBatchStore - reference implementation of BatchStore.
 *
 * Uses Effect Ref for in-memory state management. Not crash-safe but
 * suitable for development, testing, and ephemeral use cases.
 *
 * @module
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import type { TransactionId, TransactionIdRange } from "../transactional-stream/types.ts"
import { BatchStore, type BatchStoreService } from "./batch-store.ts"

// =============================================================================
// Types
// =============================================================================

type BatchMap = ReadonlyMap<TransactionId, ReadonlyArray<Record<string, unknown>>>

// =============================================================================
// Implementation
// =============================================================================

const make = Effect.gen(function*() {
  const mapRef = yield* Ref.make<BatchMap>(new Map())

  const append = (data: ReadonlyArray<Record<string, unknown>>, id: TransactionId) =>
    Ref.update(mapRef, (map) => new Map([...map, [id, data]]))

  const seek = (range: TransactionIdRange) =>
    Ref.get(mapRef).pipe(
      Effect.map((map) => {
        const ids: Array<TransactionId> = []
        for (const id of map.keys()) {
          if (id >= range.start && id <= range.end) ids.push(id)
        }
        return ids.sort((a, b) => a - b)
      })
    )

  const load = (id: TransactionId) =>
    Ref.get(mapRef).pipe(
      Effect.map((map) => map.get(id))
    )

  const prune = (cutoff: TransactionId) =>
    Ref.update(mapRef, (map) => {
      const next = new Map<TransactionId, ReadonlyArray<Record<string, unknown>>>()
      for (const [id, data] of map) {
        if (id > cutoff) next.set(id, data)
      }
      return next
    })

  return { append, seek, load, prune } satisfies BatchStoreService
})

// =============================================================================
// Layers
// =============================================================================

/**
 * Layer providing InMemoryBatchStore with empty initial state.
 */
export const layer: Layer.Layer<BatchStore> = Layer.effect(BatchStore, make)
