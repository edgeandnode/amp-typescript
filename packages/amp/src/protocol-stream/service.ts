/**
 * ProtocolStream service - provides protocol-level stream processing with reorg detection.
 *
 * The ProtocolStream wraps ArrowFlight's raw streaming with:
 * - Stateless reorg detection via block range progression
 * - Protocol validation (hash chains, network consistency)
 * - Message categorization (Data, Reorg, Watermark)
 *
 * @module
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"
import {
  ArrowFlight,
  type ArrowFlightError,
  type QueryOptions,
  type QueryResult
} from "../arrow-flight.ts"
import type { BlockRange } from "../models.ts"
import {
  ProtocolArrowFlightError,
  ProtocolValidationError,
  type ProtocolStreamError
} from "./errors.ts"
import {
  type InvalidationRange,
  type ProtocolMessage,
  data as protocolData,
  makeInvalidationRange,
  reorg as protocolReorg,
  watermark as protocolWatermark
} from "./messages.ts"
import { validateAll } from "./validation.ts"

// =============================================================================
// Options
// =============================================================================

/**
 * Options for creating a protocol stream.
 */
export interface ProtocolStreamOptions {
  /**
   * Schema to validate and decode the record batch data.
   * If provided, data will be validated against this schema.
   */
  readonly schema?: QueryOptions["schema"]

  /**
   * Resume watermark from a previous session.
   * Allows resumption of streaming queries from a known position.
   */
  readonly resumeWatermark?: ReadonlyArray<BlockRange>
}

// =============================================================================
// Service Interface
// =============================================================================

/**
 * ProtocolStream service interface.
 *
 * Provides protocol-level stream processing on top of raw Arrow Flight streams:
 * - Stateless reorg detection by monitoring block range progression
 * - Protocol validation (hash chains, network consistency, no gaps)
 * - Message categorization into Data, Reorg, and Watermark types
 */
export interface ProtocolStreamService {
  /**
   * Execute a SQL query and return a stream of protocol messages
   * with stateless reorg detection.
   *
   * Protocol messages include:
   * - `Data`: New records to process with block ranges
   * - `Reorg`: Chain reorganization detected with invalidation ranges
   * - `Watermark`: Confirmation that block ranges are complete
   *
   * @example
   * ```typescript
   * const protocolStream = yield* ProtocolStream
   *
   * yield* protocolStream.stream("SELECT * FROM eth.logs").pipe(
   *   Stream.runForEach((message) => {
   *     switch (message._tag) {
   *       case "Data":
   *         return Effect.log(`Data: ${message.data.length} records`)
   *       case "Reorg":
   *         return Effect.log(`Reorg: ${message.invalidation.length} ranges`)
   *       case "Watermark":
   *         return Effect.log(`Watermark confirmed`)
   *     }
   *   })
   * )
   * ```
   */
  readonly stream: (
    sql: string,
    options?: ProtocolStreamOptions
  ) => Stream.Stream<ProtocolMessage, ProtocolStreamError>
}

// Re-export ProtocolStreamError from errors.ts for convenience
export type { ProtocolStreamError }

// =============================================================================
// Context.Tag
// =============================================================================

/**
 * ProtocolStream Context.Tag - use this to depend on ProtocolStream in Effects.
 *
 * @example
 * ```typescript
 * const program = Effect.gen(function*() {
 *   const protocolStream = yield* ProtocolStream
 *   yield* protocolStream.stream("SELECT * FROM eth.logs").pipe(
 *     Stream.runForEach((message) => Effect.log(message._tag))
 *   )
 * })
 *
 * Effect.runPromise(program.pipe(
 *   Effect.provide(ProtocolStream.layer),
 *   Effect.provide(ArrowFlight.layer),
 *   Effect.provide(Transport.layer)
 * ))
 * ```
 */
export class ProtocolStream extends Context.Tag("Amp/ProtocolStream")<
  ProtocolStream,
  ProtocolStreamService
>() {}

// =============================================================================
// Implementation
// =============================================================================

/**
 * Internal state maintained by the protocol stream for reorg detection.
 */
interface ProtocolStreamState {
  readonly previous: ReadonlyArray<BlockRange>
  readonly initialized: boolean
}

/**
 * Detects reorgs by comparing incoming ranges to previous ranges.
 *
 * A reorg is detected when the incoming block range starts before or at
 * the previous range's end (a "backwards jump").
 */
const detectReorgs = (
  previous: ReadonlyArray<BlockRange>,
  incoming: ReadonlyArray<BlockRange>
): ReadonlyArray<InvalidationRange> => {
  const invalidations: Array<InvalidationRange> = []

  for (const incomingRange of incoming) {
    const prevRange = previous.find((p) => p.network === incomingRange.network)
    if (!prevRange) continue

    // Skip identical ranges (watermarks can repeat)
    if (
      incomingRange.network === prevRange.network &&
      incomingRange.numbers.start === prevRange.numbers.start &&
      incomingRange.numbers.end === prevRange.numbers.end &&
      incomingRange.hash === prevRange.hash &&
      incomingRange.prevHash === prevRange.prevHash
    ) {
      continue
    }

    const incomingStart = incomingRange.numbers.start
    const prevEnd = prevRange.numbers.end

    // Detect backwards jump (reorg indicator)
    if (incomingStart < prevEnd + 1) {
      invalidations.push(
        makeInvalidationRange(
          incomingRange.network,
          incomingStart,
          Math.max(incomingRange.numbers.end, prevEnd)
        )
      )
    }
  }

  return invalidations
}

const ZERO_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000"

/**
 * Create ProtocolStream service implementation.
 */
const make = Effect.gen(function*() {
  const arrowFlight = yield* ArrowFlight

  const stream = (
    sql: string,
    options?: ProtocolStreamOptions
  ): Stream.Stream<ProtocolMessage, ProtocolStreamError> => {
    // Get the underlying Arrow Flight stream
    const rawStream = arrowFlight.streamQuery(sql, {
      schema: options?.schema,
      stream: true,
      resumeWatermark: options?.resumeWatermark
    }) as unknown as Stream.Stream<
      QueryResult<ReadonlyArray<Record<string, unknown>>>,
      ArrowFlightError
    >

    const initialState: ProtocolStreamState = {
      previous: [],
      initialized: false
    }

    return rawStream.pipe(
      // Map Arrow Flight errors to protocol errors
      Stream.mapError((error: ArrowFlightError) => new ProtocolArrowFlightError({ cause: error })),

      // Process each batch with state tracking
      Stream.mapAccumEffect(initialState, (state, queryResult) =>
        Effect.gen(function*() {
          const batchData = queryResult.data
          const metadata = queryResult.metadata
          const incoming = metadata.ranges

          // Validate the incoming batch
          if (state.initialized) {
            yield* validateAll(state.previous, incoming).pipe(
              Effect.mapError((error) => new ProtocolValidationError({ cause: error }))
            )
          } else {
            // Validate prevHash for first batch
            for (const range of incoming) {
              const isGenesis = range.numbers.start === 0
              if (isGenesis) {
                if (range.prevHash !== undefined && range.prevHash !== ZERO_HASH) {
                  return yield* Effect.fail(
                    new ProtocolValidationError({
                      cause: { _tag: "InvalidPrevHashError", network: range.network }
                    })
                  )
                }
              } else {
                if (range.prevHash === undefined || range.prevHash === ZERO_HASH) {
                  return yield* Effect.fail(
                    new ProtocolValidationError({
                      cause: { _tag: "MissingPrevHashError", network: range.network, block: range.numbers.start }
                    })
                  )
                }
              }
            }
          }

          // Detect reorgs
          const invalidations = state.initialized ? detectReorgs(state.previous, incoming) : []

          // Determine message type
          let message: ProtocolMessage

          if (invalidations.length > 0) {
            message = protocolReorg(state.previous, incoming, invalidations)
          } else if (metadata.rangesComplete && batchData.length === 0) {
            message = protocolWatermark(incoming)
          } else {
            message = protocolData(batchData as unknown as ReadonlyArray<Record<string, unknown>>, incoming)
          }

          const newState: ProtocolStreamState = {
            previous: incoming,
            initialized: true
          }

          return [newState, message] as const
        })),

      Stream.withSpan("ProtocolStream.stream")
    )
  }

  return { stream } satisfies ProtocolStreamService
})

// =============================================================================
// Layer
// =============================================================================

/**
 * Layer providing ProtocolStream.
 *
 * Requires ArrowFlight in context.
 *
 * @example
 * ```typescript
 * const AppLayer = ProtocolStream.layer.pipe(
 *   Layer.provide(ArrowFlight.layer),
 *   Layer.provide(Transport.layer)
 * )
 * ```
 */
export const layer: Layer.Layer<ProtocolStream, never, ArrowFlight> =
  Layer.effect(ProtocolStream, make)
