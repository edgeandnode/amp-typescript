/**
 * Protocol Stream - provides protocol-level stream processing with reorg detection.
 *
 * This module provides the `ProtocolStream` service and related types for
 * processing Amp streams with stateless reorg detection.
 *
 * ## Overview
 *
 * The protocol stream interprets raw batches from Arrow Flight and emits
 * three types of messages:
 *
 * - **Data**: New records to process, along with the block ranges they cover
 * - **Reorg**: Chain reorganization detected, with invalidation ranges
 * - **Watermark**: Confirmation that block ranges are complete
 *
 * ## Usage
 *
 * ```typescript
 * import * as Effect from "effect/Effect"
 * import * as Stream from "effect/Stream"
 * import { ProtocolStream } from "@edgeandnode/amp/protocol-stream"
 * import { ArrowFlight, Transport } from "@edgeandnode/amp"
 *
 * const program = Effect.gen(function*() {
 *   const protocolStream = yield* ProtocolStream
 *
 *   yield* protocolStream.stream("SELECT * FROM eth.logs").pipe(
 *     Stream.runForEach((message) => {
 *       switch (message._tag) {
 *         case "Data":
 *           return Effect.log(`Received ${message.data.length} records`)
 *         case "Reorg":
 *           return Effect.log(`Reorg: invalidating ${message.invalidation.length} ranges`)
 *         case "Watermark":
 *           return Effect.log(`Watermark at block ${message.ranges[0]?.numbers.end}`)
 *       }
 *     })
 *   )
 * })
 *
 * Effect.runPromise(program.pipe(
 *   Effect.provide(ProtocolStream.layer),
 *   Effect.provide(ArrowFlight.layer),
 *   Effect.provide(Transport.layer)
 * ))
 * ```
 *
 * ## Reorg Detection
 *
 * The stream detects chain reorganizations by monitoring block range progression:
 *
 * 1. **Consecutive blocks**: Normal progression where new blocks build on previous
 * 2. **Backwards jump**: Indicates a reorg where the chain has forked
 * 3. **Forward gap**: Protocol violation (should never happen)
 *
 * When a reorg is detected, the stream emits a `Reorg` message containing:
 * - The previous known block ranges
 * - The new incoming block ranges
 * - Specific invalidation ranges describing which blocks are affected
 *
 * ## Validation
 *
 * The stream validates protocol invariants:
 * - Hash chain integrity (prevHash must match previous block's hash)
 * - Network consistency (same networks across all batches)
 * - No gaps in block sequences
 *
 * Validation errors terminate the stream, as they indicate protocol violations
 * from the server that cannot be recovered without reconnection.
 *
 * @module
 */

// =============================================================================
// Messages
// =============================================================================

export {
  InvalidationRange,
  ProtocolMessage,
  ProtocolMessageData,
  ProtocolMessageReorg,
  ProtocolMessageWatermark,
  data,
  invalidates,
  makeInvalidationRange,
  reorg,
  watermark
} from "./messages.ts"

// =============================================================================
// Errors
// =============================================================================

export {
  // Validation errors
  DuplicateNetworkError,
  GapError,
  HashMismatchOnConsecutiveBlocksError,
  InvalidPrevHashError,
  InvalidReorgError,
  MissingPrevHashError,
  NetworkCountChangedError,
  UnexpectedNetworkError,
  type ValidationError,

  // Protocol stream errors
  ProtocolArrowFlightError,
  ProtocolValidationError,
  type ProtocolStreamError
} from "./errors.ts"

// =============================================================================
// Validation
// =============================================================================

export {
  validateAll,
  validateConsecutiveness,
  validateNetworks,
  validatePrevHash
} from "./validation.ts"

// =============================================================================
// Service
// =============================================================================

export {
  ProtocolStream,
  layer,
  type ProtocolStreamService,
  type ProtocolStreamOptions
} from "./service.ts"
