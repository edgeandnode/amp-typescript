import * as Effect from "effect/Effect"
import * as Predicate from "effect/Predicate"
import { InvalidMessageTypeError, MissingFieldError } from "./errors.ts"
import { FlatBufferReader } from "./flatbuffer-reader.ts"

// =============================================================================
// Constants
// =============================================================================

export const MessageHeaderType = {
  NONE: 0,
  SCHEMA: 1,
  DICTIONARY_BATCH: 2,
  RECORD_BATCH: 3,
  TENSOR: 4,
  SPARSE_TENSOR: 5
} as const
export type MessageHeaderType = typeof MessageHeaderType[keyof typeof MessageHeaderType]

// =============================================================================
// Types
// =============================================================================

/**
 * A partial representation of the `FlightData` type.
 */
export interface FlightData {
  readonly dataHeader: Uint8Array
  readonly dataBody: Uint8Array
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Returns the Arrow Flight message type from the `FlightData` header.
 */
export const getMessageType = Effect.fn(function*(flightData: FlightData) {
  const reader = new FlatBufferReader(flightData.dataHeader)

  // The flatbuffer root table offset is at position 0
  const rootOffset = reader.readOffset(0)

  // Read the position of the message header union type discriminator
  const headerTypePosition = reader.getFieldPosition(rootOffset, 1)

  if (Predicate.isNull(headerTypePosition)) {
    return yield* new MissingFieldError({
      fieldName: "header",
      fieldIndex: 1,
      tableOffset: rootOffset
    })
  }

  const headerType = reader.readUint8(headerTypePosition)

  if (headerType < 0 || headerType > 5) {
    return yield* new InvalidMessageTypeError({ value: headerType })
  }

  return headerType as MessageHeaderType
})
