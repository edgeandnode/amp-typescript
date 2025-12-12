/**
 * String and Binary Type Generators
 * @internal
 */
import type * as Schema from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Effect from "effect/Effect"
import * as BufferUtils from "../BufferUtils.ts"
import * as Rand from "../RandomUtils.ts"
import type * as Types from "../Types.ts"

// =============================================================================
// UTF-8 Generator
// =============================================================================

export const utf8Generator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minLength = config.minLength ?? 0
      const maxLength = config.maxLength ?? 50
      const values: Array<string | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextStringBetween(minLength, maxLength))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const { data, offsets } = BufferUtils.createVariableLengthBuffers(values, false)

      return {
        validity: bitmap,
        offsets,
        data,
        children: [],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Large UTF-8 Generator
// =============================================================================

export const largeUtf8Generator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minLength = config.minLength ?? 0
      const maxLength = config.maxLength ?? 50
      const values: Array<string | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextStringBetween(minLength, maxLength))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const { data, offsets } = BufferUtils.createVariableLengthBuffers(values, true)

      return {
        validity: bitmap,
        offsets,
        data,
        children: [],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Binary Generator
// =============================================================================

export const binaryGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minLength = config.minLength ?? 0
      const maxLength = config.maxLength ?? 50
      const values: Array<Uint8Array | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextBytesBetween(minLength, maxLength))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const { data, offsets } = BufferUtils.createVariableLengthBuffers(values, false)

      return {
        validity: bitmap,
        offsets,
        data,
        children: [],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Large Binary Generator
// =============================================================================

export const largeBinaryGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minLength = config.minLength ?? 0
      const maxLength = config.maxLength ?? 50
      const values: Array<Uint8Array | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextBytesBetween(minLength, maxLength))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const { data, offsets } = BufferUtils.createVariableLengthBuffers(values, true)

      return {
        validity: bitmap,
        offsets,
        data,
        children: [],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Fixed-Size Binary Generator
// =============================================================================

export const fixedSizeBinaryGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.FixedSizeBinaryType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const values: Array<Uint8Array | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextBytes(type.byteWidth))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createFixedSizeBinaryDataBuffer(values, type.byteWidth)

      return {
        validity: bitmap,
        offsets: null,
        data,
        children: [],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}
