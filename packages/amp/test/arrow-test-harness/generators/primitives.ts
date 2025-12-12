/**
 * Primitive Type Generators
 * @internal
 */
import type * as Schema from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Effect from "effect/Effect"
import * as Random from "effect/Random"
import * as BufferUtils from "../BufferUtils.ts"
import * as Rand from "../RandomUtils.ts"
import type * as Types from "../Types.ts"

// =============================================================================
// Null Generator
// =============================================================================

export const nullGenerator: Types.DataGenerator = {
  generate: (_field, numRows, _config) =>
    Effect.succeed<Types.GeneratorResult>({
      validity: new Uint8Array(0),
      offsets: null,
      data: new Uint8Array(0),
      children: [],
      fieldNode: { length: BigInt(numRows), nullCount: BigInt(numRows) },
      values: new Array(numRows).fill(null)
    })
}

// =============================================================================
// Bool Generator
// =============================================================================

export const boolGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const values: Array<boolean | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Random.nextBoolean)
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createBoolDataBuffer(values)

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

// =============================================================================
// Int Generator
// =============================================================================

const intRanges = {
  8: { signed: { min: -128, max: 127 }, unsigned: { min: 0, max: 255 } },
  16: { signed: { min: -32768, max: 32767 }, unsigned: { min: 0, max: 65535 } },
  32: { signed: { min: -2147483648, max: 2147483647 }, unsigned: { min: 0, max: 4294967295 } }
} as const

const int64Ranges = {
  signed: { min: -9223372036854775808n, max: 9223372036854775807n },
  unsigned: { min: 0n, max: 18446744073709551615n }
} as const

export const intGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.IntType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      if (type.bitWidth === 64) {
        const range = int64Ranges[type.isSigned ? "signed" : "unsigned"]
        const values: Array<bigint | null> = []

        for (let i = 0; i < numRows; i++) {
          const isNull = yield* Rand.nextBoolWithProbability(nullRate)
          if (isNull) {
            values.push(null)
          } else {
            values.push(yield* Rand.nextBigInt(range.min, range.max))
          }
        }

        const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
        const data = BufferUtils.createInt64DataBuffer(values, type.isSigned)

        return {
          validity: bitmap,
          offsets: null,
          data,
          children: [],
          fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
          values
        }
      }

      const range = intRanges[type.bitWidth as 8 | 16 | 32][type.isSigned ? "signed" : "unsigned"]
      const values: Array<number | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Random.nextIntBetween(range.min, range.max))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = type.bitWidth === 8
        ? BufferUtils.createInt8DataBuffer(values, type.isSigned)
        : type.bitWidth === 16
        ? BufferUtils.createInt16DataBuffer(values, type.isSigned)
        : BufferUtils.createInt32DataBuffer(values, type.isSigned)

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

// =============================================================================
// Float Generator
// =============================================================================

// Special float values by precision (precision-appropriate to avoid overflow)
const SPECIAL_FLOATS_BY_PRECISION = {
  HALF: [
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    -0,
    6.1e-5, // Smallest positive float16 subnormal (approx)
    65504, // Largest finite float16
    -65504
  ],
  SINGLE: [
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    -0,
    1.4e-45, // Smallest positive float32 subnormal
    3.4028235e38, // Largest finite float32
    -3.4028235e38,
    1.1920929e-7 // Float32 epsilon
  ],
  DOUBLE: [
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    -0,
    Number.MIN_VALUE, // Smallest positive float64 subnormal (5e-324)
    Number.MAX_VALUE, // Largest finite float64 (1.7e308)
    -Number.MAX_VALUE,
    Number.EPSILON // Float64 epsilon (2.2e-16)
  ]
} as const

// Float ranges by precision (conservative ranges that avoid overflow)
const FLOAT_RANGES = {
  HALF: { min: -65504, max: 65504 }, // Float16 max
  SINGLE: { min: -3.4e38, max: 3.4e38 }, // Float32 approximate max
  DOUBLE: { min: -1.7e308, max: 1.7e308 } // Float64 approximate max
} as const

export const floatGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.FloatingPointType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const includeSpecial = config.includeSpecialFloats ?? false
      const specialRate = config.specialFloatRate ?? 0.1
      const range = FLOAT_RANGES[type.precision]
      const specialFloats = SPECIAL_FLOATS_BY_PRECISION[type.precision]
      const values: Array<number | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else if (includeSpecial && (yield* Rand.nextBoolWithProbability(specialRate))) {
          // Generate a precision-appropriate special float value
          const idx = yield* Random.nextIntBetween(0, specialFloats.length)
          values.push(specialFloats[idx])
        } else {
          // Generate a normal float in the valid range for this precision
          values.push(yield* Rand.nextFloat(range.min, range.max))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = type.precision === "HALF"
        ? BufferUtils.createFloat16DataBuffer(values)
        : type.precision === "SINGLE"
        ? BufferUtils.createFloat32DataBuffer(values)
        : BufferUtils.createFloat64DataBuffer(values)

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
