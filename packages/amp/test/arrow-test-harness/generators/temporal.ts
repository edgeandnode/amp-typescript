/**
 * Temporal Type Generators
 * @internal
 */
import type * as Schema from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Effect from "effect/Effect"
import * as Random from "effect/Random"
import * as BufferUtils from "../BufferUtils.ts"
import * as Rand from "../RandomUtils.ts"
import type * as Types from "../Types.ts"

// =============================================================================
// Date Generator
// =============================================================================

export const dateGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.DateType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      if (type.unit === "DAY") {
        const values: Array<number | null> = []
        for (let i = 0; i < numRows; i++) {
          const isNull = yield* Rand.nextBoolWithProbability(nullRate)
          if (isNull) {
            values.push(null)
          } else {
            values.push(yield* Random.nextIntBetween(0, 47482))
          }
        }
        const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
        const data = BufferUtils.createInt32DataBuffer(values, true)
        return {
          validity: bitmap,
          offsets: null,
          data,
          children: [],
          fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
          values
        }
      }

      const values: Array<bigint | null> = []
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextBigInt(0n, 4102444800000n))
        }
      }
      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createInt64DataBuffer(values, true)
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
// Time Generator
// =============================================================================

export const timeGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.TimeType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      if (type.bitWidth === 32) {
        const maxValue = type.unit === "SECOND" ? 86400 : 86400000
        const values: Array<number | null> = []
        for (let i = 0; i < numRows; i++) {
          const isNull = yield* Rand.nextBoolWithProbability(nullRate)
          if (isNull) {
            values.push(null)
          } else {
            values.push(yield* Random.nextIntBetween(0, maxValue - 1))
          }
        }
        const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
        const data = BufferUtils.createInt32DataBuffer(values, true)
        return {
          validity: bitmap,
          offsets: null,
          data,
          children: [],
          fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
          values
        }
      }

      const maxValue = type.unit === "MICROSECOND" ? 86400000000n : 86400000000000n
      const values: Array<bigint | null> = []
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          values.push(yield* Rand.nextBigInt(0n, maxValue - 1n))
        }
      }
      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createInt64DataBuffer(values, true)
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
// Timestamp Generator
// =============================================================================

export const timestampGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.TimestampType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      const multipliers: Record<string, bigint> = {
        SECOND: 1n,
        MILLISECOND: 1000n,
        MICROSECOND: 1000000n,
        NANOSECOND: 1000000000n
      }
      const mult = multipliers[type.unit]
      const maxSeconds = 4102444800n

      const values: Array<bigint | null> = []
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          const seconds = yield* Rand.nextBigInt(0n, maxSeconds)
          values.push(seconds * mult)
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createInt64DataBuffer(values, true)
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
// Duration Generator
// =============================================================================

export const durationGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.DurationType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      const values: Array<bigint | null> = []
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          const maxValue = {
            SECOND: 31536000n,
            MILLISECOND: 31536000000n,
            MICROSECOND: 31536000000000n,
            NANOSECOND: 31536000000000000n
          }[type.unit]!
          values.push(yield* Rand.nextBigInt(0n, maxValue))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createInt64DataBuffer(values, true)
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
// Interval Generator
// =============================================================================

export const intervalGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.IntervalType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      if (type.unit === "YEAR_MONTH") {
        const values: Array<number | null> = []
        for (let i = 0; i < numRows; i++) {
          const isNull = yield* Rand.nextBoolWithProbability(nullRate)
          if (isNull) {
            values.push(null)
          } else {
            values.push(yield* Random.nextIntBetween(-1200, 1200))
          }
        }
        const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
        const data = BufferUtils.createInt32DataBuffer(values, true)
        return {
          validity: bitmap,
          offsets: null,
          data,
          children: [],
          fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
          values
        }
      }

      if (type.unit === "DAY_TIME") {
        const values: Array<{ days: number; milliseconds: number } | null> = []
        const buffer = new Uint8Array(BufferUtils.align8(numRows * 8))
        const view = new DataView(buffer.buffer)

        for (let i = 0; i < numRows; i++) {
          const isNull = yield* Rand.nextBoolWithProbability(nullRate)
          if (isNull) {
            values.push(null)
          } else {
            const days = yield* Random.nextIntBetween(-3650, 3650)
            const milliseconds = yield* Random.nextIntBetween(0, 86400000)
            view.setInt32(i * 8, days, true)
            view.setInt32(i * 8 + 4, milliseconds, true)
            values.push({ days, milliseconds })
          }
        }

        const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
        return {
          validity: bitmap,
          offsets: null,
          data: buffer,
          children: [],
          fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
          values
        }
      }

      // MONTH_DAY_NANO
      const values: Array<{ months: number; days: number; nanoseconds: bigint } | null> = []
      const buffer = new Uint8Array(BufferUtils.align8(numRows * 16))
      const view = new DataView(buffer.buffer)

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          const months = yield* Random.nextIntBetween(-1200, 1200)
          const days = yield* Random.nextIntBetween(-3650, 3650)
          const nanoseconds = yield* Rand.nextBigInt(0n, 86400000000000n)
          view.setInt32(i * 16, months, true)
          view.setInt32(i * 16 + 4, days, true)
          view.setBigInt64(i * 16 + 8, nanoseconds, true)
          values.push({ months, days, nanoseconds })
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      return {
        validity: bitmap,
        offsets: null,
        data: buffer,
        children: [],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}
