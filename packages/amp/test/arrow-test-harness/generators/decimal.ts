/**
 * Decimal Type Generator
 * @internal
 */
import type * as Schema from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Effect from "effect/Effect"
import * as BufferUtils from "../BufferUtils.ts"
import * as Rand from "../RandomUtils.ts"
import type * as Types from "../Types.ts"

export const decimalGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const type = field.type as Schema.DecimalType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      // Calculate max value based on precision
      // e.g., precision=5 means max is 99999
      const maxUnscaled = 10n ** BigInt(type.precision) - 1n
      const values: Array<bigint | null> = []

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
        } else {
          // Generate a random value within the precision bounds
          values.push(yield* Rand.nextBigInt(-maxUnscaled, maxUnscaled))
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const data = BufferUtils.createDecimalDataBuffer(values, type.bitWidth as 128 | 256)

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
