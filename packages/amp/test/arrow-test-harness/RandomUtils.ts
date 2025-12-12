/**
 * Random Utilities for Arrow Test Harness
 *
 * Convenient helpers wrapping Effect's Random service for generating
 * Arrow-specific test data types.
 *
 * @internal
 */
import * as Effect from "effect/Effect"
import * as Random from "effect/Random"

// =============================================================================
// Basic Random Generators
// =============================================================================

/**
 * Generate a random bigint in the range [min, max] inclusive.
 */
export const nextBigInt = (min: bigint, max: bigint): Effect.Effect<bigint, never, Random.Random> =>
  Effect.gen(function*() {
    const range = max - min + 1n
    // For ranges that fit in a safe integer, use simple approach
    if (range <= BigInt(Number.MAX_SAFE_INTEGER)) {
      const randomValue = yield* Random.nextIntBetween(0, Number(range) - 1)
      return min + BigInt(randomValue)
    }
    // For larger ranges, combine multiple random values
    const high = yield* Random.nextIntBetween(0, 0x7FFFFFFF)
    const low = yield* Random.nextIntBetween(0, 0xFFFFFFFF)
    const combined = ((BigInt(high) << 32n) | BigInt(low >>> 0)) % range
    return min + combined
  })

/**
 * Generate a random float in the range [min, max].
 */
export const nextFloat = (min: number, max: number): Effect.Effect<number, never, Random.Random> =>
  Effect.map(Random.next, (n) => n * (max - min) + min)

/**
 * Returns true with the given probability.
 */
export const nextBoolWithProbability = (probability: number): Effect.Effect<boolean, never, Random.Random> =>
  Effect.map(Random.next, (n) => n < probability)

// =============================================================================
// String and Bytes Generators
// =============================================================================

const alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

/**
 * Generate a random alphanumeric string of the given length.
 */
export const nextString = (length: number): Effect.Effect<string, never, Random.Random> =>
  Effect.gen(function*() {
    let result = ""
    for (let i = 0; i < length; i++) {
      const idx = yield* Random.nextIntBetween(0, alphanumericChars.length - 1)
      result += alphanumericChars[idx]
    }
    return result
  })

/**
 * Generate a random string with length between min and max.
 */
export const nextStringBetween = (minLength: number, maxLength: number): Effect.Effect<string, never, Random.Random> =>
  Effect.gen(function*() {
    const length = yield* Random.nextIntBetween(minLength, maxLength)
    return yield* nextString(length)
  })

/**
 * Generate random bytes of the given length.
 */
export const nextBytes = (length: number): Effect.Effect<Uint8Array, never, Random.Random> =>
  Effect.gen(function*() {
    const bytes = new Uint8Array(length)
    for (let i = 0; i < length; i++) {
      bytes[i] = yield* Random.nextIntBetween(0, 255)
    }
    return bytes
  })

/**
 * Generate random bytes with length between min and max.
 */
export const nextBytesBetween = (
  minLength: number,
  maxLength: number
): Effect.Effect<Uint8Array, never, Random.Random> =>
  Effect.gen(function*() {
    const length = yield* Random.nextIntBetween(minLength, maxLength)
    return yield* nextBytes(length)
  })

// =============================================================================
// Date/Time Generators
// =============================================================================

/**
 * Generate a random Date within a reasonable range (1970-2100).
 */
export const nextDate = (): Effect.Effect<Date, never, Random.Random> =>
  Effect.gen(function*() {
    // Random timestamp between 1970-01-01 and 2100-01-01
    const minMs = 0
    const maxMs = 4102444800000 // 2100-01-01
    const timestamp = yield* Random.nextIntBetween(minMs, maxMs)
    return new Date(timestamp)
  })

/**
 * Generate a random timestamp in milliseconds since epoch.
 */
export const nextTimestampMs = (): Effect.Effect<number, never, Random.Random> =>
  Random.nextIntBetween(0, 4102444800000)

/**
 * Generate a random time of day in milliseconds (0 to 86399999).
 */
export const nextTimeOfDayMs = (): Effect.Effect<number, never, Random.Random> => Random.nextIntBetween(0, 86399999)

// =============================================================================
// Array Generators
// =============================================================================

/**
 * Generate an array of random values.
 */
export const nextArray = <A>(
  length: number,
  generator: Effect.Effect<A, never, Random.Random>
): Effect.Effect<Array<A>, never, Random.Random> =>
  Effect.gen(function*() {
    const result: Array<A> = []
    for (let i = 0; i < length; i++) {
      result.push(yield* generator)
    }
    return result
  })

/**
 * Generate an array of nullable values based on null rate.
 */
export const nextNullableArray = <A>(
  length: number,
  generator: Effect.Effect<A, never, Random.Random>,
  nullRate: number
): Effect.Effect<Array<A | null>, never, Random.Random> =>
  Effect.gen(function*() {
    const result: Array<A | null> = []
    for (let i = 0; i < length; i++) {
      const isNull = yield* nextBoolWithProbability(nullRate)
      if (isNull) {
        result.push(null)
      } else {
        result.push(yield* generator)
      }
    }
    return result
  })

// =============================================================================
// Type-Specific Generators
// =============================================================================

/**
 * Generate a random signed 8-bit integer.
 */
export const nextInt8 = (): Effect.Effect<number, never, Random.Random> => Random.nextIntBetween(-128, 127)

/**
 * Generate a random unsigned 8-bit integer.
 */
export const nextUint8 = (): Effect.Effect<number, never, Random.Random> => Random.nextIntBetween(0, 255)

/**
 * Generate a random signed 16-bit integer.
 */
export const nextInt16 = (): Effect.Effect<number, never, Random.Random> => Random.nextIntBetween(-32768, 32767)

/**
 * Generate a random unsigned 16-bit integer.
 */
export const nextUint16 = (): Effect.Effect<number, never, Random.Random> => Random.nextIntBetween(0, 65535)

/**
 * Generate a random signed 32-bit integer.
 */
export const nextInt32 = (): Effect.Effect<number, never, Random.Random> =>
  Random.nextIntBetween(-2147483648, 2147483647)

/**
 * Generate a random unsigned 32-bit integer.
 */
export const nextUint32 = (): Effect.Effect<number, never, Random.Random> => Random.nextIntBetween(0, 4294967295)

/**
 * Generate a random signed 64-bit integer.
 */
export const nextInt64 = (): Effect.Effect<bigint, never, Random.Random> =>
  nextBigInt(-9223372036854775808n, 9223372036854775807n)

/**
 * Generate a random unsigned 64-bit integer.
 */
export const nextUint64 = (): Effect.Effect<bigint, never, Random.Random> => nextBigInt(0n, 18446744073709551615n)

/**
 * Generate a random 32-bit float.
 */
export const nextFloat32 = (): Effect.Effect<number, never, Random.Random> => nextFloat(-3.4028235e38, 3.4028235e38)

/**
 * Generate a random 64-bit float.
 */
export const nextFloat64 = (): Effect.Effect<number, never, Random.Random> => nextFloat(-1e100, 1e100)
