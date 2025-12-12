/**
 * Arrow Value Readers
 *
 * This module provides utilities for reading typed values from Arrow buffers.
 *
 * @internal
 */
import type { Predicate } from "effect/Predicate"
import type { DictionaryRegistry } from "./Decoder.ts"
import type { DecodedColumn } from "./RecordBatch.ts"
import type { DictionaryEncoding, IntBitWidth, TimeBitWidth, TimeUnit, UnionMode } from "./Schema.ts"

// =============================================================================
// Constants
// =============================================================================

/**
 * The maximum safe integer value that can be precisely represented in JavaScript.
 * Values larger than this will lose precision when converted from bigint to number.
 */
const MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER)
const MIN_SAFE_INTEGER = BigInt(Number.MIN_SAFE_INTEGER)

// -----------------------------------------------------------------------------
// Byte Sizes
// -----------------------------------------------------------------------------

/** Number of bytes per 16-bit integer */
const BYTES_PER_INT16 = 2
/** Number of bytes per 32-bit integer */
const BYTES_PER_INT32 = 4
/** Number of bytes per 64-bit integer */
const BYTES_PER_INT64 = 8
/** Number of bytes per interval (month-day-nano) value: 4 + 4 + 8 = 16 */
const BYTES_PER_INTERVAL_MONTH_DAY_NANO = 16
/** Number of bits per byte (for validity bitmap calculations) */
const BITS_PER_BYTE = 8

// -----------------------------------------------------------------------------
// Time Constants
// -----------------------------------------------------------------------------

/** Milliseconds per day for date conversions */
const MS_PER_DAY = 86400000

/**
 * Module-level singleton TextDecoder for UTF-8 decoding.
 * Reusing a single instance avoids repeated allocations.
 */
const textDecoder = new TextDecoder()

/**
 * Lookup table for converting time units to milliseconds.
 * Defined at module level to avoid repeated object creation.
 */
const TIME_UNIT_TO_MS: Record<keyof typeof TimeUnit, number> = {
  MICROSECOND: 0.001,
  MILLISECOND: 1,
  NANOSECOND: 0.000001,
  SECOND: 1000
}

/**
 * Lookup table for converting timestamp units to milliseconds.
 * Uses functions because SECOND and MILLISECOND need overflow checking.
 */
const TIMESTAMP_UNIT_TO_MS: Record<keyof typeof TimeUnit, (v: bigint, idx: number) => number> = {
  MICROSECOND: (v) => Number(v / 1000n),
  MILLISECOND: (v, idx) => bigintToNumberSafe(v, `timestamp[${idx}] milliseconds`),
  NANOSECOND: (v) => Number(v / 1000000n),
  SECOND: (v, idx) => bigintToNumberSafe(v, `timestamp[${idx}] seconds`) * 1000
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Safely converts a bigint to a number, throwing an error if precision would be lost.
 *
 * JavaScript's `Number` type can only precisely represent integers up to 2^53 - 1.
 * This function ensures that conversions from bigint to number don't silently
 * lose precision for large values.
 *
 * @throws {RangeError} If the value exceeds Number.MAX_SAFE_INTEGER or is less than Number.MIN_SAFE_INTEGER
 */
const bigintToNumberSafe = (value: bigint, context?: string): number => {
  if (value > MAX_SAFE_INTEGER || value < MIN_SAFE_INTEGER) {
    const contextMsg = context ? ` (${context})` : ""
    throw new RangeError(
      `Value ${value}${contextMsg} exceeds safe integer range for Number conversion. ` +
        `Use BigInt-based APIs for values outside the range [${Number.MIN_SAFE_INTEGER}, ${Number.MAX_SAFE_INTEGER}].`
    )
  }
  return Number(value)
}

// =============================================================================
// Validity
// =============================================================================

/**
 * Creates a predicate function that checks whether a value at a given index is
 * valid (non-null) based on an Arrow validity bitmap buffer.
 *
 * In the Arrow columnar format, null values are tracked using a validity bitmap
 * where each bit corresponds to one value in the column. A bit value of 1 indicates
 * the value is valid (non-null), while 0 indicates the value is null.
 *
 * The bitmap uses least-significant bit (LSB) numbering within each byte, meaning
 * bit 0 of byte 0 corresponds to index 0, bit 1 of byte 0 corresponds to index 1,
 * and so on. After 8 values, we move to the next byte.
 *
 * @param validityBuffer - The validity bitmap buffer from an Arrow column. If this
 *   buffer is empty (length 0), all values are considered valid per the Arrow spec.
 *   The buffer should contain ceil(length / 8) bytes to cover all values.
 *
 * @returns A predicate function that takes an index and returns `true` if the value
 *   at that index is valid (non-null), or `false` if the value is null. For indices
 *   that exceed the buffer's range, returns `false` (treated as null) for safety.
 *
 * @example
 * ```ts
 * // Buffer where first value is valid, second is null, third is valid
 * // Binary: 00000101 = bits 0 and 2 are set
 * const bitmap = new Uint8Array([0b00000101])
 * const isValid = createValidityChecker(bitmap)
 *
 * isValid(0) // true  - bit 0 is set
 * isValid(1) // false - bit 1 is not set
 * isValid(2) // true  - bit 2 is set
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps | Arrow Validity Bitmaps}
 */
export const createValidityChecker = (validityBuffer: Uint8Array): Predicate<number> => {
  if (validityBuffer.length === 0) {
    // No validity buffer means all values are valid
    return () => true
  }

  const bufferLength = validityBuffer.length

  return (index) => {
    const byteIndex = Math.floor(index / BITS_PER_BYTE)
    // Out of bounds access - treat as invalid (null)
    if (byteIndex >= bufferLength) {
      return false
    }
    const bitIndex = index % BITS_PER_BYTE
    return (validityBuffer[byteIndex] & (1 << bitIndex)) !== 0
  }
}

// =============================================================================
// Booleans
// =============================================================================

/**
 * Reads boolean values from an Arrow boolean column's buffers.
 *
 * Arrow stores boolean values in a packed bit format where each bit represents
 * one boolean value. Like the validity bitmap, boolean data uses LSB (least-significant
 * bit) numbering: bit 0 of byte 0 is value 0, bit 7 of byte 0 is value 7, bit 0 of
 * byte 1 is value 8, and so on.
 *
 * The function respects the validity bitmap, returning `null` for values where
 * the corresponding validity bit is 0.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The packed boolean data buffer where each bit represents one
 *   boolean value. Should contain at least ceil(length / 8) bytes.
 * @param length - The number of boolean values to read from the buffers.
 *
 * @returns An array of boolean values (or null for invalid/null entries) with the
 *   specified length. The array is marked readonly to prevent mutation.
 *
 * @example
 * ```ts
 * // Data buffer: 0b00000101 = values [true, false, true, false, false, false, false, false]
 * // Validity buffer: 0b00000111 = first 3 values valid, rest null
 * const validity = new Uint8Array([0b00000111])
 * const data = new Uint8Array([0b00000101])
 * const values = readBoolValues(validity, data, 4)
 * // Result: [true, false, true, null]
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readBoolValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<boolean | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const values = new Array<boolean | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const byteIndex = Math.floor(i / BITS_PER_BYTE)
      const bitIndex = i % BITS_PER_BYTE
      values[i] = (dataBuffer[byteIndex] & (1 << bitIndex)) !== 0
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Integers
// =============================================================================

/**
 * Reads 8-bit integer values from an Arrow Int8/UInt8 column's buffers.
 *
 * Arrow stores fixed-width integers in a contiguous buffer with native byte order.
 * Each 8-bit integer occupies exactly 1 byte, with values stored sequentially
 * starting at byte offset 0.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing packed 8-bit integers. Should contain
 *   at least `length` bytes.
 * @param length - The number of integer values to read.
 * @param signed - If `true`, interprets values as signed integers (Int8, range -128 to 127).
 *   If `false`, interprets as unsigned integers (UInt8, range 0 to 255).
 *
 * @returns An array of number values (or null for invalid entries). All values fit
 *   safely within JavaScript's number type without precision loss.
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readInt8Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  signed: boolean
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? (signed ? view.getInt8(i) : view.getUint8(i)) : null
  }
  return values
}

/**
 * Reads 16-bit integer values from an Arrow Int16/UInt16 column's buffers.
 *
 * Arrow stores 16-bit integers in little-endian byte order. Each value occupies
 * 2 bytes, with values stored contiguously starting at byte offset 0.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing packed 16-bit integers. Should contain
 *   at least `length * 2` bytes.
 * @param length - The number of integer values to read.
 * @param signed - If `true`, interprets values as signed integers (Int16, range -32768 to 32767).
 *   If `false`, interprets as unsigned integers (UInt16, range 0 to 65535).
 *
 * @returns An array of number values (or null for invalid entries). All values fit
 *   safely within JavaScript's number type without precision loss.
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readInt16Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  signed: boolean
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    const offset = i * BYTES_PER_INT16
    values[i] = validityChecker(i) ? (signed ? view.getInt16(offset, true) : view.getUint16(offset, true)) : null
  }
  return values
}

/**
 * Reads 32-bit integer values from an Arrow Int32/UInt32 column's buffers.
 *
 * Arrow stores 32-bit integers in little-endian byte order. Each value occupies
 * 4 bytes, with values stored contiguously starting at byte offset 0.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing packed 32-bit integers. Should contain
 *   at least `length * 4` bytes.
 * @param length - The number of integer values to read.
 * @param signed - If `true`, interprets values as signed integers (Int32, range -2^31 to 2^31-1).
 *   If `false`, interprets as unsigned integers (UInt32, range 0 to 2^32-1). Defaults to `true`.
 *
 * @returns An array of number values (or null for invalid entries). All values fit
 *   safely within JavaScript's number type without precision loss.
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readInt32Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  signed: boolean = true
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    const offset = i * BYTES_PER_INT32
    values[i] = validityChecker(i) ? (signed ? view.getInt32(offset, true) : view.getUint32(offset, true)) : null
  }
  return values
}

/**
 * Reads 64-bit integer values from an Arrow Int64/UInt64 column's buffers.
 *
 * Arrow stores 64-bit integers in little-endian byte order. Each value occupies
 * 8 bytes, with values stored contiguously starting at byte offset 0.
 *
 * **Important**: Unlike smaller integer types, 64-bit integers are returned as
 * `bigint` values because JavaScript's `number` type can only safely represent
 * integers up to 2^53 - 1 (Number.MAX_SAFE_INTEGER). Using `bigint` ensures no
 * precision loss for values in the full Int64/UInt64 range.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing packed 64-bit integers. Should contain
 *   at least `length * 8` bytes.
 * @param length - The number of integer values to read.
 * @param signed - If `true`, interprets values as signed integers (Int64, range -2^63 to 2^63-1).
 *   If `false`, interprets as unsigned integers (UInt64, range 0 to 2^64-1). Defaults to `true`.
 *
 * @returns An array of `bigint` values (or null for invalid entries). The array uses
 *   `bigint` to preserve full 64-bit precision.
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readInt64Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  signed: boolean = true
): ReadonlyArray<bigint | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<bigint | null>(length)
  for (let i = 0; i < length; i++) {
    const offset = i * BYTES_PER_INT64
    values[i] = validityChecker(i) ? (signed ? view.getBigInt64(offset, true) : view.getBigUint64(offset, true)) : null
  }
  return values
}

// =============================================================================
// Floats
// =============================================================================

/**
 * Decodes a 16-bit half-precision floating point value (IEEE 754 binary16)
 * from its bit representation to a JavaScript number.
 *
 * Half-precision floats use:
 * - 1 bit for sign
 * - 5 bits for exponent (biased by 15)
 * - 10 bits for fraction/mantissa
 *
 * @param bits - The 16-bit unsigned integer representing the half-precision float
 * @returns The decoded floating point number
 */
const decodeFloat16 = (bits: number): number => {
  const sign = (bits >> 15) & 1
  const exponent = (bits >> 10) & 0x1f
  const fraction = bits & 0x3ff
  if (exponent === 0) return (sign ? -1 : 1) * Math.pow(2, -14) * (fraction / 1024)
  if (exponent === 31) return fraction === 0 ? (sign ? -Infinity : Infinity) : NaN
  return (sign ? -1 : 1) * Math.pow(2, exponent - 15) * (1 + fraction / 1024)
}

/**
 * Reads 16-bit half-precision floating point values from an Arrow Float16 column's buffers.
 *
 * Arrow stores Float16 values as 2 bytes per value in little-endian byte order.
 * Half-precision floats (IEEE 754 binary16) offer reduced precision but smaller
 * storage compared to single/double precision. They are commonly used in machine
 * learning and graphics applications.
 *
 * The values are decoded from their bit representation to JavaScript `number` values
 * (which are internally double-precision floats).
 *
 * **Precision note**: Float16 has limited precision (about 3-4 significant decimal digits)
 * and a smaller range than Float32/Float64. Values outside the representable range
 * become Infinity or -Infinity.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 16-bit float values. Should contain
 *   at least `length * 2` bytes.
 * @param length - The number of float values to read.
 *
 * @returns An array of number values (or null for invalid entries).
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readFloat16Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? decodeFloat16(view.getUint16(i * BYTES_PER_INT16, true)) : null
  }
  return values
}

/**
 * Reads 32-bit single-precision floating point values from an Arrow Float32 column's buffers.
 *
 * Arrow stores Float32 values as 4 bytes per value in little-endian byte order,
 * following the IEEE 754 binary32 standard. Single-precision floats offer about
 * 6-7 significant decimal digits of precision.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 32-bit float values. Should contain
 *   at least `length * 4` bytes.
 * @param length - The number of float values to read.
 *
 * @returns An array of number values (or null for invalid entries). Note that JavaScript
 *   numbers are internally 64-bit doubles, so Float32 values are promoted to double
 *   precision upon reading.
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readFloat32Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? view.getFloat32(i * BYTES_PER_INT32, true) : null
  }
  return values
}

/**
 * Reads 64-bit double-precision floating point values from an Arrow Float64 column's buffers.
 *
 * Arrow stores Float64 values as 8 bytes per value in little-endian byte order,
 * following the IEEE 754 binary64 standard. Double-precision floats offer about
 * 15-17 significant decimal digits of precision and are the native precision of
 * JavaScript's `number` type.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 64-bit float values. Should contain
 *   at least `length * 8` bytes.
 * @param length - The number of float values to read.
 *
 * @returns An array of number values (or null for invalid entries).
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readFloat64Values = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? view.getFloat64(i * BYTES_PER_INT64, true) : null
  }
  return values
}

// =============================================================================
// Decimals
// =============================================================================

/**
 * Converts a sequence of bytes (little-endian) to a bigint value.
 *
 * Arrow stores decimal values as two's complement integers in little-endian byte
 * order. This function reconstructs the bigint value from those bytes, handling
 * both signed and unsigned interpretations.
 *
 * @param bytes - The byte array in little-endian order
 * @param signed - If true, interprets as signed (two's complement)
 * @returns The bigint representation of the bytes
 */
const bytesToBigInt = (bytes: Uint8Array, signed: boolean): bigint => {
  let result = 0n
  for (let i = bytes.length - 1; i >= 0; i--) {
    result = (result << 8n) | BigInt(bytes[i])
  }
  if (signed && bytes.length > 0 && (bytes[bytes.length - 1] & 0x80) !== 0) {
    result = result - (1n << BigInt(bytes.length * 8))
  }
  return result
}

/**
 * Formats a decimal integer value as a string with the proper decimal point placement.
 *
 * Arrow Decimal types store values as scaled integers. For example, the value 123.45
 * with scale=2 is stored as the integer 12345. This function converts the integer
 * back to a properly formatted decimal string.
 *
 * @param value - The unscaled bigint value representing the decimal
 * @param scale - The number of digits after the decimal point
 * @returns A string representation of the decimal value (e.g., "123.45")
 */
const formatDecimal = (value: bigint, scale: number): string => {
  const isNegative = value < 0n
  const absValue = isNegative ? -value : value
  const str = absValue.toString()
  if (scale === 0) return isNegative ? `-${str}` : str
  const paddedStr = str.padStart(scale + 1, "0")
  const intPart = paddedStr.slice(0, -scale) || "0"
  const fracPart = paddedStr.slice(-scale)
  return `${isNegative ? "-" : ""}${intPart}.${fracPart}`
}

/**
 * Reads decimal values from an Arrow Decimal128 or Decimal256 column's buffers.
 *
 * Arrow represents decimal numbers as fixed-point integers with a specified precision
 * (total number of digits) and scale (digits after the decimal point). The values are
 * stored as little-endian two's complement integers using 128 bits (16 bytes) or
 * 256 bits (32 bytes) depending on the decimal type.
 *
 * Values are returned as strings to preserve exact precision, since JavaScript numbers
 * cannot accurately represent all decimal values (especially those with many significant
 * digits or large magnitudes).
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing the decimal values. Should contain
 *   at least `length * (bitWidth / 8)` bytes.
 * @param length - The number of decimal values to read.
 * @param scale - The number of digits after the decimal point. For example, a scale
 *   of 2 means values like "123.45".
 * @param bitWidth - The bit width of the decimal type (128 or 256 bits).
 *
 * @returns An array of string values representing the decimals (or null for invalid
 *   entries). Strings preserve exact precision without floating-point rounding errors.
 *
 * @example
 * ```ts
 * // Reading Decimal128 with precision=10, scale=2
 * // Value 12345 with scale 2 becomes "123.45"
 * const values = readDecimalValues(validity, data, 1, 2, 128)
 * // Result: ["123.45"]
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readDecimalValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  /* precision: number, */
  scale: number,
  bitWidth: number
): ReadonlyArray<string | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const byteWidth = bitWidth / 8
  const values = new Array<string | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = i * byteWidth
      const bigIntValue = bytesToBigInt(dataBuffer.subarray(start, start + byteWidth), true)
      values[i] = formatDecimal(bigIntValue, scale)
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Strings
// =============================================================================

/**
 * Reads UTF-8 encoded string values from an Arrow Utf8 column's buffers.
 *
 * Arrow's Utf8 type uses a variable-length binary layout consisting of three buffers:
 * 1. **Validity buffer**: Bitmap indicating which values are non-null
 * 2. **Offset buffer**: Array of 32-bit integers where offset[i] gives the starting
 *    byte position of string i, and offset[i+1] - offset[i] gives its length
 * 3. **Data buffer**: Contiguous UTF-8 encoded string data
 *
 * The offset buffer contains `length + 1` offsets (one extra to define the end of
 * the last string). All strings are stored back-to-back in the data buffer.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param offsetBuffer - The offset buffer containing 32-bit integer offsets into the
 *   data buffer. Should contain `(length + 1) * 4` bytes.
 * @param dataBuffer - The data buffer containing UTF-8 encoded string bytes.
 * @param length - The number of string values to read.
 *
 * @returns An array of string values (or null for invalid entries).
 *
 * @example
 * ```ts
 * // Offsets: [0, 5, 10] means string 0 is bytes 0-4, string 1 is bytes 5-9
 * // Data: "HelloWorld" (UTF-8 bytes)
 * const values = readUtf8Values(validity, offsets, data, 2)
 * // Result: ["Hello", "World"]
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout | Arrow Variable-Size Binary Layout}
 */
export const readUtf8Values = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<string | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const values = new Array<string | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = offsetView.getInt32(i * BYTES_PER_INT32, true)
      const end = offsetView.getInt32((i + 1) * BYTES_PER_INT32, true)
      values[i] = textDecoder.decode(dataBuffer.subarray(start, end))
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads UTF-8 encoded string values from an Arrow LargeUtf8 column's buffers.
 *
 * LargeUtf8 is identical to Utf8 except it uses 64-bit offsets instead of 32-bit,
 * allowing for strings that exceed 2GB in total size. The layout is:
 * 1. **Validity buffer**: Bitmap indicating which values are non-null
 * 2. **Offset buffer**: Array of 64-bit integers (as `bigint`) for byte positions
 * 3. **Data buffer**: Contiguous UTF-8 encoded string data
 *
 * **Important**: While Arrow supports 64-bit offsets, this implementation converts
 * them to JavaScript numbers for use with TypedArray methods. Offsets exceeding
 * `Number.MAX_SAFE_INTEGER` (2^53 - 1) will throw a `RangeError` to prevent silent
 * precision loss.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param offsetBuffer - The offset buffer containing 64-bit integer offsets into the
 *   data buffer. Should contain `(length + 1) * 8` bytes.
 * @param dataBuffer - The data buffer containing UTF-8 encoded string bytes.
 * @param length - The number of string values to read.
 *
 * @returns An array of string values (or null for invalid entries).
 *
 * @throws {RangeError} If any offset exceeds `Number.MAX_SAFE_INTEGER`
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout | Arrow Variable-Size Binary Layout}
 */
export const readLargeUtf8Values = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<string | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const values = new Array<string | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = bigintToNumberSafe(offsetView.getBigInt64(i * BYTES_PER_INT64, true), `large-utf8 offset[${i}]`)
      const end = bigintToNumberSafe(
        offsetView.getBigInt64((i + 1) * BYTES_PER_INT64, true),
        `large-utf8 offset[${i + 1}]`
      )
      values[i] = textDecoder.decode(dataBuffer.subarray(start, end))
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Binary
// =============================================================================

/**
 * Reads binary (byte array) values from an Arrow Binary column's buffers.
 *
 * Arrow's Binary type uses a variable-length layout identical to Utf8, but the
 * data is treated as raw bytes rather than UTF-8 encoded text:
 * 1. **Validity buffer**: Bitmap indicating which values are non-null
 * 2. **Offset buffer**: Array of 32-bit integers for byte positions
 * 3. **Data buffer**: Contiguous binary data
 *
 * Unlike string readers that decode to JavaScript strings, binary readers return
 * `Uint8Array` slices. The returned arrays are **copies** of the data (via `slice()`)
 * to ensure they remain valid even if the underlying buffer is modified or freed.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param offsetBuffer - The offset buffer containing 32-bit integer offsets into the
 *   data buffer. Should contain `(length + 1) * 4` bytes.
 * @param dataBuffer - The data buffer containing the binary data.
 * @param length - The number of binary values to read.
 *
 * @returns An array of `Uint8Array` values (or null for invalid entries). Each
 *   `Uint8Array` is an independent copy of the binary data.
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout | Arrow Variable-Size Binary Layout}
 */
export const readBinaryValues = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Uint8Array | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const values = new Array<Uint8Array | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = offsetView.getInt32(i * BYTES_PER_INT32, true)
      const end = offsetView.getInt32((i + 1) * BYTES_PER_INT32, true)
      values[i] = dataBuffer.slice(start, end)
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads binary (byte array) values from an Arrow LargeBinary column's buffers.
 *
 * LargeBinary is identical to Binary except it uses 64-bit offsets instead of 32-bit,
 * allowing for binary data that exceeds 2GB in total size.
 *
 * **Important**: While Arrow supports 64-bit offsets, this implementation converts
 * them to JavaScript numbers for use with TypedArray methods. Offsets exceeding
 * `Number.MAX_SAFE_INTEGER` (2^53 - 1) will throw a `RangeError` to prevent silent
 * precision loss.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param offsetBuffer - The offset buffer containing 64-bit integer offsets into the
 *   data buffer. Should contain `(length + 1) * 8` bytes.
 * @param dataBuffer - The data buffer containing the binary data.
 * @param length - The number of binary values to read.
 *
 * @returns An array of `Uint8Array` values (or null for invalid entries). Each
 *   `Uint8Array` is an independent copy of the binary data.
 *
 * @throws {RangeError} If any offset exceeds `Number.MAX_SAFE_INTEGER`
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout | Arrow Variable-Size Binary Layout}
 */
export const readLargeBinaryValues = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Uint8Array | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const values = new Array<Uint8Array | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = bigintToNumberSafe(offsetView.getBigInt64(i * BYTES_PER_INT64, true), `large-binary offset[${i}]`)
      const end = bigintToNumberSafe(
        offsetView.getBigInt64((i + 1) * BYTES_PER_INT64, true),
        `large-binary offset[${i + 1}]`
      )
      values[i] = dataBuffer.slice(start, end)
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads fixed-size binary values from an Arrow FixedSizeBinary column's buffers.
 *
 * Unlike variable-length Binary, FixedSizeBinary stores values of a fixed byte width.
 * All values occupy exactly `byteWidth` bytes, stored contiguously in the data buffer.
 * This layout is more memory-efficient for fixed-size data like UUIDs (16 bytes),
 * IPv6 addresses (16 bytes), or hashes (32 bytes for SHA-256).
 *
 * The layout consists of just two buffers:
 * 1. **Validity buffer**: Bitmap indicating which values are non-null
 * 2. **Data buffer**: Contiguous fixed-size binary values
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing fixed-size binary values. Should
 *   contain at least `length * byteWidth` bytes.
 * @param length - The number of binary values to read.
 * @param byteWidth - The fixed size in bytes of each value.
 *
 * @returns An array of `Uint8Array` values (or null for invalid entries). Each
 *   `Uint8Array` has exactly `byteWidth` bytes and is an independent copy.
 *
 * @example
 * ```ts
 * // Reading UUIDs (16 bytes each)
 * const uuids = readFixedSizeBinaryValues(validity, data, 3, 16)
 * // Each element is a 16-byte Uint8Array
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Fixed-Size Layout}
 */
export const readFixedSizeBinaryValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  byteWidth: number
): ReadonlyArray<Uint8Array | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const values = new Array<Uint8Array | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = i * byteWidth
      values[i] = dataBuffer.slice(start, start + byteWidth)
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Temporal
// =============================================================================

/**
 * Reads date values from an Arrow Date (DAY unit) column's buffers.
 *
 * Arrow's Date type with DAY unit stores dates as 32-bit signed integers representing
 * the number of days since the Unix epoch (January 1, 1970). This compact representation
 * is useful for storing dates without time components.
 *
 * Values are converted to JavaScript `Date` objects set to midnight UTC on the
 * corresponding date.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 32-bit day counts. Should contain
 *   at least `length * 4` bytes.
 * @param length - The number of date values to read.
 *
 * @returns An array of JavaScript `Date` objects (or null for invalid entries).
 *   Each date is set to midnight UTC on the corresponding day.
 *
 * @example
 * ```ts
 * // Day 0 = 1970-01-01, Day 1 = 1970-01-02, Day 18628 = 2021-01-01
 * const dates = readDateDayValues(validity, data, 1)
 * // Result: [Date("2021-01-01T00:00:00.000Z")]
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readDateDayValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<Date | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? new Date(view.getInt32(i * BYTES_PER_INT32, true) * MS_PER_DAY) : null
  }
  return values
}

/**
 * Reads date values from an Arrow Date (MILLISECOND unit) column's buffers.
 *
 * Arrow's Date type with MILLISECOND unit stores dates as 64-bit signed integers
 * representing milliseconds since the Unix epoch (January 1, 1970 00:00:00 UTC).
 * This allows sub-day precision while still being a date-oriented type.
 *
 * **Important**: The 64-bit millisecond values are converted to JavaScript numbers
 * for use with the `Date` constructor. Values exceeding `Number.MAX_SAFE_INTEGER`
 * will throw a `RangeError`.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 64-bit millisecond counts. Should
 *   contain at least `length * 8` bytes.
 * @param length - The number of date values to read.
 *
 * @returns An array of JavaScript `Date` objects (or null for invalid entries).
 *
 * @throws {RangeError} If any millisecond value exceeds `Number.MAX_SAFE_INTEGER`
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readDateMillisecondValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<Date | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const ms = bigintToNumberSafe(view.getBigInt64(i * BYTES_PER_INT64, true), `date-millisecond[${i}]`)
      values[i] = new Date(ms)
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads timestamp values from an Arrow Timestamp column's buffers.
 *
 * Arrow's Timestamp type stores date/time values as 64-bit signed integers with a
 * configurable time unit (SECOND, MILLISECOND, MICROSECOND, or NANOSECOND). Timestamps
 * may optionally have an associated timezone, though this reader does not adjust for
 * timezone—it returns JavaScript `Date` objects in UTC.
 *
 * The conversion to JavaScript `Date` varies by unit:
 * - **SECOND**: Multiply by 1000 to get milliseconds
 * - **MILLISECOND**: Use directly (with overflow checking)
 * - **MICROSECOND**: Divide by 1000 (loses sub-millisecond precision)
 * - **NANOSECOND**: Divide by 1,000,000 (loses sub-millisecond precision)
 *
 * **Note**: JavaScript Date only supports millisecond precision. Sub-millisecond
 * precision in MICROSECOND and NANOSECOND timestamps is truncated, not rounded.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 64-bit timestamp values. Should
 *   contain at least `length * 8` bytes.
 * @param length - The number of timestamp values to read.
 * @param unit - The time unit of the stored values: "SECOND", "MILLISECOND",
 *   "MICROSECOND", or "NANOSECOND".
 *
 * @returns An array of JavaScript `Date` objects (or null for invalid entries).
 *
 * @throws {RangeError} If SECOND or MILLISECOND values exceed safe integer range
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readTimestampValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  unit: keyof typeof TimeUnit
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<Date | null>(length)
  const converter = TIMESTAMP_UNIT_TO_MS[unit]

  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const rawValue = view.getBigInt64(i * BYTES_PER_INT64, true)
      values[i] = new Date(converter(rawValue, i))
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads time-of-day values from an Arrow Time column's buffers.
 *
 * Arrow's Time type represents a time of day (without date) with configurable
 * precision. It stores values as either 32-bit or 64-bit integers depending on
 * the unit:
 * - **SECOND** (32-bit): Seconds since midnight
 * - **MILLISECOND** (32-bit): Milliseconds since midnight
 * - **MICROSECOND** (64-bit): Microseconds since midnight
 * - **NANOSECOND** (64-bit): Nanoseconds since midnight
 *
 * Values are returned as numbers representing milliseconds since midnight, suitable
 * for time calculations or displaying as time strings.
 *
 * **Note**: When converting from MICROSECOND or NANOSECOND, the result may have
 * fractional milliseconds. For NANOSECOND times, this preserves sub-millisecond
 * precision in the number representation.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing time values. Should contain at
 *   least `length * (bitWidth / 8)` bytes.
 * @param length - The number of time values to read.
 * @param unit - The time unit: "SECOND", "MILLISECOND", "MICROSECOND", or "NANOSECOND".
 * @param bitWidth - The bit width of stored values: 32 for SECOND/MILLISECOND,
 *   64 for MICROSECOND/NANOSECOND.
 *
 * @returns An array of numbers representing milliseconds since midnight (or null
 *   for invalid entries). May include fractional milliseconds for high-precision units.
 *
 * @throws {RangeError} If 64-bit time values exceed `Number.MAX_SAFE_INTEGER`
 *
 * @example
 * ```ts
 * // Time 12:30:45.123 in milliseconds = 45045123
 * const times = readTimeValues(validity, data, 1, "MILLISECOND", 32)
 * // Result: [45045123]
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readTimeValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  unit: keyof typeof TimeUnit,
  bitWidth: TimeBitWidth
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const multiplier = TIME_UNIT_TO_MS[unit]
  const values = new Array<number | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const raw = bitWidth === 32
        ? view.getInt32(i * BYTES_PER_INT32, true)
        : bigintToNumberSafe(view.getBigInt64(i * BYTES_PER_INT64, true), `time[${i}]`)
      values[i] = raw * multiplier
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads duration values from an Arrow Duration column's buffers.
 *
 * Arrow's Duration type represents a length of time (not tied to any calendar or
 * timezone) as a 64-bit signed integer with a configurable time unit. Unlike timestamps,
 * durations are relative time spans suitable for representing intervals like "5 seconds"
 * or "100 nanoseconds".
 *
 * Values are returned as objects containing the raw `bigint` value and the unit string,
 * allowing consumers to perform precise calculations or convert to other representations
 * as needed.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 64-bit duration values. Should
 *   contain at least `length * 8` bytes.
 * @param length - The number of duration values to read.
 * @param unit - The time unit: "SECOND", "MILLISECOND", "MICROSECOND", or "NANOSECOND".
 *
 * @returns An array of objects with `{ value: bigint, unit: string }` (or null for
 *   invalid entries). The `value` is the raw count in the specified unit.
 *
 * @example
 * ```ts
 * const durations = readDurationValues(validity, data, 2, "MILLISECOND")
 * // Result: [{ value: 5000n, unit: "MILLISECOND" }, { value: 10000n, unit: "MILLISECOND" }]
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readDurationValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  unit: keyof typeof TimeUnit
): ReadonlyArray<{ value: bigint; unit: string } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<{ value: bigint; unit: string } | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? { unit, value: view.getBigInt64(i * BYTES_PER_INT64, true) } : null
  }
  return values
}

/**
 * Reads interval values from an Arrow Interval (YEAR_MONTH unit) column's buffers.
 *
 * Arrow's Interval type with YEAR_MONTH unit stores calendar intervals as 32-bit
 * signed integers representing a number of months. This is useful for date arithmetic
 * that should respect calendar boundaries (e.g., "add 1 month" should go from
 * Jan 31 to Feb 28/29, not exactly 30 days).
 *
 * Values are returned as objects with a `months` property, which can be positive
 * (future) or negative (past).
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 32-bit month counts. Should contain
 *   at least `length * 4` bytes.
 * @param length - The number of interval values to read.
 *
 * @returns An array of objects with `{ months: number }` (or null for invalid entries).
 *   For example, 14 months = 1 year and 2 months.
 *
 * @example
 * ```ts
 * const intervals = readIntervalYearMonthValues(validity, data, 2)
 * // Result: [{ months: 12 }, { months: -3 }]  // 1 year, negative 3 months
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readIntervalYearMonthValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<{ months: number } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<{ months: number } | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = validityChecker(i) ? { months: view.getInt32(i * BYTES_PER_INT32, true) } : null
  }
  return values
}

/**
 * Reads interval values from an Arrow Interval (DAY_TIME unit) column's buffers.
 *
 * Arrow's Interval type with DAY_TIME unit stores intervals as pairs of 32-bit
 * signed integers: one for days and one for milliseconds within a day. This provides
 * more precision than YEAR_MONTH while still separating day-level and sub-day components.
 *
 * Each value occupies 8 bytes:
 * - Bytes 0-3: Days (32-bit signed int)
 * - Bytes 4-7: Milliseconds (32-bit signed int)
 *
 * Values are returned as objects with `days` and `milliseconds` properties. Both
 * can be negative for intervals in the past.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 8-byte day/time pairs. Should contain
 *   at least `length * 8` bytes.
 * @param length - The number of interval values to read.
 *
 * @returns An array of objects with `{ days: number, milliseconds: number }` (or null
 *   for invalid entries).
 *
 * @example
 * ```ts
 * const intervals = readIntervalDayTimeValues(validity, data, 1)
 * // Result: [{ days: 5, milliseconds: 43200000 }]  // 5 days and 12 hours
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readIntervalDayTimeValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<{ days: number; milliseconds: number } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<{ days: number; milliseconds: number } | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const offset = i * BYTES_PER_INT64
      values[i] = { days: view.getInt32(offset, true), milliseconds: view.getInt32(offset + BYTES_PER_INT32, true) }
    } else {
      values[i] = null
    }
  }
  return values
}

/**
 * Reads interval values from an Arrow Interval (MONTH_DAY_NANO unit) column's buffers.
 *
 * Arrow's Interval type with MONTH_DAY_NANO unit provides the most comprehensive
 * interval representation, storing months, days, and nanoseconds separately. This
 * allows expressing intervals like "1 month, 2 days, and 500 nanoseconds" without
 * ambiguity about month lengths or day durations.
 *
 * Each value occupies 16 bytes:
 * - Bytes 0-3: Months (32-bit signed int)
 * - Bytes 4-7: Days (32-bit signed int)
 * - Bytes 8-15: Nanoseconds (64-bit signed int)
 *
 * The nanosecond component is returned as a `bigint` to preserve full precision,
 * as it can represent values too large for JavaScript's number type.
 *
 * @param validityBuffer - The validity bitmap indicating which values are non-null.
 *   An empty buffer indicates all values are valid.
 * @param dataBuffer - The data buffer containing 16-byte interval values. Should
 *   contain at least `length * 16` bytes.
 * @param length - The number of interval values to read.
 *
 * @returns An array of objects with `{ months: number, days: number, nanoseconds: bigint }`
 *   (or null for invalid entries).
 *
 * @example
 * ```ts
 * const intervals = readIntervalMonthDayNanoValues(validity, data, 1)
 * // Result: [{ months: 1, days: 15, nanoseconds: 3600000000000n }]  // 1 month, 15 days, 1 hour
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout | Arrow Primitive Layout}
 */
export const readIntervalMonthDayNanoValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<{ months: number; days: number; nanoseconds: bigint } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<{ months: number; days: number; nanoseconds: bigint } | null>(length)
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const offset = i * BYTES_PER_INTERVAL_MONTH_DAY_NANO
      values[i] = {
        days: view.getInt32(offset + BYTES_PER_INT32, true),
        months: view.getInt32(offset, true),
        nanoseconds: view.getBigInt64(offset + BYTES_PER_INT64, true)
      }
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Lists
// =============================================================================

const readListValues = (
  column: DecodedColumn,
  registry?: DictionaryRegistry
): ReadonlyArray<ReadonlyArray<unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const ov = new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
  const childValues = readColumnValues(children[0], registry)
  const values = new Array<Array<unknown> | null>(length)
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      values[i] = childValues.slice(
        ov.getInt32(i * BYTES_PER_INT32, true),
        ov.getInt32((i + 1) * BYTES_PER_INT32, true)
      )
    } else {
      values[i] = null
    }
  }
  return values
}

const readLargeListValues = (
  column: DecodedColumn,
  registry?: DictionaryRegistry
): ReadonlyArray<ReadonlyArray<unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const ov = new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
  const childValues = readColumnValues(children[0], registry)
  const values = new Array<Array<unknown> | null>(length)
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      const start = bigintToNumberSafe(ov.getBigInt64(i * BYTES_PER_INT64, true), `large-list offset[${i}]`)
      const end = bigintToNumberSafe(ov.getBigInt64((i + 1) * BYTES_PER_INT64, true), `large-list offset[${i + 1}]`)
      values[i] = childValues.slice(start, end)
    } else {
      values[i] = null
    }
  }
  return values
}

const readFixedSizeListValues = (
  column: DecodedColumn,
  listSize: number,
  registry?: DictionaryRegistry
): ReadonlyArray<ReadonlyArray<unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const childValues = readColumnValues(children[0], registry)
  const values = new Array<Array<unknown> | null>(length)
  for (let i = 0; i < length; i++) {
    values[i] = vc(i) ? childValues.slice(i * listSize, (i + 1) * listSize) : null
  }
  return values
}

// =============================================================================
// Structs
// =============================================================================

const readStructValues = (
  column: DecodedColumn,
  registry?: DictionaryRegistry
): ReadonlyArray<Record<string, unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const childValuesMap = new Map(children.map((c) => [c.field.name, readColumnValues(c, registry)]))
  const values = new Array<Record<string, unknown> | null>(length)
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      const obj: Record<string, unknown> = {}
      for (const c of children) obj[c.field.name] = childValuesMap.get(c.field.name)![i]
      values[i] = obj
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Maps
// =============================================================================

const readMapValues = (
  column: DecodedColumn,
  registry?: DictionaryRegistry
): ReadonlyArray<
  ReadonlyArray<{
    readonly key: unknown
    readonly value: unknown
  }> | null
> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const ov = new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
  const entriesValues = readColumnValues(children[0], registry) as Array<Record<string, unknown> | null>
  const values = new Array<Array<{ key: unknown; value: unknown }> | null>(length)
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      const start = ov.getInt32(i * BYTES_PER_INT32, true)
      const end = ov.getInt32((i + 1) * BYTES_PER_INT32, true)
      const entries = new Array<{ key: unknown; value: unknown }>(end - start)
      let idx = 0
      for (let j = start; j < end; j++) {
        const e = entriesValues[j]
        if (e) entries[idx++] = { key: e.key, value: e.value }
      }
      values[i] = entries.length === idx ? entries : entries.slice(0, idx)
    } else {
      values[i] = null
    }
  }
  return values
}

// =============================================================================
// Unions
// =============================================================================

const readUnionValues = (
  column: DecodedColumn,
  mode: keyof typeof UnionMode,
  typeIds: ReadonlyArray<number>,
  registry?: DictionaryRegistry
): ReadonlyArray<unknown> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const typeIdBuffer = buffers[0]
  const offsetView = mode === "DENSE"
    ? new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
    : null
  const childValuesArrays = children.map((c) => readColumnValues(c, registry))
  const typeIdToChild = new Map(typeIds.map((id, idx) => [id, idx]))
  const values = new Array<unknown>(length)
  for (let i = 0; i < length; i++) {
    const childIdx = typeIdToChild.get(typeIdBuffer[i])
    if (childIdx === undefined) {
      values[i] = null
    } else if (mode === "DENSE" && offsetView) {
      values[i] = childValuesArrays[childIdx][offsetView.getInt32(i * BYTES_PER_INT32, true)]
    } else {
      values[i] = childValuesArrays[childIdx][i]
    }
  }
  return values
}

// =============================================================================
// Dictionary Encoding
// =============================================================================

/**
 * Read dictionary indices from a dictionary-encoded column.
 * Dictionary-encoded columns store integer indices that reference into a dictionary.
 */
const readDictionaryIndices = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  indexType: { bitWidth: IntBitWidth; isSigned: boolean }
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values = new Array<number | null>(length)

  for (let i = 0; i < length; i++) {
    if (!validityChecker(i)) {
      values[i] = null
      continue
    }

    let index: number
    switch (indexType.bitWidth) {
      case 8:
        index = indexType.isSigned ? view.getInt8(i) : view.getUint8(i)
        break
      case 16:
        index = indexType.isSigned
          ? view.getInt16(i * BYTES_PER_INT16, true)
          : view.getUint16(i * BYTES_PER_INT16, true)
        break
      case 32:
        index = indexType.isSigned
          ? view.getInt32(i * BYTES_PER_INT32, true)
          : view.getUint32(i * BYTES_PER_INT32, true)
        break
      case 64: {
        // For 64-bit indices, we need to convert to number (may lose precision for very large indices)
        const bigIndex = indexType.isSigned
          ? view.getBigInt64(i * BYTES_PER_INT64, true)
          : view.getBigUint64(i * BYTES_PER_INT64, true)
        index = bigintToNumberSafe(bigIndex, `dictionary index[${i}]`)
        break
      }
    }
    values[i] = index
  }

  return values
}

/**
 * Read values from a dictionary-encoded column by looking up indices in the dictionary.
 */
const readDictionaryEncodedValues = (
  column: DecodedColumn,
  dictionaryEncoding: DictionaryEncoding,
  registry: DictionaryRegistry
): ReadonlyArray<unknown> => {
  const { buffers, node } = column
  const length = Number(node.length)

  // Get the dictionary from the registry
  const dictionary = registry.get(dictionaryEncoding.id)
  if (!dictionary) {
    throw new Error(
      `Dictionary with ID ${dictionaryEncoding.id} not found. ` +
        `Make sure dictionary batches are processed before record batches that reference them.`
    )
  }

  // Read the indices
  const indices = readDictionaryIndices(
    buffers[0],
    buffers[1],
    length,
    dictionaryEncoding.indexType
  )

  // Look up values in the dictionary
  const dictValues = dictionary.values
  const values = new Array<unknown>(length)
  for (let i = 0; i < length; i++) {
    const index = indices[i]
    values[i] = index === null ? null : dictValues[index]
  }

  return values
}

// =============================================================================
// Columns
// =============================================================================

/**
 * Reads and decodes values from a decoded Arrow column based on its data type.
 *
 * This is the primary entry point for extracting typed values from Arrow columnar data.
 * It dispatches to the appropriate specialized reader function based on the column's
 * Arrow data type, handling all supported Arrow types including:
 *
 * **Primitive types**: Null, Bool, Int8/16/32/64, UInt8/16/32/64, Float16/32/64, Decimal128/256
 *
 * **String/Binary types**: Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary
 *
 * **Temporal types**: Date (day/millisecond), Time (second/milli/micro/nano),
 *   Timestamp (second/milli/micro/nano), Duration, Interval (year-month/day-time/month-day-nano)
 *
 * **Nested types**: List, LargeList, FixedSizeList, Struct, Map, Union (sparse/dense)
 *
 * **Dictionary encoding**: Automatically resolves dictionary indices to actual values
 *   when a `DictionaryRegistry` is provided.
 *
 * The function handles validity bitmaps automatically, returning `null` for invalid
 * (null) values in the column.
 *
 * @param column - The decoded column containing field metadata, node information,
 *   and extracted buffers. Obtained from `decodeRecordBatch`.
 * @param registry - Optional dictionary registry for resolving dictionary-encoded
 *   columns. **Required** if the column or any nested child column uses dictionary
 *   encoding; an error will be thrown if dictionary encoding is detected but no
 *   registry is provided.
 *
 * @returns An array of decoded values corresponding to the column's data type.
 *   Returns `ReadonlyArray<unknown>` to accommodate all possible Arrow types.
 *   The actual runtime types depend on the column's Arrow type:
 *   - Primitives: `number | null`, `bigint | null`, `boolean | null`, `string | null`
 *   - Temporal: `Date | null`, `number | null`, `{ value: bigint, unit: string } | null`
 *   - Intervals: Objects with months/days/milliseconds/nanoseconds properties
 *   - Nested: Arrays and objects containing recursively decoded values
 *
 * @throws {Error} If a dictionary-encoded column is encountered without a registry
 * @throws {Error} If a referenced dictionary ID is not found in the registry
 * @throws {RangeError} If 64-bit offsets or values exceed `Number.MAX_SAFE_INTEGER`
 *
 * @example
 * ```ts
 * // Basic usage with a decoded record batch
 * const decodedBatch = decodeRecordBatch(recordBatch, body, schema)
 * const nameColumn = decodedBatch.columns[0]
 * const names = readColumnValues(nameColumn)  // string[] | null[]
 *
 * // With dictionary encoding
 * const registry = new DictionaryRegistry()
 * decodeDictionaryBatch(dictBatch, body, schema, registry, readColumnValues)
 * const categoryColumn = decodedBatch.columns[1]
 * const categories = readColumnValues(categoryColumn, registry)
 * ```
 *
 * @see {@link https://arrow.apache.org/docs/format/Columnar.html | Arrow Columnar Format}
 */
export const readColumnValues = (
  column: DecodedColumn,
  registry?: DictionaryRegistry
): ReadonlyArray<unknown> => {
  const { buffers, field, node } = column
  const length = Number(node.length)
  const type = field.type

  // Handle dictionary-encoded columns
  if (field.dictionaryEncoding) {
    if (!registry) {
      throw new Error(
        `Column "${field.name}" is dictionary-encoded but no dictionary registry was provided. ` +
          `Pass a DictionaryRegistry to readColumnValues for dictionary-encoded columns.`
      )
    }
    return readDictionaryEncodedValues(column, field.dictionaryEncoding, registry)
  }

  switch (type.typeId) {
    case "null": {
      return new Array(length).fill(null)
    }
    case "bool": {
      return readBoolValues(buffers[0], buffers[1], length)
    }
    case "int": {
      switch (type.bitWidth) {
        case 8: {
          return readInt8Values(buffers[0], buffers[1], length, type.isSigned)
        }
        case 16: {
          return readInt16Values(buffers[0], buffers[1], length, type.isSigned)
        }
        case 32: {
          return readInt32Values(buffers[0], buffers[1], length, type.isSigned)
        }
        case 64: {
          return readInt64Values(buffers[0], buffers[1], length, type.isSigned)
        }
      }
    }
    case "float": {
      switch (type.precision) {
        case "HALF": {
          return readFloat16Values(buffers[0], buffers[1], length)
        }
        case "SINGLE": {
          return readFloat32Values(buffers[0], buffers[1], length)
        }
        case "DOUBLE": {
          return readFloat64Values(buffers[0], buffers[1], length)
        }
      }
    }
    case "decimal": {
      return readDecimalValues(buffers[0], buffers[1], length, /* type.precision, */ type.scale, type.bitWidth)
    }
    case "utf8": {
      return readUtf8Values(buffers[0], buffers[1], buffers[2], length)
    }
    case "large-utf8": {
      return readLargeUtf8Values(buffers[0], buffers[1], buffers[2], length)
    }
    case "binary": {
      return readBinaryValues(buffers[0], buffers[1], buffers[2], length)
    }
    case "large-binary": {
      return readLargeBinaryValues(buffers[0], buffers[1], buffers[2], length)
    }
    case "fixed-size-binary": {
      return readFixedSizeBinaryValues(buffers[0], buffers[1], length, type.byteWidth)
    }
    case "date": {
      return type.unit === "DAY"
        ? readDateDayValues(buffers[0], buffers[1], length)
        : readDateMillisecondValues(buffers[0], buffers[1], length)
    }
    case "time": {
      return readTimeValues(buffers[0], buffers[1], length, type.unit, type.bitWidth)
    }
    case "timestamp": {
      return readTimestampValues(buffers[0], buffers[1], length, type.unit)
    }
    case "duration": {
      return readDurationValues(buffers[0], buffers[1], length, type.unit)
    }
    case "interval": {
      switch (type.unit) {
        case "YEAR_MONTH": {
          return readIntervalYearMonthValues(buffers[0], buffers[1], length)
        }
        case "DAY_TIME": {
          return readIntervalDayTimeValues(buffers[0], buffers[1], length)
        }
        case "MONTH_DAY_NANO": {
          return readIntervalMonthDayNanoValues(buffers[0], buffers[1], length)
        }
      }
    }
    case "list": {
      return readListValues(column, registry)
    }
    case "large-list": {
      return readLargeListValues(column, registry)
    }
    case "fixed-size-list": {
      return readFixedSizeListValues(column, type.listSize, registry)
    }
    case "struct": {
      return readStructValues(column, registry)
    }
    case "map": {
      return readMapValues(column, registry)
    }
    case "union": {
      return readUnionValues(column, type.mode, type.typeIds, registry)
    }
  }
}
