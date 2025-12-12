/**
 * Buffer Utilities for Arrow Test Harness
 * @internal
 */

// =============================================================================
// Alignment
// =============================================================================

export const ARROW_ALIGNMENT = 8

export const align8 = (size: number): number => {
  const remainder = size % ARROW_ALIGNMENT
  return remainder === 0 ? size : size + (ARROW_ALIGNMENT - remainder)
}

export const padToAlignment = (buffer: Uint8Array): Uint8Array => {
  const alignedSize = align8(buffer.length)
  if (alignedSize === buffer.length) {
    return buffer
  }
  const padded = new Uint8Array(alignedSize)
  padded.set(buffer)
  return padded
}

// =============================================================================
// Validity Bitmaps
// =============================================================================

export const createValidityBitmap = <T>(
  values: ReadonlyArray<T | null>
): { bitmap: Uint8Array; nullCount: number } => {
  let nullCount = 0
  for (const value of values) {
    if (value === null) nullCount++
  }

  // If no nulls, return empty buffer (Arrow optimization)
  if (nullCount === 0) {
    return { bitmap: new Uint8Array(0), nullCount: 0 }
  }

  const numBytes = Math.ceil(values.length / 8)
  const bitmap = new Uint8Array(align8(numBytes))

  for (let i = 0; i < values.length; i++) {
    if (values[i] !== null) {
      const byteIndex = Math.floor(i / 8)
      const bitIndex = i % 8
      bitmap[byteIndex] |= 1 << bitIndex
    }
  }

  return { bitmap, nullCount }
}

export const createValidityBitmapFromFlags = (validity: ReadonlyArray<boolean>): Uint8Array => {
  const numBytes = Math.ceil(validity.length / 8)
  const bitmap = new Uint8Array(align8(numBytes))

  for (let i = 0; i < validity.length; i++) {
    if (validity[i]) {
      const byteIndex = Math.floor(i / 8)
      const bitIndex = i % 8
      bitmap[byteIndex] |= 1 << bitIndex
    }
  }

  return bitmap
}

// =============================================================================
// Offset Buffers
// =============================================================================

export const createInt32OffsetBuffer = (offsets: ReadonlyArray<number>): Uint8Array => {
  const buffer = new Uint8Array(align8(offsets.length * 4))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < offsets.length; i++) {
    view.setInt32(i * 4, offsets[i], true)
  }

  return buffer
}

export const createInt64OffsetBuffer = (offsets: ReadonlyArray<bigint>): Uint8Array => {
  const buffer = new Uint8Array(align8(offsets.length * 8))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < offsets.length; i++) {
    view.setBigInt64(i * 8, offsets[i], true)
  }

  return buffer
}

// =============================================================================
// Variable-Length Data Buffers
// =============================================================================

export const createVariableLengthBuffers = (
  values: ReadonlyArray<string | Uint8Array | null>,
  large: boolean
): { offsets: Uint8Array; data: Uint8Array } => {
  const encoder = new TextEncoder()
  const encodedValues: Array<Uint8Array> = []
  const offsets: Array<number | bigint> = large ? [0n] : [0]
  let currentOffset = large ? 0n : 0

  for (const value of values) {
    if (value === null) {
      offsets.push(currentOffset)
    } else {
      const bytes = typeof value === "string" ? encoder.encode(value) : value
      encodedValues.push(bytes)
      if (large) {
        currentOffset = (currentOffset as bigint) + BigInt(bytes.length)
      } else {
        currentOffset = (currentOffset as number) + bytes.length
      }
      offsets.push(currentOffset)
    }
  }

  const totalLength = large ? Number(currentOffset as bigint) : (currentOffset as number)
  const data = new Uint8Array(align8(totalLength))
  let pos = 0
  for (const bytes of encodedValues) {
    data.set(bytes, pos)
    pos += bytes.length
  }

  const offsetBuffer = large
    ? createInt64OffsetBuffer(offsets as Array<bigint>)
    : createInt32OffsetBuffer(offsets as Array<number>)

  return { offsets: offsetBuffer, data }
}

// =============================================================================
// Fixed-Width Data Buffers
// =============================================================================

export const createBoolDataBuffer = (values: ReadonlyArray<boolean | null>): Uint8Array => {
  const numBytes = Math.ceil(values.length / 8)
  const buffer = new Uint8Array(align8(numBytes))

  for (let i = 0; i < values.length; i++) {
    if (values[i] === true) {
      const byteIndex = Math.floor(i / 8)
      const bitIndex = i % 8
      buffer[byteIndex] |= 1 << bitIndex
    }
  }

  return buffer
}

export const createInt8DataBuffer = (values: ReadonlyArray<number | null>, signed: boolean): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    const value = values[i] ?? 0
    if (signed) {
      view.setInt8(i, value)
    } else {
      view.setUint8(i, value)
    }
  }

  return buffer
}

export const createInt16DataBuffer = (values: ReadonlyArray<number | null>, signed: boolean): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * 2))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    const value = values[i] ?? 0
    if (signed) {
      view.setInt16(i * 2, value, true)
    } else {
      view.setUint16(i * 2, value, true)
    }
  }

  return buffer
}

export const createInt32DataBuffer = (values: ReadonlyArray<number | null>, signed: boolean): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * 4))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    const value = values[i] ?? 0
    if (signed) {
      view.setInt32(i * 4, value, true)
    } else {
      view.setUint32(i * 4, value, true)
    }
  }

  return buffer
}

export const createInt64DataBuffer = (values: ReadonlyArray<bigint | null>, signed: boolean): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * 8))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    const value = values[i] ?? 0n
    if (signed) {
      view.setBigInt64(i * 8, value, true)
    } else {
      view.setBigUint64(i * 8, value, true)
    }
  }

  return buffer
}

export const createFloat32DataBuffer = (values: ReadonlyArray<number | null>): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * 4))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    view.setFloat32(i * 4, values[i] ?? 0, true)
  }

  return buffer
}

export const createFloat64DataBuffer = (values: ReadonlyArray<number | null>): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * 8))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    view.setFloat64(i * 8, values[i] ?? 0, true)
  }

  return buffer
}

const encodeFloat16 = (value: number): number => {
  if (value === 0) return 0
  if (!Number.isFinite(value)) {
    if (Number.isNaN(value)) return 0x7E00
    return value > 0 ? 0x7C00 : 0xFC00
  }

  const sign = value < 0 ? 1 : 0
  const absValue = Math.abs(value)

  if (absValue < 6.103515625e-5) {
    const mantissa = Math.round(absValue / 5.960464477539063e-8)
    return (sign << 15) | mantissa
  }

  let exponent = Math.floor(Math.log2(absValue))
  let mantissa = absValue / Math.pow(2, exponent) - 1

  exponent += 15
  if (exponent >= 31) return (sign << 15) | 0x7C00
  if (exponent <= 0) return (sign << 15)

  mantissa = Math.round(mantissa * 1024)
  return (sign << 15) | (exponent << 10) | (mantissa & 0x3FF)
}

export const createFloat16DataBuffer = (values: ReadonlyArray<number | null>): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * 2))
  const view = new DataView(buffer.buffer)

  for (let i = 0; i < values.length; i++) {
    const encoded = encodeFloat16(values[i] ?? 0)
    view.setUint16(i * 2, encoded, true)
  }

  return buffer
}

export const createFixedSizeBinaryDataBuffer = (
  values: ReadonlyArray<Uint8Array | null>,
  byteWidth: number
): Uint8Array => {
  const buffer = new Uint8Array(align8(values.length * byteWidth))

  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (value !== null) {
      buffer.set(value.subarray(0, byteWidth), i * byteWidth)
    }
  }

  return buffer
}

// =============================================================================
// Decimal Data Buffers
// =============================================================================

const bigIntToBytes = (value: bigint, byteWidth: number): Uint8Array => {
  const bytes = new Uint8Array(byteWidth)
  let v = value < 0n ? -value : value
  const isNegative = value < 0n

  for (let i = 0; i < byteWidth; i++) {
    bytes[i] = Number(v & 0xFFn)
    v >>= 8n
  }

  if (isNegative) {
    let carry = 1
    for (let i = 0; i < byteWidth; i++) {
      const inverted = (~bytes[i] & 0xFF) + carry
      bytes[i] = inverted & 0xFF
      carry = inverted >> 8
    }
  }

  return bytes
}

export const createDecimalDataBuffer = (
  values: ReadonlyArray<bigint | null>,
  bitWidth: 128 | 256
): Uint8Array => {
  const byteWidth = bitWidth / 8
  const buffer = new Uint8Array(align8(values.length * byteWidth))

  for (let i = 0; i < values.length; i++) {
    const value = values[i] ?? 0n
    const bytes = bigIntToBytes(value, byteWidth)
    buffer.set(bytes, i * byteWidth)
  }

  return buffer
}

// =============================================================================
// Union Type Buffers
// =============================================================================

export const createTypeIdBuffer = (typeIds: ReadonlyArray<number>): Uint8Array => {
  const buffer = new Uint8Array(align8(typeIds.length))

  for (let i = 0; i < typeIds.length; i++) {
    buffer[i] = typeIds[i]
  }

  return buffer
}
