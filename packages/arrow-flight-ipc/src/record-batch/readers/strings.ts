import { createValidityChecker } from "./validity.ts"

// =============================================================================
// Strings
// =============================================================================

export const readUtf8Values = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<string | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const decoder = new TextDecoder()
  const values: Array<string | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = offsetView.getInt32(i * 4, true)
      const end = offsetView.getInt32((i + 1) * 4, true)
      values.push(decoder.decode(dataBuffer.subarray(start, end)))
    } else {
      values.push(null)
    }
  }
  return values
}

export const readLargeUtf8Values = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<string | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const decoder = new TextDecoder()
  const values: Array<string | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = Number(offsetView.getBigInt64(i * 8, true))
      const end = Number(offsetView.getBigInt64((i + 1) * 8, true))
      values.push(decoder.decode(dataBuffer.subarray(start, end)))
    } else {
      values.push(null)
    }
  }
  return values
}

// =============================================================================
// Binary
// =============================================================================

export const readBinaryValues = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Uint8Array | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const values: Array<Uint8Array | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = offsetView.getInt32(i * 4, true)
      const end = offsetView.getInt32((i + 1) * 4, true)
      values.push(dataBuffer.slice(start, end))
    } else {
      values.push(null)
    }
  }
  return values
}

export const readLargeBinaryValues = (
  validityBuffer: Uint8Array,
  offsetBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Uint8Array | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const offsetView = new DataView(offsetBuffer.buffer, offsetBuffer.byteOffset, offsetBuffer.byteLength)
  const values: Array<Uint8Array | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = Number(offsetView.getBigInt64(i * 8, true))
      const end = Number(offsetView.getBigInt64((i + 1) * 8, true))
      values.push(dataBuffer.slice(start, end))
    } else {
      values.push(null)
    }
  }
  return values
}

export const readFixedSizeBinaryValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  byteWidth: number
): ReadonlyArray<Uint8Array | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const values: Array<Uint8Array | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = i * byteWidth
      values.push(dataBuffer.slice(start, start + byteWidth))
    } else {
      values.push(null)
    }
  }
  return values
}
