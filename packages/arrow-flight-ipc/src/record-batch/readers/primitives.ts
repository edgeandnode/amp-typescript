import type { Predicate } from "effect/Predicate"
import { createValidityChecker } from "./validity.ts"

// =============================================================================
// Booleans
// =============================================================================

export const readBoolValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<boolean | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const values: Array<boolean | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const byteIndex = Math.floor(i / 8)
      const bitIndex = i % 8
      values.push((dataBuffer[byteIndex] & (1 << bitIndex)) !== 0)
    } else {
      values.push(null)
    }
  }
  return values
}

// =============================================================================
// Integers
// =============================================================================

export const readInt8Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number,
  signed: boolean
): ReadonlyArray<number | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? (signed ? view.getInt8(i) : view.getUint8(i)) : null)
  }
  return values
}

export const readInt16Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number,
  signed: boolean
): ReadonlyArray<number | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? (signed ? view.getInt16(i * 2, true) : view.getUint16(i * 2, true)) : null)
  }
  return values
}

export const readInt32Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number,
  signed: boolean = true
): ReadonlyArray<number | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? (signed ? view.getInt32(i * 4, true) : view.getUint32(i * 4, true)) : null)
  }
  return values
}

export const readInt64Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number,
  signed: boolean = true
): ReadonlyArray<bigint | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<bigint | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? (signed ? view.getBigInt64(i * 8, true) : view.getBigUint64(i * 8, true)) : null)
  }
  return values
}

// =============================================================================
// Floats
// =============================================================================

const decodeFloat16 = (bits: number): number => {
  const sign = (bits >> 15) & 1
  const exponent = (bits >> 10) & 0x1f
  const fraction = bits & 0x3ff
  if (exponent === 0) return (sign ? -1 : 1) * Math.pow(2, -14) * (fraction / 1024)
  if (exponent === 31) return fraction === 0 ? (sign ? -Infinity : Infinity) : NaN
  return (sign ? -1 : 1) * Math.pow(2, exponent - 15) * (1 + fraction / 1024)
}

export const readFloat16Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number
): ReadonlyArray<number | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? decodeFloat16(view.getUint16(i * 2, true)) : null)
  }
  return values
}

export const readFloat32Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number
): ReadonlyArray<number | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? view.getFloat32(i * 4, true) : null)
  }
  return values
}

export const readFloat64Values = (
  dataBuffer: Uint8Array,
  validityChecker: Predicate<number>,
  length: number
): ReadonlyArray<number | null> => {
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? view.getFloat64(i * 8, true) : null)
  }
  return values
}
