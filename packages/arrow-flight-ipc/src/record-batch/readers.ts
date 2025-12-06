import type { Predicate } from "effect/Predicate"
import type { TimeBitWidth, TimeUnit, UnionMode } from "../schema/types.ts"
import type { DecodedColumn } from "./types.ts"

// =============================================================================
// Validity
// =============================================================================

/**
 * Read a validity bitmap and return a function to check if index is valid.
 */
export const createValidityChecker = (validityBuffer: Uint8Array): Predicate<number> => {
  if (validityBuffer.length === 0) {
    // No validity buffer means all values are valid
    return () => true
  }

  return (index) => {
    const byteIndex = Math.floor(index / 8)
    const bitIndex = index % 8
    return (validityBuffer[byteIndex] & (1 << bitIndex)) !== 0
  }
}

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

// =============================================================================
// Decimals
// =============================================================================

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
  const values: Array<string | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = i * byteWidth
      const bigIntValue = bytesToBigInt(dataBuffer.subarray(start, start + byteWidth), true)
      values.push(formatDecimal(bigIntValue, scale))
    } else {
      values.push(null)
    }
  }
  return values
}

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

// =============================================================================
// Temporal
// =============================================================================

const MS_PER_DAY = 86400000

export const readDateDayValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<Date | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? new Date(view.getInt32(i * 4, true) * MS_PER_DAY) : null)
  }
  return values
}

export const readDateMillisecondValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<Date | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? new Date(Number(view.getBigInt64(i * 8, true))) : null)
  }
  return values
}

export const readTimestampValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  unit: keyof typeof TimeUnit
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<Date | null> = []

  const toMs: Record<keyof typeof TimeUnit, (v: bigint) => number> = {
    "SECOND": (v) => Number(v) * 1000,
    "MILLISECOND": (v) => Number(v),
    "MICROSECOND": (v) => Number(v / 1000n),
    "NANOSECOND": (v) => Number(v / 1000000n)
  }

  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const rawValue = view.getBigInt64(i * 8, true)
      values.push(new Date(toMs[unit](rawValue)))
    } else {
      values.push(null)
    }
  }
  return values
}

export const readTimeValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  unit: keyof typeof TimeUnit,
  bitWidth: TimeBitWidth
): ReadonlyArray<number | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const toMs: Record<string, number> = {
    "second": 1000,
    "millisecond": 1,
    "microsecond": 0.001,
    "nanosecond": 0.000001
  }
  const values: Array<number | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const raw = bitWidth === 32 ? view.getInt32(i * 4, true) : Number(view.getBigInt64(i * 8, true))
      values.push(raw * toMs[unit])
    } else {
      values.push(null)
    }
  }
  return values
}

export const readDurationValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  unit: keyof typeof TimeUnit
): ReadonlyArray<{ value: bigint; unit: string } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<{ value: bigint; unit: string } | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? { value: view.getBigInt64(i * 8, true), unit } : null)
  }
  return values
}

export const readIntervalYearMonthValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<{ months: number } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<{ months: number } | null> = []
  for (let i = 0; i < length; i++) {
    values.push(validityChecker(i) ? { months: view.getInt32(i * 4, true) } : null)
  }
  return values
}

export const readIntervalDayTimeValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<{ days: number; milliseconds: number } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<{ days: number; milliseconds: number } | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      values.push({ days: view.getInt32(i * 8, true), milliseconds: view.getInt32(i * 8 + 4, true) })
    } else {
      values.push(null)
    }
  }
  return values
}

export const readIntervalMonthDayNanoValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number
): ReadonlyArray<{ months: number; days: number; nanoseconds: bigint } | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<{ months: number; days: number; nanoseconds: bigint } | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const offset = i * 16
      values.push({
        months: view.getInt32(offset, true),
        days: view.getInt32(offset + 4, true),
        nanoseconds: view.getBigInt64(offset + 8, true)
      })
    } else {
      values.push(null)
    }
  }
  return values
}

// =============================================================================
// Lists
// =============================================================================

const readListValues = (column: DecodedColumn): ReadonlyArray<ReadonlyArray<unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const ov = new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
  const childValues = readColumnValues(children[0])
  const values: Array<Array<unknown> | null> = []
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      values.push(childValues.slice(ov.getInt32(i * 4, true), ov.getInt32((i + 1) * 4, true)))
    } else {
      values.push(null)
    }
  }
  return values
}

const readLargeListValues = (column: DecodedColumn): ReadonlyArray<ReadonlyArray<unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const ov = new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
  const childValues = readColumnValues(children[0])
  const values: Array<Array<unknown> | null> = []
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      const start = Number(ov.getBigInt64(i * 8, true))
      const end = Number(ov.getBigInt64((i + 1) * 8, true))
      values.push(childValues.slice(start, end))
    } else {
      values.push(null)
    }
  }
  return values
}

const readFixedSizeListValues = (
  column: DecodedColumn,
  listSize: number
): ReadonlyArray<ReadonlyArray<unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const childValues = readColumnValues(children[0])
  const values: Array<Array<unknown> | null> = []
  for (let i = 0; i < length; i++) {
    values.push(vc(i) ? childValues.slice(i * listSize, (i + 1) * listSize) : null)
  }
  return values
}

// =============================================================================
// Structs
// =============================================================================

const readStructValues = (column: DecodedColumn): ReadonlyArray<Record<string, unknown> | null> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const childValuesMap = new Map(children.map((c) => [c.field.name, readColumnValues(c)]))
  const values: Array<Record<string, unknown> | null> = []
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      const obj: Record<string, unknown> = {}
      for (const c of children) obj[c.field.name] = childValuesMap.get(c.field.name)![i]
      values.push(obj)
    } else {
      values.push(null)
    }
  }
  return values
}

// =============================================================================
// Maps
// =============================================================================

const readMapValues = (column: DecodedColumn): ReadonlyArray<
  ReadonlyArray<{
    readonly key: unknown
    readonly value: unknown
  }> | null
> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const vc = createValidityChecker(buffers[0])
  const ov = new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
  const entriesValues = readColumnValues(children[0]) as Array<Record<string, unknown> | null>
  const values: Array<Array<{ key: unknown; value: unknown }> | null> = []
  for (let i = 0; i < length; i++) {
    if (vc(i)) {
      const start = ov.getInt32(i * 4, true)
      const end = ov.getInt32((i + 1) * 4, true)
      const entries: Array<{ key: unknown; value: unknown }> = []
      for (let j = start; j < end; j++) {
        const e = entriesValues[j]
        if (e) entries.push({ key: e.key, value: e.value })
      }
      values.push(entries)
    } else {
      values.push(null)
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
  typeIds: ReadonlyArray<number>
): ReadonlyArray<unknown> => {
  const { buffers, children, node } = column
  const length = Number(node.length)
  const typeIdBuffer = buffers[0]
  const offsetView = mode === "DENSE"
    ? new DataView(buffers[1].buffer, buffers[1].byteOffset, buffers[1].byteLength)
    : null
  const childValuesArrays = children.map((c) => readColumnValues(c))
  const typeIdToChild = new Map(typeIds.map((id, idx) => [id, idx]))
  const values: Array<unknown> = []
  for (let i = 0; i < length; i++) {
    const childIdx = typeIdToChild.get(typeIdBuffer[i])
    if (childIdx === undefined) {
      values.push(null)
    } else if (mode === "DENSE" && offsetView) {
      values.push(childValuesArrays[childIdx][offsetView.getInt32(i * 4, true)])
    } else {
      values.push(childValuesArrays[childIdx][i])
    }
  }
  return values
}

// =============================================================================
// Columns
// =============================================================================

export const readColumnValues = (column: DecodedColumn): ReadonlyArray<unknown> => {
  const { buffers, field, node } = column
  const length = Number(node.length)
  const type = field.type

  switch (type.typeId) {
    case "null": {
      return new Array(length).fill(null)
    }
    case "bool": {
      return readBoolValues(buffers[0], buffers[1], length)
    }
    case "int": {
      const vc = createValidityChecker(buffers[0])
      switch (type.bitWidth) {
        case 8: {
          return readInt8Values(buffers[1], vc, length, type.isSigned)
        }
        case 16: {
          return readInt16Values(buffers[1], vc, length, type.isSigned)
        }
        case 32: {
          return readInt32Values(buffers[1], vc, length, type.isSigned)
        }
        case 64: {
          return readInt64Values(buffers[1], vc, length, type.isSigned)
        }
      }
    }
    case "float": {
      const vc = createValidityChecker(buffers[0])
      switch (type.precision) {
        case "HALF": {
          return readFloat16Values(buffers[1], vc, length)
        }
        case "SINGLE": {
          return readFloat32Values(buffers[1], vc, length)
        }
        case "DOUBLE": {
          return readFloat64Values(buffers[1], vc, length)
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
      return readListValues(column)
    }
    case "large-list": {
      return readLargeListValues(column)
    }
    case "fixed-size-list": {
      return readFixedSizeListValues(column, type.listSize)
    }
    case "struct": {
      return readStructValues(column)
    }
    case "map": {
      return readMapValues(column)
    }
    case "union": {
      return readUnionValues(column, type.mode, type.typeIds)
    }
  }
}
