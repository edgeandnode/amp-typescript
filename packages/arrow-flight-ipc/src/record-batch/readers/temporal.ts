import { createValidityChecker } from "./validity.ts"

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
  unit: "second" | "millisecond" | "microsecond" | "nanosecond"
): ReadonlyArray<Date | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const view = new DataView(dataBuffer.buffer, dataBuffer.byteOffset, dataBuffer.byteLength)
  const values: Array<Date | null> = []

  const toMs: Record<string, (v: bigint) => number> = {
    "second": (v) => Number(v) * 1000,
    "millisecond": (v) => Number(v),
    "microsecond": (v) => Number(v / 1000n),
    "nanosecond": (v) => Number(v / 1000000n)
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
  unit: "second" | "millisecond" | "microsecond" | "nanosecond",
  bitWidth: 32 | 64
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
  unit: "second" | "millisecond" | "microsecond" | "nanosecond"
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
