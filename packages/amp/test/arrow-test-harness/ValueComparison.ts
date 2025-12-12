/**
 * Value Comparison Utilities
 *
 * Provides type-aware comparison between expected values (from generators)
 * and actual decoded values (from readers).
 *
 * @internal
 */
import { readColumnValues } from "@edgeandnode/amp/internal/arrow-flight-ipc/Readers"
import type { DecodedRecordBatch } from "@edgeandnode/amp/internal/arrow-flight-ipc/RecordBatch"
import type { ArrowDataType, ArrowField, ArrowSchema } from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"

/**
 * Result of comparing values.
 */
export interface ComparisonResult {
  readonly success: boolean
  readonly errors: ReadonlyArray<ComparisonError>
}

export interface ComparisonError {
  readonly field: string
  readonly index: number
  readonly expected: unknown
  readonly actual: unknown
  readonly message: string
}

/**
 * Compare expected values from generation against decoded values from parsing.
 */
export const compareColumnValues = (
  fieldName: string,
  fieldType: ArrowDataType,
  expected: ReadonlyArray<unknown>,
  actual: ReadonlyArray<unknown>
): ComparisonResult => {
  const errors: Array<ComparisonError> = []

  if (expected.length !== actual.length) {
    errors.push({
      field: fieldName,
      index: -1,
      expected: expected.length,
      actual: actual.length,
      message: `Length mismatch: expected ${expected.length}, got ${actual.length}`
    })
    return { success: false, errors }
  }

  for (let i = 0; i < expected.length; i++) {
    const exp = expected[i]
    const act = actual[i]

    if (!valuesEqual(fieldType, exp, act)) {
      errors.push({
        field: fieldName,
        index: i,
        expected: exp,
        actual: act,
        message: `Value mismatch at index ${i}`
      })
    }
  }

  return { success: errors.length === 0, errors }
}

/**
 * Compare a single expected value against an actual decoded value.
 * Handles type-specific conversions between generator and reader formats.
 */
const valuesEqual = (type: ArrowDataType, expected: unknown, actual: unknown): boolean => {
  // Both null
  if (expected === null && actual === null) return true
  // One null, one not
  if (expected === null || actual === null) return false

  switch (type.typeId) {
    case "null":
      return true

    case "bool":
    case "int":
      return expected === actual

    case "float":
      return floatsEqual(expected as number, actual as number, type.precision)

    case "decimal":
      return decimalsEqual(expected as bigint, actual as string, type.scale)

    case "utf8":
    case "large-utf8":
      return expected === actual

    case "binary":
    case "large-binary":
    case "fixed-size-binary":
      return uint8ArraysEqual(expected as Uint8Array, actual as Uint8Array)

    case "date":
      return datesEqual(expected, actual as Date, type.unit)

    case "time":
      return timesEqual(expected as number | bigint, actual as number, type.unit)

    case "timestamp":
      return timestampsEqual(expected as bigint, actual as Date, type.unit)

    case "duration":
      return durationsEqual(expected as bigint, actual as { value: bigint; unit: string })

    case "interval":
      return intervalsEqual(expected, actual, type.unit)

    case "list":
    case "large-list":
      return listsEqual(type, expected as Array<unknown>, actual as Array<unknown>)

    case "fixed-size-list":
      return listsEqual(type, expected as Array<unknown>, actual as Array<unknown>)

    case "struct":
      return structsEqual(type, expected as Record<string, unknown>, actual as Record<string, unknown>)

    case "map":
      return mapsEqual(type, expected as Array<unknown>, actual as Array<unknown>)

    case "union":
      return unionsEqual(type, expected, actual)

    default:
      return expected === actual
  }
}

const floatsEqual = (expected: number, actual: number, precision: string): boolean => {
  // Handle NaN (NaN !== NaN in JS, so use Number.isNaN)
  if (Number.isNaN(expected) && Number.isNaN(actual)) return true
  if (Number.isNaN(expected) || Number.isNaN(actual)) return false

  // Handle infinities
  if (!Number.isFinite(expected) && !Number.isFinite(actual)) return expected === actual
  if (!Number.isFinite(expected) || !Number.isFinite(actual)) return false

  // Handle negative zero (0 === -0 in JS, but they're distinct in IEEE754)
  if (expected === 0 && actual === 0) {
    return Object.is(expected, actual)
  }

  // Float16 has very low precision
  if (precision === "HALF") {
    const tolerance = Math.abs(expected) * 0.01 + 0.1
    return Math.abs(expected - actual) <= tolerance
  }

  // Float32 comparison with tolerance
  if (precision === "SINGLE") {
    const tolerance = Math.abs(expected) * 1e-6 + 1e-6
    return Math.abs(expected - actual) <= tolerance
  }

  // Float64 comparison
  const tolerance = Math.abs(expected) * 1e-14 + 1e-14
  return Math.abs(expected - actual) <= tolerance
}

const decimalsEqual = (expected: bigint, actual: string, scale: number): boolean => {
  // Convert expected bigint to formatted decimal string
  const isNegative = expected < 0n
  const absValue = isNegative ? -expected : expected
  const str = absValue.toString()

  let expectedStr: string
  if (scale === 0) {
    expectedStr = isNegative ? `-${str}` : str
  } else {
    const paddedStr = str.padStart(scale + 1, "0")
    const intPart = paddedStr.slice(0, -scale) || "0"
    const fracPart = paddedStr.slice(-scale)
    expectedStr = `${isNegative ? "-" : ""}${intPart}.${fracPart}`
  }

  return expectedStr === actual
}

const uint8ArraysEqual = (expected: Uint8Array, actual: Uint8Array): boolean => {
  if (expected.length !== actual.length) return false
  for (let i = 0; i < expected.length; i++) {
    if (expected[i] !== actual[i]) return false
  }
  return true
}

const MS_PER_DAY = 86400000

const datesEqual = (expected: unknown, actual: Date, unit: string): boolean => {
  if (unit === "DAY") {
    // Generator stores days since epoch as number
    const expectedMs = (expected as number) * MS_PER_DAY
    return expectedMs === actual.getTime()
  }
  // MILLISECOND: generator stores ms since epoch as bigint
  return Number(expected as bigint) === actual.getTime()
}

const timesEqual = (
  expected: number | bigint,
  actual: number,
  unit: string
): boolean => {
  // Generator stores raw time value, reader converts to ms
  const toMs: Record<string, number> = {
    "SECOND": 1000,
    "MILLISECOND": 1,
    "MICROSECOND": 0.001,
    "NANOSECOND": 0.000001
  }
  const expectedMs = Number(expected) * toMs[unit]
  return Math.abs(expectedMs - actual) < 0.001
}

const timestampsEqual = (expected: bigint, actual: Date, unit: string): boolean => {
  const toMs: Record<string, (v: bigint) => number> = {
    "SECOND": (v) => Number(v) * 1000,
    "MILLISECOND": (v) => Number(v),
    "MICROSECOND": (v) => Number(v / 1000n),
    "NANOSECOND": (v) => Number(v / 1000000n)
  }
  const expectedMs = toMs[unit](expected)
  return expectedMs === actual.getTime()
}

const durationsEqual = (
  expected: bigint,
  actual: { value: bigint; unit: string }
): boolean => {
  return expected === actual.value
}

const intervalsEqual = (expected: unknown, actual: unknown, unit: string): boolean => {
  if (unit === "YEAR_MONTH") {
    // Generator stores number of months directly
    return expected === (actual as { months: number }).months
  }
  if (unit === "DAY_TIME") {
    const exp = expected as { days: number; milliseconds: number }
    const act = actual as { days: number; milliseconds: number }
    return exp.days === act.days && exp.milliseconds === act.milliseconds
  }
  // MONTH_DAY_NANO
  const exp = expected as { months: number; days: number; nanoseconds: bigint }
  const act = actual as { months: number; days: number; nanoseconds: bigint }
  return exp.months === act.months && exp.days === act.days && exp.nanoseconds === act.nanoseconds
}

const listsEqual = (
  type: ArrowDataType,
  expected: Array<unknown>,
  actual: Array<unknown>
): boolean => {
  if (expected.length !== actual.length) return false

  // Get child type - for lists, the child is the item type
  const childType = "children" in type
    ? ((type as { children?: ReadonlyArray<ArrowField> }).children?.[0]?.type)
    : undefined

  if (!childType) {
    // Fallback to simple comparison
    return JSON.stringify(expected) === JSON.stringify(actual)
  }

  for (let i = 0; i < expected.length; i++) {
    if (!valuesEqual(childType, expected[i], actual[i])) return false
  }
  return true
}

const structsEqual = (
  type: ArrowDataType,
  expected: Record<string, unknown>,
  actual: Record<string, unknown>
): boolean => {
  const children = "children" in type
    ? ((type as { children?: ReadonlyArray<ArrowField> }).children ?? [])
    : []

  for (const child of children) {
    if (!valuesEqual(child.type, expected[child.name], actual[child.name])) {
      return false
    }
  }
  return true
}

const mapsEqual = (
  type: ArrowDataType,
  expected: Array<unknown>,
  actual: Array<unknown>
): boolean => {
  if (expected.length !== actual.length) return false

  for (let i = 0; i < expected.length; i++) {
    const expEntry = expected[i] as { key: unknown; value: unknown }
    const actEntry = actual[i] as { key: unknown; value: unknown }

    // Get key/value types from map's entries child
    const entriesField = "children" in type
      ? ((type as { children?: ReadonlyArray<ArrowField> }).children?.[0])
      : undefined

    if (entriesField && entriesField.children.length >= 2) {
      const keyType = entriesField.children[0].type
      const valueType = entriesField.children[1].type

      if (!valuesEqual(keyType, expEntry.key, actEntry.key)) return false
      if (!valuesEqual(valueType, expEntry.value, actEntry.value)) return false
    } else {
      if (expEntry.key !== actEntry.key) return false
      if (expEntry.value !== actEntry.value) return false
    }
  }
  return true
}

const unionsEqual = (
  type: ArrowDataType,
  expected: unknown,
  actual: unknown
): boolean => {
  // For unions, we just compare the selected values directly
  // The type information of the selected variant would be needed for deep comparison
  // For now, use JSON comparison as a fallback
  return JSON.stringify(expected) === JSON.stringify(actual)
}

/**
 * Verify that all columns in a decoded record batch match expected values.
 */
export const verifyDecodedValues = (
  schema: ArrowSchema,
  decoded: DecodedRecordBatch,
  expectedValues: Record<string, ReadonlyArray<unknown>>
): ComparisonResult => {
  const allErrors: Array<ComparisonError> = []

  for (const field of schema.fields) {
    const column = decoded.getColumn(field.name)
    if (!column) {
      allErrors.push({
        field: field.name,
        index: -1,
        expected: "column",
        actual: null,
        message: `Column "${field.name}" not found in decoded batch`
      })
      continue
    }

    const actualValues = readColumnValues(column)
    const expected = expectedValues[field.name]

    if (!expected) {
      allErrors.push({
        field: field.name,
        index: -1,
        expected: "expected values",
        actual: null,
        message: `No expected values for column "${field.name}"`
      })
      continue
    }

    const result = compareColumnValues(field.name, field.type, expected, actualValues)
    for (const err of result.errors) {
      allErrors.push(err)
    }
  }

  return { success: allErrors.length === 0, errors: allErrors }
}

/**
 * Format comparison errors for test output.
 */
export const formatComparisonErrors = (errors: ReadonlyArray<ComparisonError>): string => {
  if (errors.length === 0) return "No errors"

  const lines = errors.slice(0, 10).map((e) => {
    if (e.index === -1) {
      return `  ${e.field}: ${e.message}`
    }
    return `  ${e.field}[${e.index}]: expected ${formatValue(e.expected)}, got ${formatValue(e.actual)}`
  })

  if (errors.length > 10) {
    lines.push(`  ... and ${errors.length - 10} more errors`)
  }

  return lines.join("\n")
}

const formatValue = (v: unknown): string => {
  if (v === null) return "null"
  if (v === undefined) return "undefined"
  if (typeof v === "bigint") return `${v}n`
  if (v instanceof Date) return v.toISOString()
  if (v instanceof Uint8Array) return `Uint8Array(${v.length})`
  if (Array.isArray(v)) return `Array(${v.length})`
  if (typeof v === "object") return JSON.stringify(v)
  return String(v)
}
