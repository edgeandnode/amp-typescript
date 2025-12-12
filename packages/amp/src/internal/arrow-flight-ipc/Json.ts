/**
 * Arrow RecordBatch to JSON Conversion
 *
 * This module provides utilities for converting decoded Arrow RecordBatch
 * data to JSON-compatible row objects.
 *
 * @internal
 */
import type { DictionaryRegistry } from "./Decoder.ts"
import { readColumnValues } from "./Readers.ts"
import type { DecodedRecordBatch } from "./RecordBatch.ts"

export interface RecordBatchToJsonOptions {
  bigIntHandling?: "string" | "number" | "bigint"
  dateHandling?: "iso" | "timestamp" | "date"
  binaryHandling?: "base64" | "hex" | "array"
  includeNulls?: boolean
  /**
   * Dictionary registry for resolving dictionary-encoded columns.
   * Required if the record batch contains dictionary-encoded fields.
   */
  dictionaryRegistry?: DictionaryRegistry
}

const uint8ArrayToBase64 = (bytes: Uint8Array): string => {
  let binary = ""
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i])
  return btoa(binary)
}

const uint8ArrayToHex = (bytes: Uint8Array): string => {
  return Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("")
}

const convertValue = (
  value: unknown,
  opts: { bigIntHandling: string; dateHandling: string; binaryHandling: string }
): unknown => {
  if (value === null || value === undefined) return null
  if (typeof value === "bigint") {
    return opts.bigIntHandling === "string"
      ? value.toString()
      : opts.bigIntHandling === "number"
      ? Number(value)
      : value
  }
  if (value instanceof Date) {
    return opts.dateHandling === "iso"
      ? value.toISOString()
      : opts.dateHandling === "timestamp"
      ? value.getTime()
      : value
  }
  if (value instanceof Uint8Array) {
    return opts.binaryHandling === "base64"
      ? uint8ArrayToBase64(value)
      : opts.binaryHandling === "hex"
      ? uint8ArrayToHex(value)
      : Array.from(value)
  }
  if (Array.isArray(value)) {
    return value.map((v) => convertValue(v, opts))
  }
  if (typeof value === "object") {
    const result: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value)) result[k] = convertValue(v, opts)
    return result
  }
  return value
}

/**
 * Convert a decoded record batch to an array of JSON-compatible row objects.
 */
export const recordBatchToJson = (
  batch: DecodedRecordBatch,
  options: RecordBatchToJsonOptions = {}
): Array<Record<string, unknown>> => {
  const {
    bigIntHandling = "string",
    binaryHandling = "base64",
    dateHandling = "iso",
    dictionaryRegistry,
    includeNulls = true
  } = options

  const opts = { bigIntHandling, dateHandling, binaryHandling }
  const numRows = Number(batch.numRows)
  const rows: Array<Record<string, unknown>> = []
  const columnValues = new Map(batch.columns.map((c) => [c.field.name, readColumnValues(c, dictionaryRegistry)]))

  for (let i = 0; i < numRows; i++) {
    const row: Record<string, unknown> = {}
    for (const col of batch.columns) {
      const value = columnValues.get(col.field.name)![i]
      if (value === null) {
        if (includeNulls) row[col.field.name] = null
      } else {
        row[col.field.name] = convertValue(value, opts)
      }
    }
    rows.push(row)
  }
  return rows
}
