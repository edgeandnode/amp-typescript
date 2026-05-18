import * as Schema from "effect/Schema"
import { type ExplainCell, ExplainResult, PlanNode } from "../../arrow-flight/types.ts"

// =============================================================================
// Internal Schemas
// =============================================================================

/**
 * The wire shape of a single row returned by the Amp Arrow Flight backend
 * when an `EXPLAIN` / `EXPLAIN ANALYZE` query is executed.
 */
export const ExplainResultRow = Schema.Struct({
  plan_type: Schema.String,
  plan: Schema.String
}).annotate({ identifier: "Amp/ArrowFlight/Explain/ResultRow" })
export type ExplainResultRow = typeof ExplainResultRow.Type

// =============================================================================
// Constants
// =============================================================================

const INDENT_WIDTH = 2

const METRICS_RE = /,\s*metrics=\[(.+)\]$/
const DURATION_RE = /^([\d.]+(?:e[+-]?\d+)?)\s*(ns|µs|us|ms|s)$/
const NUMBER_UNIT_RE = /^([\d.]+(?:e[+-]?\d+)?)\s*(KB|MB|GB|TB|K|M|G)$/i
const TOTAL_MATCHED_RE =
  /^([\d.]+(?:e[+-]?\d+)?\s*(?:KB|MB|GB|TB|K|M|G)?)\s+total\s*→\s*([\d.]+(?:e[+-]?\d+)?\s*(?:KB|MB|GB|TB|K|M|G)?)\s+matched$/
const PCT_RATIO_RE = /^([\d.]+(?:e[+-]?\d+)?)%\s*\([^)]*\)$/
const LEADING_NUMBER_RE = /^\{?\s*([\d.]+(?:e[+-]?\d+)?)\s*[KMG]?\b/

const DURATION_UNITS: Record<string, number> = {
  ns: 1e-9,
  µs: 1e-6,
  us: 1e-6,
  ms: 1e-3,
  s: 1
}

const MULTIPLIER_UNITS: Record<string, number> = {
  K: 1e3,
  M: 1e6,
  G: 1e9,
  KB: 1e3,
  MB: 1e6,
  GB: 1e9,
  TB: 1e12
}

// =============================================================================
// SQL Prefix
// =============================================================================

const LEADING_EXPLAIN_RE = /^\s*EXPLAIN\b/i

/**
 * Prepend `EXPLAIN` (or `EXPLAIN ANALYZE` when `analyze` is true) to the
 * given SQL, unless the SQL already starts with `EXPLAIN` (in any case),
 * in which case it is returned unchanged. This avoids producing invalid
 * statements like `EXPLAIN EXPLAIN ANALYZE SELECT ...` when callers paste
 * a query that was already prefixed.
 */
export const prefixExplain = (sql: string, analyze: boolean): string => {
  if (LEADING_EXPLAIN_RE.test(sql)) return sql
  return (analyze ? "EXPLAIN ANALYZE " : "EXPLAIN ") + sql
}

// =============================================================================
// Plan Parsing
// =============================================================================

/**
 * Parse a DataFusion EXPLAIN / EXPLAIN ANALYZE plan text into a list of
 * `PlanNode` entries. Indentation determines tree depth (2 spaces per level).
 */
export const parsePlan = (planText: string): ReadonlyArray<PlanNode> => {
  const text = planText.trim()
  if (text.length === 0) return []

  const nodes: Array<PlanNode> = []
  for (const line of text.split("\n")) {
    const stripped = line.replace(/^[ ]+/, "")
    const depth = (line.length - stripped.length) / INDENT_WIDTH

    const match = METRICS_RE.exec(stripped)
    let nodePart: string
    let metrics: Record<string, string>
    if (match !== null) {
      nodePart = stripped.slice(0, match.index)
      metrics = parseMetrics(match[1]!)
    } else {
      nodePart = stripped
      metrics = {}
    }

    const { name, properties } = splitNameAndProperties(nodePart)
    nodes.push(PlanNode.make({ name, depth, properties, metrics }))
  }
  return nodes
}

/**
 * Convert a list of `PlanNode`s into the table form returned by
 * `ArrowFlight.explain`. Mirrors the Python `ExplainResult.to_dataframe()`
 * shape: numeric properties only, expanded metrics, durations in seconds with
 * `_secs` suffix, `total → matched` split into two columns.
 *
 * `columns` is the union of all keys across rows, in first-appearance order
 * (`node`, `depth`, then properties, then metric columns).
 */
export const planToTable = (nodes: ReadonlyArray<PlanNode>): ExplainResult => {
  const rows: Array<Record<string, ExplainCell>> = []
  const seen = new Set<string>()
  const columns: Array<string> = []
  const recordKey = (key: string): void => {
    if (seen.has(key)) return
    seen.add(key)
    columns.push(key)
  }

  for (const node of nodes) {
    const row: Record<string, ExplainCell> = {}
    row["node"] = node.name
    recordKey("node")
    row["depth"] = node.depth
    recordKey("depth")
    for (const key in node.properties) {
      const num = extractNumeric(node.properties[key]!)
      if (num !== null) {
        row[key] = num
        recordKey(key)
      }
    }
    for (const key in node.metrics) {
      expandMetric(key, node.metrics[key]!, row, recordKey)
    }
    rows.push(row)
  }
  return ExplainResult.make({ columns, rows })
}

// =============================================================================
// Internal Helpers
// =============================================================================

const splitNameAndProperties = (
  nodePart: string
): { readonly name: string; readonly properties: Record<string, string> } => {
  const colon = nodePart.indexOf(":")
  if (colon > 0 && nodePart.slice(0, colon).indexOf(" ") === -1) {
    const name = nodePart.slice(0, colon)
    const propsStr = nodePart
      .slice(colon + 1)
      .trim()
      .replace(/,$/, "")
    return {
      name,
      properties: propsStr.length === 0 ? {} : parseMetrics(propsStr)
    }
  }
  return { name: nodePart.replace(/,$/, ""), properties: {} }
}

/**
 * Parse a `key=value, key=value` string. Walks character-by-character so values
 * can contain commas inside `()` or `[]` without being split.
 */
const parseMetrics = (s: string): Record<string, string> => {
  const result: Record<string, string> = {}
  const n = s.length
  let i = 0
  while (i < n) {
    const eq = s.indexOf("=", i)
    if (eq === -1) break
    const key = s.slice(i, eq).trim()

    let depthP = 0
    let depthB = 0
    let j = eq + 1
    while (j < n) {
      const c = s.charCodeAt(j)
      if (c === 40 /* ( */) depthP++
      else if (c === 41 /* ) */) depthP--
      else if (c === 91 /* [ */) depthB++
      else if (c === 93 /* ] */) depthB--
      else if (c === 44 /* , */ && depthP === 0 && depthB === 0) break
      j++
    }

    result[key] = s.slice(eq + 1, j).trim()
    i = j + 1
  }
  return result
}

const parseNumberWithUnit = (raw: string): number | null => {
  const s = raw.trim()
  const m = NUMBER_UNIT_RE.exec(s)
  if (m !== null) {
    const mult = MULTIPLIER_UNITS[m[2]!.toUpperCase()]!
    return parseFloat(m[1]!) * mult
  }
  if (/^-?\d+$/.test(s)) return parseInt(s, 10)
  const f = parseFloat(s)
  if (!Number.isNaN(f) && /^-?[\d.]+(?:e[+-]?\d+)?$/.test(s)) return f
  return null
}

const extractNumeric = (raw: string): number | null => {
  const value = raw.trim()
  const num = parseNumberWithUnit(value)
  if (num !== null) return num
  const m = LEADING_NUMBER_RE.exec(value)
  if (m !== null) {
    return parseNumberWithUnit(m[0].replace(/^\{/, "").trim())
  }
  return null
}

/**
 * Expand a single metric into one or more cells written into `row`, recording
 * each emitted column key via `recordKey` so the caller can build a stable,
 * first-appearance ordering of the table's columns.
 *
 * Mirrors Python `_expand_metric` plus the `_secs` rename that Python applies
 * post-hoc via `df.rename`. We rename inline because the per-row Record we
 * build *is* the table — there's no second pass.
 */
const expandMetric = (
  key: string,
  raw: string,
  row: Record<string, ExplainCell>,
  recordKey: (key: string) => void
): void => {
  const value = raw.trim()

  if (value.startsWith("N/A")) {
    row[key] = null
    recordKey(key)
    return
  }

  const tm = TOTAL_MATCHED_RE.exec(value)
  if (tm !== null) {
    const totalKey = `${key}_total`
    const matchedKey = `${key}_matched`
    row[totalKey] = parseNumberWithUnit(tm[1]!)
    row[matchedKey] = parseNumberWithUnit(tm[2]!)
    recordKey(totalKey)
    recordKey(matchedKey)
    return
  }

  const pct = PCT_RATIO_RE.exec(value)
  if (pct !== null) {
    row[key] = parseFloat(pct[1]!)
    recordKey(key)
    return
  }

  const dur = DURATION_RE.exec(value)
  if (dur !== null) {
    const secsKey = `${key}_secs`
    row[secsKey] = parseFloat(dur[1]!) * DURATION_UNITS[dur[2]!]!
    recordKey(secsKey)
    return
  }

  const num = parseNumberWithUnit(value)
  if (num !== null) {
    row[key] = num
    recordKey(key)
    return
  }

  row[key] = value
  recordKey(key)
}
