import * as Schema from "effect/Schema"
import { type BlockRange, NonNegativeInt, type RecordBatchMetadata } from "../core/domain.ts"

// =============================================================================
// Types
// =============================================================================

/**
 * Represents the result received from the `ArrowFlight` service when a query
 * is successfully executed.
 */
export interface QueryResult<A> {
  readonly data: ReadonlyArray<A>
  readonly metadata: RecordBatchMetadata
}

/**
 * Represents options that can be passed to `ArrowFlight.query` to control how
 * the query is executed.
 */
export interface QueryOptions {
  readonly schema?: Schema.Codec<unknown, unknown, never, never> | undefined
  /**
   * Controls how BigInt values are represented in decoded query output.
   * @default "string"
   */
  readonly bigIntHandling?: "string" | "number" | "bigint" | undefined
  /**
   * Controls how binary values are represented in decoded query output.
   * @default "base64"
   */
  readonly binaryHandling?: "base64" | "hex" | "array" | undefined
  /**
   * Controls how Date values are represented in decoded query output.
   * @default "iso"
   */
  readonly dateHandling?: "iso" | "timestamp" | "date" | undefined
  /**
   * Controls whether null-valued fields are included in decoded query output.
   * @default true
   */
  readonly includeNulls?: boolean | undefined
  /**
   * Sets the `stream` Amp query setting to `true`.
   */
  readonly stream?: boolean | undefined
  /**
   * A set of block ranges which will be converted into a resume watermark
   * header and sent with the query. This allows resumption of streaming queries.
   */
  readonly resumeWatermark?: ReadonlyArray<BlockRange> | undefined
}

/**
 * A utility type to extract the result type for a query.
 */
export type ExtractQueryResult<Options extends QueryOptions> = Options extends {
  readonly schema: Schema.Top
} ? QueryResult<Schema.Schema.Type<Options["schema"]>>
  : QueryResult<Record<string, unknown>>

// =============================================================================
// Explain
// =============================================================================

/**
 * A single normalized cell value in an `ExplainRow`. EXPLAIN output values are
 * either parsed numbers, raw strings (when no parser matches), or `null` (when
 * the metric is `N/A`).
 */
export const ExplainCell = Schema.NullOr(
  Schema.Union([Schema.String, Schema.Number])
).annotate({ identifier: "ExplainCell" })
export type ExplainCell = typeof ExplainCell.Type

/**
 * A single row in the table returned by `ArrowFlight.explain` — one row per
 * plan node. Always contains `node` (operator name) and `depth` (tree level);
 * additional columns are derived per-node from numeric properties and from
 * expanded metrics (durations renamed with a `_secs` suffix, `total → matched`
 * split into `<key>_total` / `<key>_matched`, etc.).
 */
export const ExplainRow = Schema.Record(Schema.String, ExplainCell)
  .annotate({ identifier: "ExplainRow" })
export type ExplainRow = typeof ExplainRow.Type

/**
 * The intermediate parsed shape of a single plan node, before normalization
 * into the table form. `properties` and `metrics` are kept as raw strings here
 * so callers that need the unprocessed values can access them.
 */
export const PlanNode = Schema.Struct({
  name: Schema.String,
  depth: NonNegativeInt,
  properties: Schema.Record(Schema.String, Schema.String),
  metrics: Schema.Record(Schema.String, Schema.String)
}).annotate({ identifier: "PlanNode" })
export type PlanNode = typeof PlanNode.Type

/**
 * The result returned by `ArrowFlight.explain` — a tabular view of the parsed
 * plan suitable for direct rendering or serialization. `columns` is the union
 * of all keys across `rows` in first-appearance order (`node`, `depth`, then
 * properties, then metric columns), so callers can drive a table render
 * without recomputing the header set themselves.
 */
export const ExplainResult = Schema.Struct({
  columns: Schema.Array(Schema.String),
  rows: Schema.Array(ExplainRow)
}).annotate({ identifier: "ExplainResult" })
export type ExplainResult = typeof ExplainResult.Type
