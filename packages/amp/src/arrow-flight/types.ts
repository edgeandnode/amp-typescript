import type * as Schema from "effect/Schema"
import type { BlockRange, RecordBatchMetadata } from "../core/domain.ts"

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
  readonly schema?: Schema.Any | undefined
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
