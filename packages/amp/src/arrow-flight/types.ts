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
  readonly schema: Schema.Schema<infer _A, infer _I, infer _R>
} ? QueryResult<_A>
  : QueryResult<Record<string, unknown>>
