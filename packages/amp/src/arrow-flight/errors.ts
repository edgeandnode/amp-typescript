import * as Schema from "effect/Schema"

// =============================================================================
// Errors
// =============================================================================

// TODO: improve the error model
/**
 * Represents the possible errors that can occur when executing an Arrow Flight
 * query.
 */
export type ArrowFlightError =
  | RpcError
  | NoEndpointsError
  | MultipleEndpointsError
  | TicketNotFoundError
  | ParseRecordBatchError
  | ParseDictionaryBatchError
  | ParseSchemaError

/**
 * Represents an Arrow Flight RPC request that failed.
 */
export class RpcError extends Schema.TaggedError<RpcError>(
  "Amp/RpcError"
)("RpcError", {
  method: Schema.String,
  /**
   * The underlying reason for the failed RPC request.
   */
  cause: Schema.Defect
}) {}

/**
 * Represents an error that occurred as a result of a `FlightInfo` request
 * returning an empty list of endpoints from which data can be acquired.
 */
export class NoEndpointsError extends Schema.TaggedError<NoEndpointsError>(
  "Amp/NoEndpointsError"
)("NoEndpointsError", {
  /**
   * The SQL query that was requested.
   */
  query: Schema.String
}) {}

// TODO: determine if this is _really_ a logical error case
/**
 * Represents an error that occured as a result of a `FlightInfo` request
 * returning multiple endpoints from which data can be acquired.
 *
 * For Amp queries, there should only ever be **one** authoritative source
 * of data.
 */
export class MultipleEndpointsError extends Schema.TaggedError<MultipleEndpointsError>(
  "Amp/MultipleEndpointsError"
)("MultipleEndpointsError", {
  /**
   * The SQL query that was requested.
   */
  query: Schema.String
}) {}

/**
 * Represents an error that occurred as a result of a `FlightInfo` request
 * whose endpoint did not have a ticket.
 */
export class TicketNotFoundError extends Schema.TaggedError<TicketNotFoundError>(
  "Amp/TicketNotFoundError"
)("TicketNotFoundError", {
  /**
   * The SQL query that was requested.
   */
  query: Schema.String
}) {}

/**
 * Represents an error that occurred as a result of failing to parse an Apache
 * Arrow RecordBatch.
 */
export class ParseRecordBatchError extends Schema.TaggedError<ParseRecordBatchError>(
  "Amp/ParseRecordBatchError"
)("ParseRecordBatchError", {
  /**
   * The underlying reason for the failure to parse a record batch.
   */
  cause: Schema.Defect
}) {}

/**
 * Represents an error that occurred as a result of failing to parse an Apache
 * Arrow DictionaryBatch.
 */
export class ParseDictionaryBatchError extends Schema.TaggedError<ParseDictionaryBatchError>(
  "Amp/ParseDictionaryBatchError"
)("ParseDictionaryBatchError", {
  /**
   * The underlying reason for the failure to parse a dictionary batch.
   */
  cause: Schema.Defect
}) {}

/**
 * Represents an error that occurred as a result of failing to parse an Apache
 * Arrow Schema.
 */
export class ParseSchemaError extends Schema.TaggedError<ParseSchemaError>(
  "Amp/ParseSchemaError"
)("ParseSchemaError", {
  /**
   * The underlying reason for the failure to parse a schema.
   */
  cause: Schema.Defect
}) {}
