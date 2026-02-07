/**
 * An implementation of the Arrow Flight protocol.
 *
 * Provides the `ArrowFlight` service for executing SQL queries against an
 * Arrow Flight API, along with transport abstractions, error types, and
 * interceptor layers for authentication.
 *
 * @module
 */

// =============================================================================
// Transport
// =============================================================================

export { Interceptors, layerInterceptorBearerAuth, Transport } from "./arrow-flight/transport.ts"

// =============================================================================
// Errors
// =============================================================================

export {
  type ArrowFlightError,
  MultipleEndpointsError,
  NoEndpointsError,
  ParseDictionaryBatchError,
  ParseRecordBatchError,
  ParseSchemaError,
  RpcError,
  TicketNotFoundError
} from "./arrow-flight/errors.ts"

// =============================================================================
// Types
// =============================================================================

export { type ExtractQueryResult, type QueryOptions, type QueryResult } from "./arrow-flight/types.ts"

// =============================================================================
// Service
// =============================================================================

export { ArrowFlight, layer } from "./arrow-flight/service.ts"
