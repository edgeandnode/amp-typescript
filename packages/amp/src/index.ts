/**
 * An implementation of the Arrow Flight protocol.
 *
 * Includes `streamProtocol` method for protocol-level streaming with reorg detection.
 */
export * as ArrowFlight from "./arrow-flight.ts"

/**
 * Utilities for performing authentication / authorization related operations.
 */
export * as Auth from "./auth/service.ts"

/**
 * Authentication error domain model.
 */
export * as AuthErrors from "./auth/error.ts"

/**
 * Operations for interacting with the Amp administration API.
 */
export * as AdminApi from "./admin/api.ts"

/**
 * Domain models for the Amp SDK.
 */
export * as Domain from "./core/domain.ts"

/**
 * Operations for interacting with the Amp registry API.
 */
export * as RegistryApi from "./registry/api.ts"

/**
 * Protocol stream service with reorg detection.
 */
export * as ProtocolStream from "./protocol-stream.ts"

/**
 * CDC (Change Data Capture) stream service with Insert/Delete events and
 * at-least-once delivery.
 */
export * as CdcStream from "./cdc-stream.ts"

/**
 * Transactional stream service with exactly-once semantics for data processing.
 */
export * as TransactionalStream from "./transactional-stream.ts"
