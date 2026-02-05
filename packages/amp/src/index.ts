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
 * Operations for interacting with the Amp registry API.
 */
export * as RegistryApi from "./registry/api.ts"

/**
 * Protocol stream types and validation for reorg detection.
 *
 * These types are used by `ArrowFlight.streamProtocol()`.
 */
export * as ProtocolStream from "./protocol-stream/index.ts"
