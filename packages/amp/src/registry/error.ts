/**
 * This module contains error definitions which represent the standard error
 * responses returned by the Amp Registry API.
 *
 * Errors provide structured error details including a machine-readable error
 * code and a human-readable message.
 *
 * ## Error Code Conventions
 * - Error codes use SCREAMING_SNAKE_CASE (e.g., `DATASET_NOT_FOUND`)
 * - Codes are stable and can be relied upon programmatically
 * - Messages may change and should only be used for display/logging
 *
 * ## Example JSON Response
 * ```json
 * {
 *   "error_code": "DATASET_NOT_FOUND",
 *   "error_message": "dataset 'eth_mainnet' version '1.0.0' not found",
 *   "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
 * }
 * ```
 */
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import * as Schema from "effect/Schema"

/**
 * Machine-readable error code in SCREAMING_SNAKE_CASE format
 *
 * Error codes are stable across API versions and should be used
 * for programmatic error handling. Examples: `INVALID_SELECTOR`,
 * `DATASET_NOT_FOUND`, `REGISTRY_DB_ERROR`
 */
const ErrorCode = <Code extends string>(
  code: Code
): Schema.PropertySignature<":", Code, "error_code", ":", Code> =>
  Schema.Literal(code).pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  )

const BaseErrorFields = {
  /**
   * Human-readable error message
   *
   * Messages provide detailed context about the error but may change
   * over time. Use `error_code` for programmatic decisions.
   */
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  ),
  /**
   * Request ID for tracing and correlation
   *
   * This ID can be used to correlate error responses with server logs
   * for debugging and support purposes. The ID is generated per-request
   * and appears in both logs and error responses.
   */
  requestId: Schema.optional(Schema.String).pipe(
    Schema.fromKey("request_id")
  )
}

export class DatasetConversionError extends Schema.Class<DatasetConversionError>(
  "Amp/RegistryApi/DatasetConversionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DATASET_CONVERSION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class DatasetNotFoundError extends Schema.Class<DatasetNotFoundError>(
  "Amp/RegistryApi/DatasetNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DATASET_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

export class DatasetVersionConversionError extends Schema.Class<DatasetVersionConversionError>(
  "Amp/RegistryApi/DatasetVersionConversionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DATASET_VERSION_CONVERSION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class DatasetVersionNotFoundError extends Schema.Class<DatasetVersionNotFoundError>(
  "Amp/RegistryApi/DatasetVersionNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("VERSION_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

export class ForbiddenError extends Schema.Class<ForbiddenError>(
  "Amp/RegistryApi/ForbiddenError"
)({
  ...BaseErrorFields,
  code: ErrorCode("FORBIDDEN")
}, HttpApiSchema.annotations({ status: 403 })) {}

export class InvalidDatasetOwnerPathError extends Schema.Class<InvalidDatasetOwnerPathError>(
  "Amp/RegistryApi/InvalidDatasetOwnerPathError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_DATASET_OWNER_PATH")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidDatasetReferenceError extends Schema.Class<InvalidDatasetReferenceError>(
  "Amp/RegistryApi/InvalidDatasetReferenceError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_REFERENCE")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class InvalidDatasetSelectorError extends Schema.Class<InvalidDatasetSelectorError>(
  "Amp/RegistryApi/InvalidDatasetSelectorError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_DATASET_SELECTOR")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidManifestError extends Schema.Class<InvalidManifestError>(
  "Amp/RegistryApi/InvalidManifestError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_MANIFEST")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidManifestHashError extends Schema.Class<InvalidManifestHashError>(
  "Amp/RegistryApi/InvalidManifestHashError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_MANIFEST_HASH")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class InvalidNamespaceError extends Schema.Class<InvalidNamespaceError>(
  "Amp/RegistryApi/InvalidNamespaceError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_NAMESPACE")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidQueryParametersError extends Schema.Class<InvalidQueryParametersError>(
  "Amp/RegistryApi/InvalidQueryParametersError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_QUERY_PARAMETERS")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidPathParametersError extends Schema.Class<InvalidPathParametersError>(
  "Amp/RegistryApi/InvalidPathParametersError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_PATH_PARAMETERS")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidRequestBodyError extends Schema.Class<InvalidRequestBodyError>(
  "Amp/RegistryApi/InvalidRequestBodyError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_REQUEST_BODY")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class InvalidSelectorError extends Schema.Class<InvalidSelectorError>(
  "Amp/RegistryApi/InvalidSelectorError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_SELECTOR")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class LatestDatasetVersionNotFoundError extends Schema.Class<LatestDatasetVersionNotFoundError>(
  "Amp/RegistryApi/LatestDatasetVersionNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LATEST_VERSION_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

export class LimitInvalidError extends Schema.Class<LimitInvalidError>(
  "Amp/RegistryApi/LimitInvalidError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIMIT_INVALID")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class LimitTooLargeError extends Schema.Class<LimitTooLargeError>(
  "Amp/RegistryApi/LimitTooLargeError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIMIT_TOO_LARGE")
}, HttpApiSchema.annotations({ status: 400 })) {}

export class ManifestDeserializationError extends Schema.Class<ManifestDeserializationError>(
  "Amp/RegistryApi/ManifestDeserializationError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_DESERIALIZATION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class ManifestNotFoundError extends Schema.Class<ManifestNotFoundError>(
  "Amp/RegistryApi/ManifestNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

export class ManifestRetrievalError extends Schema.Class<ManifestRetrievalError>(
  "Amp/RegistryApi/ManifestRetrievalError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_RETRIEVAL_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class NamespaceAccessDeniedError extends Schema.Class<NamespaceAccessDeniedError>(
  "Amp/RegistryApi/NamespaceAccessDeniedError"
)({
  ...BaseErrorFields,
  code: ErrorCode("NAMESPACE_ACCESS_DENIED")
}, HttpApiSchema.annotations({ status: 403 })) {}

export class RegistryDatabaseError extends Schema.Class<RegistryDatabaseError>(
  "Amp/RegistryApi/RegistryDatabaseError"
)({
  ...BaseErrorFields,
  code: ErrorCode("AMP_REGISTRY_DB_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class SavedQueryConversionError extends Schema.Class<SavedQueryConversionError>(
  "Amp/RegistryApi/SavedQueryConversionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("SAVED_QUERY_CONVERSION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

export class ServiceUnavailableError extends Schema.Class<ServiceUnavailableError>(
  "Amp/RegistryApi/ServiceUnavailableError"
)({
  ...BaseErrorFields,
  code: ErrorCode("SERVICE_UNAVAILABLE")
}, HttpApiSchema.annotations({ status: 503 })) {}
