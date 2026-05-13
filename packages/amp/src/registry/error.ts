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
import * as Schema from "effect/Schema"

export const makeError = <
  const Code extends string,
  const Tag extends string,
  const Fields extends Schema.Struct.Fields = {}
>(code: Code, tag: Tag, fields?: Fields): Schema.encodeKeys<
  Schema.Struct<
    Fields & {
      readonly _tag: Schema.withDecodingDefaultKey<Schema.tag<Tag>>
      /**
       * Machine-readable error code in SCREAMING_SNAKE_CASE format
       *
       * Error codes are stable across API versions and should be used
       * for programmatic error handling. Examples: `INVALID_SELECTOR`,
       * `DATASET_NOT_FOUND`, `REGISTRY_DB_ERROR`
       */
      readonly code: Schema.withConstructorDefault<Schema.Literal<Code>>
      /**
       * Human-readable error message
       *
       * Messages provide detailed context about the error but may change
       * over time. Use `error_code` for programmatic decisions.
       */
      readonly message: Schema.String
      /**
       * Request ID for tracing and correlation
       *
       * This ID can be used to correlate error responses with server logs
       * for debugging and support purposes. The ID is generated per-request
       * and appears in both logs and error responses.
       */
      readonly requestId: Schema.optional<Schema.String>
    }
  >,
  {
    readonly code: "error_code"
    readonly message: "error_message"
    readonly requestId: "request_id"
  }
> =>
  Schema.Struct({
    ...fields,
    _tag: Schema.tagDefaultOmit(tag),
    /**
     * Machine-readable error code in SCREAMING_SNAKE_CASE format
     *
     * Error codes are stable across API versions and should be used
     * for programmatic error handling. Examples: `INVALID_SELECTOR`,
     * `DATASET_NOT_FOUND`, `REGISTRY_DB_ERROR`
     */
    code: Schema.Literal(code),
    /**
     * Human-readable error message
     *
     * Messages provide detailed context about the error but may change
     * over time. Use `error_code` for programmatic decisions.
     */
    message: Schema.String,
    /**
     * Request ID for tracing and correlation
     *
     * This ID can be used to correlate error responses with server logs
     * for debugging and support purposes. The ID is generated per-request
     * and appears in both logs and error responses.
     */
    requestId: Schema.optional(Schema.String)
  }).pipe(Schema.encodeKeys({
    code: "error_code",
    message: "error_message",
    requestId: "request_id"
  })) as any

export const DatasetConversionError = makeError(
  "DATASET_CONVERSION_ERROR",
  "DatasetConversionError"
).annotate({ httpApiStatus: 500 })

export type DatasetConversionError = typeof DatasetConversionError.Type

export const DatasetNotFoundError = makeError(
  "DATASET_NOT_FOUND",
  "DatasetNotFoundError"
).annotate({ httpApiStatus: 404 })

export type DatasetNotFoundError = typeof DatasetNotFoundError.Type

export const DatasetVersionConversionError = makeError(
  "DATASET_VERSION_CONVERSION_ERROR",
  "DatasetVersionConversionError"
).annotate({ httpApiStatus: 500 })

export type DatasetVersionConversionError = typeof DatasetVersionConversionError.Type

export const DatasetVersionNotFoundError = makeError(
  "VERSION_NOT_FOUND",
  "DatasetVersionNotFoundError"
).annotate({ httpApiStatus: 404 })

export type DatasetVersionNotFoundError = typeof DatasetVersionNotFoundError.Type

export const ForbiddenError = makeError(
  "FORBIDDEN",
  "ForbiddenError"
).annotate({ httpApiStatus: 403 })

export type ForbiddenError = typeof ForbiddenError.Type

export const InvalidDatasetOwnerPathError = makeError(
  "INVALID_DATASET_OWNER_PATH",
  "InvalidDatasetOwnerPathError"
).annotate({ httpApiStatus: 400 })

export type InvalidDatasetOwnerPathError = typeof InvalidDatasetOwnerPathError.Type

export const InvalidDatasetReferenceError = makeError(
  "INVALID_REFERENCE",
  "InvalidDatasetReferenceError"
).annotate({ httpApiStatus: 500 })

export type InvalidDatasetReferenceError = typeof InvalidDatasetReferenceError.Type

export const InvalidDatasetSelectorError = makeError(
  "INVALID_DATASET_SELECTOR",
  "InvalidDatasetSelectorError"
).annotate({ httpApiStatus: 400 })

export type InvalidDatasetSelectorError = typeof InvalidDatasetSelectorError.Type

export const InvalidManifestError = makeError(
  "INVALID_MANIFEST",
  "InvalidManifestError"
).annotate({ httpApiStatus: 400 })

export type InvalidManifestError = typeof InvalidManifestError.Type

export const InvalidManifestHashError = makeError(
  "INVALID_MANIFEST_HASH",
  "InvalidManifestHashError"
).annotate({ httpApiStatus: 500 })

export type InvalidManifestHashError = typeof InvalidManifestHashError.Type

export const InvalidNamespaceError = makeError(
  "INVALID_NAMESPACE",
  "InvalidNamespaceError"
).annotate({ httpApiStatus: 400 })

export type InvalidNamespaceError = typeof InvalidNamespaceError.Type

export const InvalidQueryParametersError = makeError(
  "INVALID_QUERY_PARAMETERS",
  "InvalidQueryParametersError"
).annotate({ httpApiStatus: 400 })

export type InvalidQueryParametersError = typeof InvalidQueryParametersError.Type

export const InvalidPathParametersError = makeError(
  "INVALID_PATH_PARAMETERS",
  "InvalidPathParametersError"
).annotate({ httpApiStatus: 400 })

export type InvalidPathParametersError = typeof InvalidPathParametersError.Type

export const InvalidRequestBodyError = makeError(
  "INVALID_REQUEST_BODY",
  "InvalidRequestBodyError"
).annotate({ httpApiStatus: 400 })

export type InvalidRequestBodyError = typeof InvalidRequestBodyError.Type

export const InvalidSelectorError = makeError(
  "INVALID_SELECTOR",
  "InvalidSelectorError"
).annotate({ httpApiStatus: 400 })

export type InvalidSelectorError = typeof InvalidSelectorError.Type

export const LatestDatasetVersionNotFoundError = makeError(
  "LATEST_VERSION_NOT_FOUND",
  "LatestDatasetVersionNotFoundError"
).annotate({ httpApiStatus: 404 })

export type LatestDatasetVersionNotFoundError = typeof LatestDatasetVersionNotFoundError.Type

export const LimitInvalidError = makeError(
  "LIMIT_INVALID",
  "LimitInvalidError"
).annotate({ httpApiStatus: 400 })

export type LimitInvalidError = typeof LimitInvalidError.Type

export const LimitTooLargeError = makeError(
  "LIMIT_TOO_LARGE",
  "LimitTooLargeError"
).annotate({ httpApiStatus: 400 })

export type LimitTooLargeError = typeof LimitTooLargeError.Type

export const ManifestDeserializationError = makeError(
  "MANIFEST_DESERIALIZATION_ERROR",
  "ManifestDeserializationError"
).annotate({ httpApiStatus: 500 })

export type ManifestDeserializationError = typeof ManifestDeserializationError.Type

export const ManifestNotFoundError = makeError(
  "MANIFEST_NOT_FOUND",
  "ManifestNotFoundError"
).annotate({ httpApiStatus: 404 })

export type ManifestNotFoundError = typeof ManifestNotFoundError.Type

export const ManifestRetrievalError = makeError(
  "MANIFEST_RETRIEVAL_ERROR",
  "ManifestRetrievalError"
).annotate({ httpApiStatus: 500 })

export type ManifestRetrievalError = typeof ManifestRetrievalError.Type

export const NamespaceAccessDeniedError = makeError(
  "NAMESPACE_ACCESS_DENIED",
  "NamespaceAccessDeniedError"
).annotate({ httpApiStatus: 403 })

export type NamespaceAccessDeniedError = typeof NamespaceAccessDeniedError.Type

export const RegistryDatabaseError = makeError(
  "AMP_REGISTRY_DB_ERROR",
  "RegistryDatabaseError"
).annotate({ httpApiStatus: 500 })

export type RegistryDatabaseError = typeof RegistryDatabaseError.Type

export const SavedQueryConversionError = makeError(
  "SAVED_QUERY_CONVERSION_ERROR",
  "SavedQueryConversionError"
).annotate({ httpApiStatus: 500 })

export type SavedQueryConversionError = typeof SavedQueryConversionError.Type

export const ServiceUnavailableError = makeError(
  "SERVICE_UNAVAILABLE",
  "ServiceUnavailableError"
).annotate({ httpApiStatus: 503 })

export type ServiceUnavailableError = typeof ServiceUnavailableError.Type
