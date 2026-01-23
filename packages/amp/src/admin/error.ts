/**
 * This module contains error definitions which represent the standard error
 * responses returned by the Amp Admin API.
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
 *   "error_message": "dataset 'eth_mainnet' version '1.0.0' not found"
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
 * `DATASET_NOT_FOUND`, `METADATA_DB_ERROR`
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
  )
}

// =============================================================================
// Dataset Errors
// =============================================================================

/**
 * CatalogQualifiedTable - Table reference includes a catalog qualifier.
 *
 * Causes:
 * - SQL query contains a catalog-qualified table reference (catalog.schema.table)
 * - Only dataset-qualified tables are supported (dataset.table)
 */
export class CatalogQualifiedTableError extends Schema.Class<CatalogQualifiedTableError>(
  "Amp/AdminApi/CatalogQualifiedTableError"
)({
  ...BaseErrorFields,
  code: ErrorCode("CATALOG_QUALIFIED_TABLE")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * CatalogQualifiedFunction - Function reference includes a catalog qualifier.
 *
 * Causes:
 * - SQL query contains a catalog-qualified function reference (catalog.schema.function)
 * - Only dataset-qualified functions are supported (dataset.function)
 */
export class CatalogQualifiedFunctionError extends Schema.Class<CatalogQualifiedFunctionError>(
  "Amp/AdminApi/CatalogQualifiedFunctionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("CATALOG_QUALIFIED_FUNCTION")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * DatasetNotFound - The requested dataset does not exist.
 *
 * Causes:
 * - Dataset ID does not exist in the system
 * - Dataset has been deleted
 * - Dataset not yet registered
 */
export class DatasetNotFoundError extends Schema.Class<DatasetNotFoundError>(
  "Amp/AdminApi/DatasetNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DATASET_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * DatasetStoreError - Failure in dataset storage operations.
 *
 * Causes:
 * - File/object store retrieval failures
 * - Manifest parsing errors (TOML/JSON)
 * - Unsupported dataset kind
 * - Dataset name validation failures
 */
export class DatasetStoreError extends Schema.Class<DatasetStoreError>(
  "Amp/AdminApi/DatasetStoreError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DATASET_STORE_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * DependencyAliasNotFound - Dependency alias not found in dependencies map.
 *
 * Causes:
 * - Table reference uses an alias not provided in dependencies
 * - Function reference uses an alias not provided in dependencies
 */
export class DependencyAliasNotFoundError extends Schema.Class<DependencyAliasNotFoundError>(
  "Amp/AdminApi/DependencyAliasNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DEPENDENCY_ALIAS_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * DependencyNotFound - Dependency not found in dataset store.
 *
 * Causes:
 * - Referenced dependency does not exist in dataset store
 * - Specified version or hash cannot be found
 */
export class DependencyNotFoundError extends Schema.Class<DependencyNotFoundError>(
  "Amp/AdminApi/DependencyNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DEPENDENCY_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * DependencyResolution - Failed to resolve dependency.
 *
 * Causes:
 * - Database query fails during resolution
 */
export class DependencyResolutionError extends Schema.Class<DependencyResolutionError>(
  "Amp/AdminApi/DependencyResolutionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DEPENDENCY_RESOLUTION")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * EmptyTablesAndFunctions - No tables or functions provided.
 *
 * Causes:
 * - At least one table or function is required for schema analysis
 */
export class EmptyTablesAndFunctionsError extends Schema.Class<EmptyTablesAndFunctionsError>(
  "Amp/AdminApi/EmptyTablesAndFunctionsError"
)({
  ...BaseErrorFields,
  code: ErrorCode("EMPTY_TABLES_AND_FUNCTIONS")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * EthCallNotAvailable - eth_call function not available for dataset.
 *
 * Causes:
 * - eth_call function is referenced in SQL but dataset doesn't support it
 * - Dataset is not an EVM RPC dataset
 */
export class EthCallNotAvailableError extends Schema.Class<EthCallNotAvailableError>(
  "Amp/AdminApi/EthCallNotAvailableError"
)({
  ...BaseErrorFields,
  code: ErrorCode("ETH_CALL_NOT_AVAILABLE")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * EthCallUdfCreationError - Failed to create ETH call UDF.
 *
 * Causes:
 * - Invalid provider configuration for dataset
 * - Provider connection issues
 */
export class EthCallUdfCreationError extends Schema.Class<EthCallUdfCreationError>(
  "Amp/AdminApi/EthCallUdfCreationError"
)({
  ...BaseErrorFields,
  code: ErrorCode("ETH_CALL_UDF_CREATION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * FunctionNotFoundInDataset - Function not found in referenced dataset.
 *
 * Causes:
 * - SQL query references a function that doesn't exist in the dataset
 * - Function name is misspelled
 */
export class FunctionNotFoundInDatasetError extends Schema.Class<FunctionNotFoundInDatasetError>(
  "Amp/AdminApi/FunctionNotFoundInDatasetError"
)({
  ...BaseErrorFields,
  code: ErrorCode("FUNCTION_NOT_FOUND_IN_DATASET")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * FunctionReferenceResolution - Failed to resolve function references from SQL.
 *
 * Causes:
 * - Unsupported DML statements encountered
 */
export class FunctionReferenceResolutionError extends Schema.Class<FunctionReferenceResolutionError>(
  "Amp/AdminApi/FunctionReferenceResolutionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("FUNCTION_REFERENCE_RESOLUTION")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * GetDatasetError - Failed to retrieve dataset from store.
 *
 * Causes:
 * - Dataset manifest is invalid or corrupted
 * - Unsupported dataset kind
 * - Storage backend errors when reading dataset
 */
export class GetDatasetError extends Schema.Class<GetDatasetError>(
  "Amp/AdminApi/GetDatasetError"
)({
  ...BaseErrorFields,
  code: ErrorCode("GET_DATASET_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * GetManifestPathError - Failed to query manifest path from metadata database.
 */
export class GetManifestPathError extends Schema.Class<GetManifestPathError>(
  "Amp/AdminApi/GetManifestPathError"
)({
  ...BaseErrorFields,
  code: ErrorCode("GET_MANIFEST_PATH_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * GetSyncProgressError - Failed to retrieve the dataset sync progress
 *
 * Causes:
 * - Unable to resolve the dataset synchronization progress server side
 */
export class GetSyncProgressError extends Schema.Class<GetSyncProgressError>(
  "Amp/AdminApi/GetSyncProgressError"
)({
  ...BaseErrorFields,
  code: ErrorCode("GET_SYNC_PROGRESS_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

// =============================================================================
// Job Errors
// =============================================================================

/**
 * InvalidJobId - The provided job ID is malformed or invalid.
 *
 * Causes:
 * - Job ID contains invalid characters
 * - Job ID format does not match expected pattern
 */
export class InvalidJobIdError extends Schema.Class<InvalidJobIdError>(
  "Amp/AdminApi/InvalidJobIdError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_JOB_ID")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * JobNotFound - The requested job does not exist.
 *
 * Causes:
 * - Job ID does not exist in the system
 * - Job has been deleted
 */
export class JobNotFoundError extends Schema.Class<JobNotFoundError>(
  "Amp/AdminApi/JobNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("JOB_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * JobConflict - Job exists but cannot be deleted (not in terminal state).
 */
export class JobConflictError extends Schema.Class<JobConflictError>(
  "Amp/AdminApi/JobConflictError"
)({
  ...BaseErrorFields,
  code: ErrorCode("JOB_CONFLICT")
}, HttpApiSchema.annotations({ status: 409 })) {}

/**
 * GetJobError - Failed to retrieve job from scheduler.
 */
export class GetJobError extends Schema.Class<GetJobError>(
  "Amp/AdminApi/GetJobError"
)({
  ...BaseErrorFields,
  code: ErrorCode("GET_JOB_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * DeleteJobError - Failed to delete job from scheduler.
 */
export class DeleteJobError extends Schema.Class<DeleteJobError>(
  "Amp/AdminApi/DeleteJobError"
)({
  ...BaseErrorFields,
  code: ErrorCode("DELETE_JOB_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * StopJobError - Database error during stop operation.
 */
export class StopJobError extends Schema.Class<StopJobError>(
  "Amp/AdminApi/StopJobError"
)({
  ...BaseErrorFields,
  code: ErrorCode("STOP_JOB_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ListJobsError - Failed to list jobs from scheduler.
 */
export class ListJobsError extends Schema.Class<ListJobsError>(
  "Amp/AdminApi/ListJobsError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIST_JOBS_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * UnexpectedStateConflict - Internal state machine error.
 */
export class UnexpectedStateConflictError extends Schema.Class<UnexpectedStateConflictError>(
  "Amp/AdminApi/UnexpectedStateConflictError"
)({
  ...BaseErrorFields,
  code: ErrorCode("UNEXPECTED_STATE_CONFLICT")
}, HttpApiSchema.annotations({ status: 500 })) {}

// =============================================================================
// Manifest Errors
// =============================================================================

/**
 * InvalidManifest - Dataset manifest is semantically invalid.
 *
 * Causes:
 * - Invalid dataset references in SQL views
 * - Circular dependencies between datasets
 * - Schema validation failures
 */
export class InvalidManifestError extends Schema.Class<InvalidManifestError>(
  "Amp/AdminApi/InvalidManifestError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_MANIFEST")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * ManifestLinkingError - Failed to link manifest to dataset.
 */
export class ManifestLinkingError extends Schema.Class<ManifestLinkingError>(
  "Amp/AdminApi/ManifestLinkingError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_LINKING_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ManifestNotFound - Manifest with the provided hash not found.
 */
export class ManifestNotFoundError extends Schema.Class<ManifestNotFoundError>(
  "Amp/AdminApi/ManifestNotFoundError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_NOT_FOUND")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * ManifestRegistrationError - Failed to register manifest in the system.
 */
export class ManifestRegistrationError extends Schema.Class<ManifestRegistrationError>(
  "Amp/AdminApi/ManifestRegistrationError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_REGISTRATION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ManifestValidationError - Manifest validation error.
 *
 * Causes:
 * - SQL queries contain non-incremental operations
 * - Invalid table references in SQL
 * - Type inference errors
 */
export class ManifestValidationError extends Schema.Class<ManifestValidationError>(
  "Amp/AdminApi/ManifestValidationError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_VALIDATION_ERROR")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * ManifestStorageError - Failed to write manifest to object store.
 */
export class ManifestStorageError extends Schema.Class<ManifestStorageError>(
  "Amp/AdminApi/ManifestStorageError"
)({
  ...BaseErrorFields,
  code: ErrorCode("MANIFEST_STORAGE_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ParseManifestError - Failed to parse manifest JSON.
 */
export class ParseManifestError extends Schema.Class<ParseManifestError>(
  "Amp/AdminApi/ParseManifestError"
)({
  ...BaseErrorFields,
  code: ErrorCode("PARSE_MANIFEST_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ReadManifestError - Failed to read manifest from object store.
 */
export class ReadManifestError extends Schema.Class<ReadManifestError>(
  "Amp/AdminApi/ReadManifestError"
)({
  ...BaseErrorFields,
  code: ErrorCode("READ_MANIFEST_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

// =============================================================================
// Request Validation Errors
// =============================================================================

/**
 * InvalidPath - Invalid path parameters.
 */
export class InvalidPathError extends Schema.Class<InvalidPathError>(
  "Amp/AdminApi/InvalidPathError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_PATH")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidBody - Invalid request body.
 */
export class InvalidBodyError extends Schema.Class<InvalidBodyError>(
  "Amp/AdminApi/InvalidBodyError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_BODY")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidPathParams - Invalid request path parameters.
 */
export class InvalidPathParamsError extends Schema.Class<InvalidPathParamsError>(
  "Amp/AdminApi/InvalidPathParamsError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_PATH_PARAMS")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidPayloadFormat - Invalid request payload format.
 */
export class InvalidPayloadFormatError extends Schema.Class<InvalidPayloadFormatError>(
  "Amp/AdminApi/InvalidPayloadFormatError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_PAYLOAD_FORMAT")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidQueryParameters - Invalid query parameters.
 */
export class InvalidQueryParametersError extends Schema.Class<InvalidQueryParametersError>(
  "Amp/AdminApi/InvalidQueryParametersError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_QUERY_PARAMETERS")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidRequest - The request is malformed or contains invalid parameters.
 */
export class InvalidRequestError extends Schema.Class<InvalidRequestError>(
  "Amp/AdminApi/InvalidRequestError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_REQUEST")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidSelector - The provided dataset selector is malformed or invalid.
 */
export class InvalidSelectorError extends Schema.Class<InvalidSelectorError>(
  "Amp/AdminApi/InvalidSelectorError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_SELECTOR")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidTableName - Table name does not conform to SQL identifier rules.
 */
export class InvalidTableNameError extends Schema.Class<InvalidTableNameError>(
  "Amp/AdminApi/InvalidTableNameError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_TABLE_NAME")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidTableSql - SQL syntax error in table definition.
 */
export class InvalidTableSqlError extends Schema.Class<InvalidTableSqlError>(
  "Amp/AdminApi/InvalidTableSqlError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_TABLE_SQL")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidDependencyAliasForTableRef - Invalid dependency alias in table reference.
 */
export class InvalidDependencyAliasForTableRefError extends Schema.Class<InvalidDependencyAliasForTableRefError>(
  "Amp/AdminApi/InvalidDependencyAliasForTableRefError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_DEPENDENCY_ALIAS_FOR_TABLE_REF")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * InvalidDependencyAliasForFunctionRef - Invalid dependency alias in function reference.
 */
export class InvalidDependencyAliasForFunctionRefError extends Schema.Class<InvalidDependencyAliasForFunctionRefError>(
  "Amp/AdminApi/InvalidDependencyAliasForFunctionRefError"
)({
  ...BaseErrorFields,
  code: ErrorCode("INVALID_DEPENDENCY_ALIAS_FOR_FUNCTION_REF")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * LimitTooLarge - The requested limit exceeds the maximum allowed value.
 */
export class LimitTooLargeError extends Schema.Class<LimitTooLargeError>(
  "Amp/AdminApi/LimitTooLargeError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIMIT_TOO_LARGE")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * LimitInvalid - The requested limit is invalid (zero).
 */
export class LimitInvalidError extends Schema.Class<LimitInvalidError>(
  "Amp/AdminApi/LimitInvalidError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIMIT_INVALID")
}, HttpApiSchema.annotations({ status: 400 })) {}

// =============================================================================
// Database Errors
// =============================================================================

/**
 * MetadataDbError - Database operation failure in the metadata PostgreSQL database.
 */
export class MetadataDbError extends Schema.Class<MetadataDbError>(
  "Amp/AdminApi/MetadataDbError"
)({
  ...BaseErrorFields,
  code: ErrorCode("METADATA_DB_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * PhysicalTableError - Failed to access the physical table metadata.
 */
export class PhysicalTableError extends Schema.Class<PhysicalTableError>(
  "Amp/AdminApi/PhysicalTableError"
)({
  ...BaseErrorFields,
  code: ErrorCode("PHYSICAL_TABLE_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ResolveRevisionError - Failed to resolve the dataset revision.
 */
export class ResolveRevisionError extends Schema.Class<ResolveRevisionError>(
  "Amp/AdminApi/ResolveRevisionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("RESOLVE_REVISION_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

// =============================================================================
// Query/Schema Errors
// =============================================================================

/**
 * NonIncrementalQuery - SQL query contains non-incremental operations.
 *
 * Causes:
 * - SQL contains LIMIT, ORDER BY, GROUP BY, DISTINCT, window functions
 * - SQL uses outer joins
 */
export class NonIncrementalQueryError extends Schema.Class<NonIncrementalQueryError>(
  "Amp/AdminApi/NonIncrementalQueryError"
)({
  ...BaseErrorFields,
  code: ErrorCode("NON_INCREMENTAL_QUERY")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * SchemaInference - Failed to infer output schema from query.
 */
export class SchemaInferenceError extends Schema.Class<SchemaInferenceError>(
  "Amp/AdminApi/SchemaInferenceError"
)({
  ...BaseErrorFields,
  code: ErrorCode("SCHEMA_INFERENCE")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * TableNotFoundInDataset - Table not found in dataset.
 */
export class TableNotFoundInDatasetError extends Schema.Class<TableNotFoundInDatasetError>(
  "Amp/AdminApi/TableNotFoundInDatasetError"
)({
  ...BaseErrorFields,
  code: ErrorCode("TABLE_NOT_FOUND_IN_DATASET")
}, HttpApiSchema.annotations({ status: 404 })) {}

/**
 * TableReferenceResolution - Failed to extract table references from SQL.
 */
export class TableReferenceResolutionError extends Schema.Class<TableReferenceResolutionError>(
  "Amp/AdminApi/TableReferenceResolutionError"
)({
  ...BaseErrorFields,
  code: ErrorCode("TABLE_REFERENCE_RESOLUTION")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * UnqualifiedTable - Table reference is not qualified with a dataset.
 */
export class UnqualifiedTableError extends Schema.Class<UnqualifiedTableError>(
  "Amp/AdminApi/UnqualifiedTableError"
)({
  ...BaseErrorFields,
  code: ErrorCode("UNQUALIFIED_TABLE")
}, HttpApiSchema.annotations({ status: 400 })) {}

// =============================================================================
// Scheduler/Worker Errors
// =============================================================================

/**
 * SchedulerError - Indicates a failure in the job scheduling system.
 */
export class SchedulerError extends Schema.Class<SchedulerError>(
  "Amp/AdminApi/SchedulerError"
)({
  ...BaseErrorFields,
  code: ErrorCode("SCHEDULER_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * WorkerNotAvailable - Specified worker not found or inactive.
 */
export class WorkerNotAvailableError extends Schema.Class<WorkerNotAvailableError>(
  "Amp/AdminApi/WorkerNotAvailableError"
)({
  ...BaseErrorFields,
  code: ErrorCode("WORKER_NOT_AVAILABLE")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * SchedulerListWorkersError - Failed to list workers from the scheduler.
 */
export class SchedulerListWorkersError extends Schema.Class<SchedulerListWorkersError>(
  "Amp/AdminApi/SchedulerListWorkersError"
)({
  ...BaseErrorFields,
  code: ErrorCode("SCHEDULER_LIST_WORKERS_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

// =============================================================================
// Store Errors
// =============================================================================

/**
 * StoreError - Dataset store operation error.
 */
export class StoreError extends Schema.Class<StoreError>(
  "Amp/AdminApi/StoreError"
)({
  ...BaseErrorFields,
  code: ErrorCode("STORE_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * UnsupportedDatasetKind - Dataset kind is not supported.
 */
export class UnsupportedDatasetKindError extends Schema.Class<UnsupportedDatasetKindError>(
  "Amp/AdminApi/UnsupportedDatasetKindError"
)({
  ...BaseErrorFields,
  code: ErrorCode("UNSUPPORTED_DATASET_KIND")
}, HttpApiSchema.annotations({ status: 400 })) {}

/**
 * VersionTaggingError - Failed to tag version for the dataset.
 */
export class VersionTaggingError extends Schema.Class<VersionTaggingError>(
  "Amp/AdminApi/VersionTaggingError"
)({
  ...BaseErrorFields,
  code: ErrorCode("VERSION_TAGGING_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ListAllDatasetsError - Failed to list all datasets from dataset store.
 */
export class ListAllDatasetsError extends Schema.Class<ListAllDatasetsError>(
  "Amp/AdminApi/ListAllDatasetsError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIST_ALL_DATASETS_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}

/**
 * ListVersionTagsError - Failed to list version tags from dataset store.
 */
export class ListVersionTagsError extends Schema.Class<ListVersionTagsError>(
  "Amp/AdminApi/ListVersionTagsError"
)({
  ...BaseErrorFields,
  code: ErrorCode("LIST_VERSION_TAGS_ERROR")
}, HttpApiSchema.annotations({ status: 500 })) {}
