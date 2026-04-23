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
       * `DATASET_NOT_FOUND`, `METADATA_DB_ERROR`
       */
      readonly code: Schema.withConstructorDefault<Schema.Literal<Code>>
      /**
       * Human-readable error message
       *
       * Messages provide detailed context about the error but may change
       * over time. Use `error_code` for programmatic decisions.
       */
      readonly message: Schema.String
    }
  >,
  { readonly code: "error_code"; readonly message: "error_message" }
> =>
  Schema.Struct({
    ...fields,
    _tag: Schema.tagDefaultOmit(tag),
    /**
     * Machine-readable error code in SCREAMING_SNAKE_CASE format
     *
     * Error codes are stable across API versions and should be used
     * for programmatic error handling. Examples: `INVALID_SELECTOR`,
     * `DATASET_NOT_FOUND`, `METADATA_DB_ERROR`
     */
    code: Schema.Literal(code),
    /**
     * Human-readable error message
     *
     * Messages provide detailed context about the error but may change
     * over time. Use `error_code` for programmatic decisions.
     */
    message: Schema.String
  }).pipe(Schema.encodeKeys({
    code: "error_code",
    message: "error_message"
  })) as any

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
export const CatalogQualifiedTableError = makeError(
  "CATALOG_QUALIFIED_TABLE",
  "CatalogQualifiedTableError"
).annotate({ httpApiStatus: 400 })

export type CatalogQualifiedTableError = typeof CatalogQualifiedTableError.Type

/**
 * CatalogQualifiedFunction - Function reference includes a catalog qualifier.
 *
 * Causes:
 * - SQL query contains a catalog-qualified function reference (catalog.schema.function)
 * - Only dataset-qualified functions are supported (dataset.function)
 */
export const CatalogQualifiedFunctionError = makeError(
  "CATALOG_QUALIFIED_FUNCTION",
  "CatalogQualifiedFunctionError"
).annotate({ httpApiStatus: 400 })

export type CatalogQualifiedFunctionError = typeof CatalogQualifiedFunctionError.Type

/**
 * DatasetNotFound - The requested dataset does not exist.
 *
 * Causes:
 * - Dataset ID does not exist in the system
 * - Dataset has been deleted
 * - Dataset not yet registered
 */
export const DatasetNotFoundError = makeError(
  "DATASET_NOT_FOUND",
  "DatasetNotFoundError"
).annotate({ httpApiStatus: 404 })

export type DatasetNotFoundError = typeof DatasetNotFoundError.Type

/**
 * DatasetStoreError - Failure in dataset storage operations.
 *
 * Causes:
 * - File/object store retrieval failures
 * - Manifest parsing errors (TOML/JSON)
 * - Unsupported dataset kind
 * - Dataset name validation failures
 */
export const DatasetStoreError = makeError(
  "DATASET_STORE_ERROR",
  "DatasetStoreError"
).annotate({ httpApiStatus: 500 })

export type DatasetStoreError = typeof DatasetStoreError.Type

/**
 * DependencyAliasNotFound - Dependency alias not found in dependencies map.
 *
 * Causes:
 * - Table reference uses an alias not provided in dependencies
 * - Function reference uses an alias not provided in dependencies
 */
export const DependencyAliasNotFoundError = makeError(
  "DEPENDENCY_ALIAS_NOT_FOUND",
  "DependencyAliasNotFoundError"
).annotate({ httpApiStatus: 400 })

export type DependencyAliasNotFoundError = typeof DependencyAliasNotFoundError.Type

/**
 * DependencyNotFound - Dependency not found in dataset store.
 *
 * Causes:
 * - Referenced dependency does not exist in dataset store
 * - Specified version or hash cannot be found
 */
export const DependencyNotFoundError = makeError(
  "DEPENDENCY_NOT_FOUND",
  "DependencyNotFoundError"
).annotate({ httpApiStatus: 404 })

export type DependencyNotFoundError = typeof DependencyNotFoundError.Type

/**
 * DependencyResolution - Failed to resolve dependency.
 *
 * Causes:
 * - Database query fails during resolution
 */
export const DependencyResolutionError = makeError(
  "DEPENDENCY_RESOLUTION",
  "DependencyResolutionError"
).annotate({ httpApiStatus: 500 })

export type DependencyResolutionError = typeof DependencyResolutionError.Type

/**
 * EmptyTablesAndFunctions - No tables or functions provided.
 *
 * Causes:
 * - At least one table or function is required for schema analysis
 */
export const EmptyTablesAndFunctionsError = makeError(
  "EMPTY_TABLES_AND_FUNCTIONS",
  "EmptyTablesAndFunctionsError"
).annotate({ httpApiStatus: 400 })

export type EmptyTablesAndFunctionsError = typeof EmptyTablesAndFunctionsError.Type

/**
 * EthCallNotAvailable - eth_call function not available for dataset.
 *
 * Causes:
 * - eth_call function is referenced in SQL but dataset doesn't support it
 * - Dataset is not an EVM RPC dataset
 */
export const EthCallNotAvailableError = makeError(
  "ETH_CALL_NOT_AVAILABLE",
  "EthCallNotAvailableError"
).annotate({ httpApiStatus: 404 })

export type EthCallNotAvailableError = typeof EthCallNotAvailableError.Type

/**
 * EthCallUdfCreationError - Failed to create ETH call UDF.
 *
 * Causes:
 * - Invalid provider configuration for dataset
 * - Provider connection issues
 */
export const EthCallUdfCreationError = makeError(
  "ETH_CALL_UDF_CREATION_ERROR",
  "EthCallUdfCreationError"
).annotate({ httpApiStatus: 500 })

export type EthCallUdfCreationError = typeof EthCallUdfCreationError.Type

/**
 * FunctionNotFoundInDataset - Function not found in referenced dataset.
 *
 * Causes:
 * - SQL query references a function that doesn't exist in the dataset
 * - Function name is misspelled
 */
export const FunctionNotFoundInDatasetError = makeError(
  "FUNCTION_NOT_FOUND_IN_DATASET",
  "FunctionNotFoundInDatasetError"
).annotate({ httpApiStatus: 404 })

export type FunctionNotFoundInDatasetError = typeof FunctionNotFoundInDatasetError.Type

/**
 * FunctionReferenceResolution - Failed to resolve function references from SQL.
 *
 * Causes:
 * - Unsupported DML statements encountered
 */
export const FunctionReferenceResolutionError = makeError(
  "FUNCTION_REFERENCE_RESOLUTION",
  "FunctionReferenceResolutionError"
).annotate({ httpApiStatus: 500 })

export type FunctionReferenceResolutionError = typeof FunctionReferenceResolutionError.Type

/**
 * GetDatasetError - Failed to retrieve dataset from store.
 *
 * Causes:
 * - Dataset manifest is invalid or corrupted
 * - Unsupported dataset kind
 * - Storage backend errors when reading dataset
 */
export const GetDatasetError = makeError(
  "GET_DATASET_ERROR",
  "GetDatasetError"
).annotate({ httpApiStatus: 500 })

export type GetDatasetError = typeof GetDatasetError.Type

/**
 * GetManifestPathError - Failed to query manifest path from metadata database.
 */
export const GetManifestPathError = makeError(
  "GET_MANIFEST_PATH_ERROR",
  "GetManifestPathError"
).annotate({ httpApiStatus: 500 })

export type GetManifestPathError = typeof GetManifestPathError.Type

/**
 * GetSyncProgressError - Failed to retrieve the dataset sync progress
 *
 * Causes:
 * - Unable to resolve the dataset synchronization progress server side
 */
export const GetSyncProgressError = makeError(
  "GET_SYNC_PROGRESS_ERROR",
  "GetSyncProgressError"
).annotate({ httpApiStatus: 500 })

export type GetSyncProgressError = typeof GetSyncProgressError.Type

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
export const InvalidJobIdError = makeError(
  "INVALID_JOB_ID",
  "InvalidJobIdError"
).annotate({ httpApiStatus: 400 })

export type InvalidJobIdError = typeof InvalidJobIdError.Type

/**
 * JobNotFound - The requested job does not exist.
 *
 * Causes:
 * - Job ID does not exist in the system
 * - Job has been deleted
 */
export const JobNotFoundError = makeError(
  "JOB_NOT_FOUND",
  "JobNotFoundError"
).annotate({ httpApiStatus: 404 })

export type JobNotFoundError = typeof JobNotFoundError.Type

/**
 * JobConflict - Job exists but cannot be deleted (not in terminal state).
 */
export const JobConflictError = makeError(
  "JOB_CONFLICT",
  "JobConflictError"
).annotate({ httpApiStatus: 409 })

export type JobConflictError = typeof JobConflictError.Type

/**
 * GetJobError - Failed to retrieve job from scheduler.
 */
export const GetJobError = makeError(
  "GET_JOB_ERROR",
  "GetJobError"
).annotate({ httpApiStatus: 500 })

export type GetJobError = typeof GetJobError.Type

/**
 * DeleteJobError - Failed to delete job from scheduler.
 */
export const DeleteJobError = makeError(
  "DELETE_JOB_ERROR",
  "DeleteJobError"
).annotate({ httpApiStatus: 500 })

export type DeleteJobError = typeof DeleteJobError.Type

/**
 * StopJobError - Database error during stop operation.
 */
export const StopJobError = makeError(
  "STOP_JOB_ERROR",
  "StopJobError"
).annotate({ httpApiStatus: 500 })

export type StopJobError = typeof StopJobError.Type

/**
 * ListJobsError - Failed to list jobs from scheduler.
 */
export const ListJobsError = makeError(
  "LIST_JOBS_ERROR",
  "ListJobsError"
).annotate({ httpApiStatus: 500 })

export type ListJobsError = typeof ListJobsError.Type

/**
 * UnexpectedStateConflict - Internal state machine error.
 */
export const UnexpectedStateConflictError = makeError(
  "UNEXPECTED_STATE_CONFLICT",
  "UnexpectedStateConflictError"
).annotate({ httpApiStatus: 500 })

export type UnexpectedStateConflictError = typeof UnexpectedStateConflictError.Type

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
export const InvalidManifestError = makeError(
  "INVALID_MANIFEST",
  "InvalidManifestError"
).annotate({ httpApiStatus: 400 })

export type InvalidManifestError = typeof InvalidManifestError.Type

/**
 * ManifestLinkingError - Failed to link manifest to dataset.
 */
export const ManifestLinkingError = makeError(
  "MANIFEST_LINKING_ERROR",
  "ManifestLinkingError"
).annotate({ httpApiStatus: 500 })

export type ManifestLinkingError = typeof ManifestLinkingError.Type

/**
 * ManifestNotFound - Manifest with the provided hash not found.
 */
export const ManifestNotFoundError = makeError(
  "MANIFEST_NOT_FOUND",
  "ManifestNotFoundError"
).annotate({ httpApiStatus: 404 })

export type ManifestNotFoundError = typeof ManifestNotFoundError.Type

/**
 * ManifestRegistrationError - Failed to register manifest in the system.
 */
export const ManifestRegistrationError = makeError(
  "MANIFEST_REGISTRATION_ERROR",
  "ManifestRegistrationError"
).annotate({ httpApiStatus: 500 })

export type ManifestRegistrationError = typeof ManifestRegistrationError.Type

/**
 * ManifestValidationError - Manifest validation error.
 *
 * Causes:
 * - SQL queries contain non-incremental operations
 * - Invalid table references in SQL
 * - Type inference errors
 */
export const ManifestValidationError = makeError(
  "MANIFEST_VALIDATION_ERROR",
  "ManifestValidationError"
).annotate({ httpApiStatus: 400 })

export type ManifestValidationError = typeof ManifestValidationError.Type

/**
 * ManifestStorageError - Failed to write manifest to object store.
 */
export const ManifestStorageError = makeError(
  "MANIFEST_STORAGE_ERROR",
  "ManifestStorageError"
).annotate({ httpApiStatus: 500 })

export type ManifestStorageError = typeof ManifestStorageError.Type

/**
 * ParseManifestError - Failed to parse manifest JSON.
 */
export const ParseManifestError = makeError(
  "PARSE_MANIFEST_ERROR",
  "ParseManifestError"
).annotate({ httpApiStatus: 500 })

export type ParseManifestError = typeof ParseManifestError.Type

/**
 * ReadManifestError - Failed to read manifest from object store.
 */
export const ReadManifestError = makeError(
  "READ_MANIFEST_ERROR",
  "ReadManifestError"
).annotate({ httpApiStatus: 500 })

export type ReadManifestError = typeof ReadManifestError.Type

// =============================================================================
// Request Validation Errors
// =============================================================================

/**
 * InvalidPath - Invalid path parameters.
 */
export const InvalidPathError = makeError(
  "INVALID_PATH",
  "InvalidPathError"
).annotate({ httpApiStatus: 400 })

export type InvalidPathError = typeof InvalidPathError.Type

/**
 * InvalidBody - Invalid request body.
 */
export const InvalidBodyError = makeError(
  "INVALID_BODY",
  "InvalidBodyError"
).annotate({ httpApiStatus: 400 })

export type InvalidBodyError = typeof InvalidBodyError.Type

/**
 * InvalidPathParams - Invalid request path parameters.
 */
export const InvalidPathParamsError = makeError(
  "INVALID_PATH_PARAMS",
  "InvalidPathParamsError"
).annotate({ httpApiStatus: 400 })

export type InvalidPathParamsError = typeof InvalidPathParamsError.Type

/**
 * InvalidPayloadFormat - Invalid request payload format.
 */
export const InvalidPayloadFormatError = makeError(
  "INVALID_PAYLOAD_FORMAT",
  "InvalidPayloadFormatError"
).annotate({ httpApiStatus: 400 })

export type InvalidPayloadFormatError = typeof InvalidPayloadFormatError.Type

/**
 * InvalidQueryParameters - Invalid query parameters.
 */
export const InvalidQueryParametersError = makeError(
  "INVALID_QUERY_PARAMETERS",
  "InvalidQueryParametersError"
).annotate({ httpApiStatus: 400 })

export type InvalidQueryParametersError = typeof InvalidQueryParametersError.Type

/**
 * InvalidRequest - The request is malformed or contains invalid parameters.
 */
export const InvalidRequestError = makeError(
  "INVALID_REQUEST",
  "InvalidRequestError"
).annotate({ httpApiStatus: 400 })

export type InvalidRequestError = typeof InvalidRequestError.Type

/**
 * InvalidSelector - The provided dataset selector is malformed or invalid.
 */
export const InvalidSelectorError = makeError(
  "INVALID_SELECTOR",
  "InvalidSelectorError"
).annotate({ httpApiStatus: 400 })

export type InvalidSelectorError = typeof InvalidSelectorError.Type

/**
 * InvalidTableName - Table name does not conform to SQL identifier rules.
 */
export const InvalidTableNameError = makeError(
  "INVALID_TABLE_NAME",
  "InvalidTableNameError"
).annotate({ httpApiStatus: 400 })

export type InvalidTableNameError = typeof InvalidTableNameError.Type

/**
 * InvalidTableSql - SQL syntax error in table definition.
 */
export const InvalidTableSqlError = makeError(
  "INVALID_TABLE_SQL",
  "InvalidTableSqlError"
).annotate({ httpApiStatus: 400 })

export type InvalidTableSqlError = typeof InvalidTableSqlError.Type

/**
 * InvalidDependencyAliasForTableRef - Invalid dependency alias in table reference.
 */
export const InvalidDependencyAliasForTableRefError = makeError(
  "INVALID_DEPENDENCY_ALIAS_FOR_TABLE_REF",
  "InvalidDependencyAliasForTableRefError"
).annotate({ httpApiStatus: 400 })

export type InvalidDependencyAliasForTableRefError = typeof InvalidDependencyAliasForTableRefError.Type

/**
 * InvalidDependencyAliasForFunctionRef - Invalid dependency alias in function reference.
 */
export const InvalidDependencyAliasForFunctionRefError = makeError(
  "INVALID_DEPENDENCY_ALIAS_FOR_FUNCTION_REF",
  "InvalidDependencyAliasForFunctionRefError"
).annotate({ httpApiStatus: 400 })

export type InvalidDependencyAliasForFunctionRefError = typeof InvalidDependencyAliasForFunctionRefError.Type

/**
 * LimitTooLarge - The requested limit exceeds the maximum allowed value.
 */
export const LimitTooLargeError = makeError(
  "LIMIT_TOO_LARGE",
  "LimitTooLargeError"
).annotate({ httpApiStatus: 400 })

export type LimitTooLargeError = typeof LimitTooLargeError.Type

/**
 * LimitInvalid - The requested limit is invalid (zero).
 */
export const LimitInvalidError = makeError(
  "LIMIT_INVALID",
  "LimitInvalidError"
).annotate({ httpApiStatus: 400 })

export type LimitInvalidError = typeof LimitInvalidError.Type

// =============================================================================
// Database Errors
// =============================================================================

/**
 * MetadataDbError - Database operation failure in the metadata PostgreSQL database.
 */
export const MetadataDbError = makeError(
  "METADATA_DB_ERROR",
  "MetadataDbError"
).annotate({ httpApiStatus: 500 })

export type MetadataDbError = typeof MetadataDbError.Type

/**
 * PhysicalTableError - Failed to access the physical table metadata.
 */
export const PhysicalTableError = makeError(
  "PHYSICAL_TABLE_ERROR",
  "PhysicalTableError"
).annotate({ httpApiStatus: 500 })

export type PhysicalTableError = typeof PhysicalTableError.Type

/**
 * ResolveRevisionError - Failed to resolve the dataset revision.
 */
export const ResolveRevisionError = makeError(
  "RESOLVE_REVISION_ERROR",
  "ResolveRevisionError"
).annotate({ httpApiStatus: 500 })

export type ResolveRevisionError = typeof ResolveRevisionError.Type

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
export const NonIncrementalQueryError = makeError(
  "NON_INCREMENTAL_QUERY",
  "NonIncrementalQueryError"
).annotate({ httpApiStatus: 400 })

export type NonIncrementalQueryError = typeof NonIncrementalQueryError.Type

/**
 * SchemaInference - Failed to infer output schema from query.
 */
export const SchemaInferenceError = makeError(
  "SCHEMA_INFERENCE",
  "SchemaInferenceError"
).annotate({ httpApiStatus: 500 })

export type SchemaInferenceError = typeof SchemaInferenceError.Type

/**
 * TableNotFoundInDataset - Table not found in dataset.
 */
export const TableNotFoundInDatasetError = makeError(
  "TABLE_NOT_FOUND_IN_DATASET",
  "TableNotFoundInDatasetError"
).annotate({ httpApiStatus: 404 })

export type TableNotFoundInDatasetError = typeof TableNotFoundInDatasetError.Type

/**
 * TableReferenceResolution - Failed to extract table references from SQL.
 */
export const TableReferenceResolutionError = makeError(
  "TABLE_REFERENCE_RESOLUTION",
  "TableReferenceResolutionError"
).annotate({ httpApiStatus: 400 })

export type TableReferenceResolutionError = typeof TableReferenceResolutionError.Type

/**
 * UnqualifiedTable - Table reference is not qualified with a dataset.
 */
export const UnqualifiedTableError = makeError(
  "UNQUALIFIED_TABLE",
  "UnqualifiedTableError"
).annotate({ httpApiStatus: 400 })

export type UnqualifiedTableError = typeof UnqualifiedTableError.Type

// =============================================================================
// Scheduler/Worker Errors
// =============================================================================

/**
 * SchedulerError - Indicates a failure in the job scheduling system.
 */
export const SchedulerError = makeError(
  "SCHEDULER_ERROR",
  "SchedulerError"
).annotate({ httpApiStatus: 500 })

export type SchedulerError = typeof SchedulerError.Type

/**
 * WorkerNotAvailable - Specified worker not found or inactive.
 */
export const WorkerNotAvailableError = makeError(
  "WORKER_NOT_AVAILABLE",
  "WorkerNotAvailableError"
).annotate({ httpApiStatus: 400 })

export type WorkerNotAvailableError = typeof WorkerNotAvailableError.Type

/**
 * SchedulerListWorkersError - Failed to list workers from the scheduler.
 */
export const SchedulerListWorkersError = makeError(
  "SCHEDULER_LIST_WORKERS_ERROR",
  "SchedulerListWorkersError"
).annotate({ httpApiStatus: 500 })

export type SchedulerListWorkersError = typeof SchedulerListWorkersError.Type

// =============================================================================
// Store Errors
// =============================================================================

/**
 * StoreError - Dataset store operation error.
 */
export const StoreError = makeError(
  "STORE_ERROR",
  "StoreError"
).annotate({ httpApiStatus: 500 })

export type StoreError = typeof StoreError.Type

/**
 * UnsupportedDatasetKind - Dataset kind is not supported.
 */
export const UnsupportedDatasetKindError = makeError(
  "UNSUPPORTED_DATASET_KIND",
  "UnsupportedDatasetKindError"
).annotate({ httpApiStatus: 400 })

export type UnsupportedDatasetKindError = typeof UnsupportedDatasetKindError.Type

/**
 * VersionTaggingError - Failed to tag version for the dataset.
 */
export const VersionTaggingError = makeError(
  "VERSION_TAGGING_ERROR",
  "VersionTaggingError"
).annotate({ httpApiStatus: 500 })

export type VersionTaggingError = typeof VersionTaggingError.Type

/**
 * ListAllDatasetsError - Failed to list all datasets from dataset store.
 */
export const ListAllDatasetsError = makeError(
  "LIST_ALL_DATASETS_ERROR",
  "ListAllDatasetsError"
).annotate({ httpApiStatus: 500 })

export type ListAllDatasetsError = typeof ListAllDatasetsError.Type

/**
 * ListVersionTagsError - Failed to list version tags from dataset store.
 */
export const ListVersionTagsError = makeError(
  "LIST_VERSION_TAGS_ERROR",
  "ListVersionTagsError"
).annotate({ httpApiStatus: 500 })

export type ListVersionTagsError = typeof ListVersionTagsError.Type
