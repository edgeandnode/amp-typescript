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
>(
  code: Code,
  tag: Tag,
  fields?: Fields
): Schema.encodeKeys<
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
  }).pipe(
    Schema.encodeKeys({
      code: "error_code",
      message: "error_message"
    })
  ) as any

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
export const CatalogQualifiedTableError = makeError("CATALOG_QUALIFIED_TABLE", "CatalogQualifiedTableError").annotate({
  httpApiStatus: 400
})

export type CatalogQualifiedTableError = typeof CatalogQualifiedTableError.Type

/**
 * DatasetNotFound - The requested dataset does not exist.
 *
 * Causes:
 * - Dataset ID does not exist in the system
 * - Dataset has been deleted
 * - Dataset not yet registered
 */
export const DatasetNotFoundError = makeError("DATASET_NOT_FOUND", "DatasetNotFoundError").annotate({
  httpApiStatus: 404
})

export type DatasetNotFoundError = typeof DatasetNotFoundError.Type

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
export const DependencyNotFoundError = makeError("DEPENDENCY_NOT_FOUND", "DependencyNotFoundError").annotate({
  httpApiStatus: 404
})

export type DependencyNotFoundError = typeof DependencyNotFoundError.Type

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
 * GetDatasetError - Failed to retrieve dataset from store.
 *
 * Causes:
 * - Dataset manifest is invalid or corrupted
 * - Unsupported dataset kind
 * - Storage backend errors when reading dataset
 */
export const GetDatasetError = makeError("GET_DATASET_ERROR", "GetDatasetError").annotate({ httpApiStatus: 500 })

export type GetDatasetError = typeof GetDatasetError.Type

/**
 * GetManifestPathError - Failed to query manifest path from metadata database.
 */
export const GetManifestPathError = makeError("GET_MANIFEST_PATH_ERROR", "GetManifestPathError").annotate({
  httpApiStatus: 500
})

export type GetManifestPathError = typeof GetManifestPathError.Type

/**
 * InvalidStoredDatasetNamespace - A stored dataset namespace is malformed.
 */
export const InvalidStoredDatasetNamespaceError = makeError(
  "INVALID_STORED_DATASET_NAMESPACE",
  "InvalidStoredDatasetNamespaceError"
).annotate({ httpApiStatus: 500 })

export type InvalidStoredDatasetNamespaceError = typeof InvalidStoredDatasetNamespaceError.Type

/**
 * InvalidStoredDatasetName - A stored dataset name is malformed.
 */
export const InvalidStoredDatasetNameError = makeError(
  "INVALID_STORED_DATASET_NAME",
  "InvalidStoredDatasetNameError"
).annotate({ httpApiStatus: 500 })

export type InvalidStoredDatasetNameError = typeof InvalidStoredDatasetNameError.Type

/**
 * InvalidStoredDatasetVersion - A stored dataset version is malformed.
 */
export const InvalidStoredDatasetVersionError = makeError(
  "INVALID_STORED_DATASET_VERSION",
  "InvalidStoredDatasetVersionError"
).annotate({ httpApiStatus: 500 })

export type InvalidStoredDatasetVersionError = typeof InvalidStoredDatasetVersionError.Type

/**
 * ListDatasetTagsError - Failed to list the tags pointing at a dataset manifest.
 */
export const ListDatasetTagsError = makeError("LIST_DATASET_TAGS_ERROR", "ListDatasetTagsError").annotate({
  httpApiStatus: 500
})

export type ListDatasetTagsError = typeof ListDatasetTagsError.Type

/**
 * NamespaceNotFound - The namespace does not exist or is unavailable to the caller.
 */
export const NamespaceNotFoundError = makeError("NAMESPACE_NOT_FOUND", "NamespaceNotFoundError").annotate({
  httpApiStatus: 404
})

export type NamespaceNotFoundError = typeof NamespaceNotFoundError.Type

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
export const InvalidJobIdError = makeError("INVALID_JOB_ID", "InvalidJobIdError").annotate({ httpApiStatus: 400 })

export type InvalidJobIdError = typeof InvalidJobIdError.Type

/**
 * JobNotFound - The requested job does not exist.
 *
 * Causes:
 * - Job ID does not exist in the system
 * - Job has been deleted
 */
export const JobNotFoundError = makeError("JOB_NOT_FOUND", "JobNotFoundError").annotate({ httpApiStatus: 404 })

export type JobNotFoundError = typeof JobNotFoundError.Type

/**
 * JobConflict - Job exists but cannot be deleted (not in terminal state).
 */
export const JobConflictError = makeError("JOB_CONFLICT", "JobConflictError").annotate({ httpApiStatus: 409 })

export type JobConflictError = typeof JobConflictError.Type

/**
 * GetJobError - Failed to retrieve job from scheduler.
 */
export const GetJobError = makeError("GET_JOB_ERROR", "GetJobError").annotate({ httpApiStatus: 500 })

export type GetJobError = typeof GetJobError.Type

/**
 * DeleteJobError - Failed to delete job from scheduler.
 */
export const DeleteJobError = makeError("DELETE_JOB_ERROR", "DeleteJobError").annotate({ httpApiStatus: 500 })

export type DeleteJobError = typeof DeleteJobError.Type

/**
 * StopJobError - Database error during stop operation.
 */
export const StopJobError = makeError("STOP_JOB_ERROR", "StopJobError").annotate({ httpApiStatus: 500 })

export type StopJobError = typeof StopJobError.Type

/**
 * ListJobsError - Failed to list jobs from scheduler.
 */
export const ListJobsError = makeError("LIST_JOBS_ERROR", "ListJobsError").annotate({ httpApiStatus: 500 })

export type ListJobsError = typeof ListJobsError.Type

/**
 * UnexpectedStateConflict - Internal state machine error.
 */
export const UnexpectedStateConflictError = makeError(
  "UNEXPECTED_STATE_CONFLICT",
  "UnexpectedStateConflictError"
).annotate({ httpApiStatus: 500 })

export type UnexpectedStateConflictError = typeof UnexpectedStateConflictError.Type

/**
 * ListJobDescriptorsError - Failed to list the descriptors of the listed jobs.
 */
export const ListJobDescriptorsError = makeError("LIST_JOB_DESCRIPTORS_ERROR", "ListJobDescriptorsError").annotate({
  httpApiStatus: 500
})

export type ListJobDescriptorsError = typeof ListJobDescriptorsError.Type

/**
 * GetDescriptorError - Failed to retrieve the descriptor of a job.
 */
export const GetDescriptorError = makeError("GET_DESCRIPTOR_ERROR", "GetDescriptorError").annotate({
  httpApiStatus: 500
})

export type GetDescriptorError = typeof GetDescriptorError.Type

/**
 * DeserializeJobDescriptorError - Failed to deserialize a job descriptor.
 */
export const DeserializeJobDescriptorError = makeError(
  "DESERIALIZE_JOB_DESCRIPTOR_ERROR",
  "DeserializeJobDescriptorError"
).annotate({ httpApiStatus: 500 })

export type DeserializeJobDescriptorError = typeof DeserializeJobDescriptorError.Type

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
export const InvalidManifestError = makeError("INVALID_MANIFEST", "InvalidManifestError").annotate({
  httpApiStatus: 400
})

export type InvalidManifestError = typeof InvalidManifestError.Type

/**
 * ManifestLinkingError - Failed to link manifest to dataset.
 */
export const ManifestLinkingError = makeError("MANIFEST_LINKING_ERROR", "ManifestLinkingError").annotate({
  httpApiStatus: 500
})

export type ManifestLinkingError = typeof ManifestLinkingError.Type

/**
 * ManifestNotFound - Manifest with the provided hash not found.
 */
export const ManifestNotFoundError = makeError("MANIFEST_NOT_FOUND", "ManifestNotFoundError").annotate({
  httpApiStatus: 404
})

export type ManifestNotFoundError = typeof ManifestNotFoundError.Type

/**
 * ManifestRegistrationError - Failed to register manifest in the system.
 */
export const ManifestRegistrationError = makeError("MANIFEST_REGISTRATION_ERROR", "ManifestRegistrationError").annotate(
  { httpApiStatus: 500 }
)

export type ManifestRegistrationError = typeof ManifestRegistrationError.Type

/**
 * ManifestValidationError - Manifest validation error.
 *
 * Causes:
 * - SQL queries contain non-incremental operations
 * - Invalid table references in SQL
 * - Type inference errors
 */
export const ManifestValidationError = makeError("MANIFEST_VALIDATION_ERROR", "ManifestValidationError").annotate({
  httpApiStatus: 400
})

export type ManifestValidationError = typeof ManifestValidationError.Type

/**
 * ManifestStorageError - Failed to write manifest to object store.
 */
export const ManifestStorageError = makeError("MANIFEST_STORAGE_ERROR", "ManifestStorageError").annotate({
  httpApiStatus: 500
})

export type ManifestStorageError = typeof ManifestStorageError.Type

/**
 * ParseManifestError - Failed to parse manifest JSON.
 */
export const ParseManifestError = makeError("PARSE_MANIFEST_ERROR", "ParseManifestError").annotate({
  httpApiStatus: 500
})

export type ParseManifestError = typeof ParseManifestError.Type

/**
 * ReadManifestError - Failed to read manifest from object store.
 */
export const ReadManifestError = makeError("READ_MANIFEST_ERROR", "ReadManifestError").annotate({ httpApiStatus: 500 })

export type ReadManifestError = typeof ReadManifestError.Type

/**
 * InvalidSortedByConfig - The manifest `sorted_by` configuration is invalid.
 */
export const InvalidSortedByConfigError = makeError("INVALID_SORTED_BY_CONFIG", "InvalidSortedByConfigError").annotate({
  httpApiStatus: 400
})

export type InvalidSortedByConfigError = typeof InvalidSortedByConfigError.Type

/**
 * ManifestSerializationError - Failed to serialize the validated manifest.
 */
export const ManifestSerializationError = makeError(
  "MANIFEST_SERIALIZATION_ERROR",
  "ManifestSerializationError"
).annotate({ httpApiStatus: 500 })

export type ManifestSerializationError = typeof ManifestSerializationError.Type

/**
 * ManifestTransactionBeginError - Failed to begin the manifest registration transaction.
 */
export const ManifestTransactionBeginError = makeError(
  "MANIFEST_TRANSACTION_BEGIN_ERROR",
  "ManifestTransactionBeginError"
).annotate({ httpApiStatus: 500 })

export type ManifestTransactionBeginError = typeof ManifestTransactionBeginError.Type

/**
 * ManifestTransactionCommitError - Failed to commit the manifest registration transaction.
 */
export const ManifestTransactionCommitError = makeError(
  "MANIFEST_TRANSACTION_COMMIT_ERROR",
  "ManifestTransactionCommitError"
).annotate({ httpApiStatus: 500 })

export type ManifestTransactionCommitError = typeof ManifestTransactionCommitError.Type

/**
 * SortedManifestTableNotFound - Internal manifest consistency failure.
 */
export const SortedManifestTableNotFoundError = makeError(
  "SORTED_MANIFEST_TABLE_NOT_FOUND",
  "SortedManifestTableNotFoundError"
).annotate({ httpApiStatus: 500 })

export type SortedManifestTableNotFoundError = typeof SortedManifestTableNotFoundError.Type

/**
 * SortedTableStatementNotFound - Internal manifest consistency failure.
 */
export const SortedTableStatementNotFoundError = makeError(
  "SORTED_TABLE_STATEMENT_NOT_FOUND",
  "SortedTableStatementNotFoundError"
).annotate({ httpApiStatus: 500 })

export type SortedTableStatementNotFoundError = typeof SortedTableStatementNotFoundError.Type

/**
 * PhaserProviderNotFound - No Phaser provider is configured for the manifest network.
 */
export const PhaserProviderNotFoundError = makeError(
  "PHASER_PROVIDER_NOT_FOUND",
  "PhaserProviderNotFoundError"
).annotate({ httpApiStatus: 400 })

export type PhaserProviderNotFoundError = typeof PhaserProviderNotFoundError.Type

/**
 * PhaserProviderParseFailed - The Phaser provider configuration is invalid.
 */
export const PhaserProviderParseFailedError = makeError(
  "PHASER_PROVIDER_PARSE_FAILED",
  "PhaserProviderParseFailedError"
).annotate({ httpApiStatus: 400 })

export type PhaserProviderParseFailedError = typeof PhaserProviderParseFailedError.Type

/**
 * PhaserProviderNetworkMismatch - The Phaser provider network does not match the manifest.
 */
export const PhaserProviderNetworkMismatchError = makeError(
  "PHASER_PROVIDER_NETWORK_MISMATCH",
  "PhaserProviderNetworkMismatchError"
).annotate({ httpApiStatus: 400 })

export type PhaserProviderNetworkMismatchError = typeof PhaserProviderNetworkMismatchError.Type

/**
 * PhaserConnectionFailed - Failed to connect to the Phaser bridge.
 */
export const PhaserConnectionFailedError = makeError(
  "PHASER_CONNECTION_FAILED",
  "PhaserConnectionFailedError"
).annotate({ httpApiStatus: 502 })

export type PhaserConnectionFailedError = typeof PhaserConnectionFailedError.Type

/**
 * PhaserDiscoveryFailed - Phaser bridge table discovery failed.
 */
export const PhaserDiscoveryFailedError = makeError("PHASER_DISCOVERY_FAILED", "PhaserDiscoveryFailedError").annotate({
  httpApiStatus: 502
})

export type PhaserDiscoveryFailedError = typeof PhaserDiscoveryFailedError.Type

/**
 * PhaserInvalidDiscovery - Phaser bridge returned invalid table metadata.
 */
export const PhaserInvalidDiscoveryError = makeError(
  "PHASER_INVALID_DISCOVERY",
  "PhaserInvalidDiscoveryError"
).annotate({ httpApiStatus: 502 })

export type PhaserInvalidDiscoveryError = typeof PhaserInvalidDiscoveryError.Type

/**
 * PhaserNoTablesDiscovered - Phaser bridge returned no usable tables.
 */
export const PhaserNoTablesDiscoveredError = makeError(
  "PHASER_NO_TABLES_DISCOVERED",
  "PhaserNoTablesDiscoveredError"
).annotate({ httpApiStatus: 502 })

export type PhaserNoTablesDiscoveredError = typeof PhaserNoTablesDiscoveredError.Type

// =============================================================================
// Request Validation Errors
// =============================================================================

/**
 * InvalidPath - Invalid path parameters.
 */
export const InvalidPathError = makeError("INVALID_PATH", "InvalidPathError").annotate({ httpApiStatus: 400 })

export type InvalidPathError = typeof InvalidPathError.Type

/**
 * InvalidPayloadFormat - Invalid request payload format.
 */
export const InvalidPayloadFormatError = makeError("INVALID_PAYLOAD_FORMAT", "InvalidPayloadFormatError").annotate({
  httpApiStatus: 400
})

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
 * InvalidTableName - Table name does not conform to SQL identifier rules.
 */
export const InvalidTableNameError = makeError("INVALID_TABLE_NAME", "InvalidTableNameError").annotate({
  httpApiStatus: 400
})

export type InvalidTableNameError = typeof InvalidTableNameError.Type

/**
 * InvalidTableSql - SQL syntax error in table definition.
 */
export const InvalidTableSqlError = makeError("INVALID_TABLE_SQL", "InvalidTableSqlError").annotate({
  httpApiStatus: 400
})

export type InvalidTableSqlError = typeof InvalidTableSqlError.Type

/**
 * LimitTooLarge - The requested limit exceeds the maximum allowed value.
 */
export const LimitTooLargeError = makeError("LIMIT_TOO_LARGE", "LimitTooLargeError").annotate({ httpApiStatus: 400 })

export type LimitTooLargeError = typeof LimitTooLargeError.Type

/**
 * LimitInvalid - The requested limit is invalid (zero).
 */
export const LimitInvalidError = makeError("LIMIT_INVALID", "LimitInvalidError").annotate({ httpApiStatus: 400 })

export type LimitInvalidError = typeof LimitInvalidError.Type

// =============================================================================
// Database Errors
// =============================================================================

/**
 * ResolveRevisionError - Failed to resolve the dataset revision.
 */
export const ResolveRevisionError = makeError("RESOLVE_REVISION_ERROR", "ResolveRevisionError").annotate({
  httpApiStatus: 500
})

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
export const NonIncrementalQueryError = makeError("NON_INCREMENTAL_QUERY", "NonIncrementalQueryError").annotate({
  httpApiStatus: 400
})

export type NonIncrementalQueryError = typeof NonIncrementalQueryError.Type

/**
 * SchemaInference - Failed to infer output schema from query.
 */
export const SchemaInferenceError = makeError("SCHEMA_INFERENCE", "SchemaInferenceError").annotate({
  httpApiStatus: 500
})

export type SchemaInferenceError = typeof SchemaInferenceError.Type

/**
 * TableNotFoundInDataset - Table not found in dataset.
 */
export const TableNotFoundInDatasetError = makeError(
  "TABLE_NOT_FOUND_IN_DATASET",
  "TableNotFoundInDatasetError"
).annotate({ httpApiStatus: 400 })

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
 * CatalogQualifiedTableInNetworkResolution - A catalog-qualified table was encountered while resolving table networks.
 */
export const CatalogQualifiedTableInNetworkResolutionError = makeError(
  "CATALOG_QUALIFIED_TABLE_IN_NETWORK_RESOLUTION",
  "CatalogQualifiedTableInNetworkResolutionError"
).annotate({ httpApiStatus: 500 })

export type CatalogQualifiedTableInNetworkResolutionError = typeof CatalogQualifiedTableInNetworkResolutionError.Type

/**
 * CyclicDependency - Tables in the request reference each other cyclically.
 */
export const CyclicDependencyError = makeError("CYCLIC_DEPENDENCY", "CyclicDependencyError").annotate({
  httpApiStatus: 400
})

export type CyclicDependencyError = typeof CyclicDependencyError.Type

/**
 * DependencyManifestLinkCheck - Failed to verify that a dependency manifest is linked to its dataset.
 */
export const DependencyManifestLinkCheckError = makeError(
  "DEPENDENCY_MANIFEST_LINK_CHECK",
  "DependencyManifestLinkCheckError"
).annotate({ httpApiStatus: 500 })

export type DependencyManifestLinkCheckError = typeof DependencyManifestLinkCheckError.Type

/**
 * DependencyVersionResolution - Failed to resolve the version of a dependency.
 */
export const DependencyVersionResolutionError = makeError(
  "DEPENDENCY_VERSION_RESOLUTION",
  "DependencyVersionResolutionError"
).annotate({ httpApiStatus: 500 })

export type DependencyVersionResolutionError = typeof DependencyVersionResolutionError.Type

/**
 * InvalidPlan - The query plan could not be built from the user-provided SQL.
 */
export const InvalidPlanError = makeError("INVALID_PLAN", "InvalidPlanError").annotate({ httpApiStatus: 400 })

export type InvalidPlanError = typeof InvalidPlanError.Type

/**
 * MissingBlockNum - The table output is missing the required block number column.
 */
export const MissingBlockNumError = makeError("MISSING_BLOCK_NUM", "MissingBlockNumError").annotate({
  httpApiStatus: 400
})

export type MissingBlockNumError = typeof MissingBlockNumError.Type

/**
 * MissingTs - The table output is missing the required timestamp column.
 */
export const MissingTsError = makeError("MISSING_TS", "MissingTsError").annotate({ httpApiStatus: 400 })

export type MissingTsError = typeof MissingTsError.Type

/**
 * NoTableReferences - A table query does not reference any table.
 */
export const NoTableReferencesError = makeError("NO_TABLE_REFERENCES", "NoTableReferencesError").annotate({
  httpApiStatus: 400
})

export type NoTableReferencesError = typeof NoTableReferencesError.Type

/**
 * SelfReferencingTable - A table query references itself.
 */
export const SelfReferencingTableError = makeError("SELF_REFERENCING_TABLE", "SelfReferencingTableError").annotate({
  httpApiStatus: 400
})

export type SelfReferencingTableError = typeof SelfReferencingTableError.Type

/**
 * SelfRefNetworksNotResolved - The networks of a self-referenced table could not be resolved.
 */
export const SelfRefNetworksNotResolvedError = makeError(
  "SELF_REF_NETWORKS_NOT_RESOLVED",
  "SelfRefNetworksNotResolvedError"
).annotate({ httpApiStatus: 500 })

export type SelfRefNetworksNotResolvedError = typeof SelfRefNetworksNotResolvedError.Type

/**
 * SelfRefTableNotFound - A self-referenced table does not exist in the request.
 */
export const SelfRefTableNotFoundError = makeError("SELF_REF_TABLE_NOT_FOUND", "SelfRefTableNotFoundError").annotate({
  httpApiStatus: 400
})

export type SelfRefTableNotFoundError = typeof SelfRefTableNotFoundError.Type

/**
 * SessionConfigError - Failed to configure the query session.
 */
export const SessionConfigError = makeError("SESSION_CONFIG_ERROR", "SessionConfigError").annotate({
  httpApiStatus: 500
})

export type SessionConfigError = typeof SessionConfigError.Type

/**
 * StaticSourceNotMaterializable - A static dataset source cannot be materialized by a derived table.
 */
export const StaticSourceNotMaterializableError = makeError(
  "STATIC_SOURCE_NOT_MATERIALIZABLE",
  "StaticSourceNotMaterializableError"
).annotate({ httpApiStatus: 400 })

export type StaticSourceNotMaterializableError = typeof StaticSourceNotMaterializableError.Type

/**
 * TableReferencesNotResolved - The table references of a query could not be resolved.
 */
export const TableReferencesNotResolvedError = makeError(
  "TABLE_REFERENCES_NOT_RESOLVED",
  "TableReferencesNotResolvedError"
).annotate({ httpApiStatus: 500 })

export type TableReferencesNotResolvedError = typeof TableReferencesNotResolvedError.Type

// =============================================================================
// Scheduler/Worker Errors
// =============================================================================

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
 * VersionTaggingError - Failed to tag version for the dataset.
 */
export const VersionTaggingError = makeError("VERSION_TAGGING_ERROR", "VersionTaggingError").annotate({
  httpApiStatus: 500
})

export type VersionTaggingError = typeof VersionTaggingError.Type

/**
 * ListAllDatasetsError - Failed to list all datasets from dataset store.
 */
export const ListAllDatasetsError = makeError("LIST_ALL_DATASETS_ERROR", "ListAllDatasetsError").annotate({
  httpApiStatus: 500
})

export type ListAllDatasetsError = typeof ListAllDatasetsError.Type

/**
 * ListVersionTagsError - Failed to list version tags from dataset store.
 */
export const ListVersionTagsError = makeError("LIST_VERSION_TAGS_ERROR", "ListVersionTagsError").annotate({
  httpApiStatus: 500
})

export type ListVersionTagsError = typeof ListVersionTagsError.Type
