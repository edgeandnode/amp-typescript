import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiClient from "@effect/platform/HttpApiClient"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import * as HttpClient from "@effect/platform/HttpClient"
import * as HttpClientError from "@effect/platform/HttpClientError"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import { constUndefined } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Auth from "./Auth.ts"
import * as Models from "./Models.ts"

// =============================================================================
// Admin API Params
// =============================================================================

/**
 * A URL parameter for the dataset namespace.
 */
const datasetNamespaceParam = HttpApiSchema.param("namespace", Models.DatasetNamespace)

/**
 * A URL parameter for the dataset name.
 */
const datasetNameParam = HttpApiSchema.param("name", Models.DatasetName)

/**
 * A URL parameter for the dataset revision.
 */
const datasetRevisionParam = HttpApiSchema.param("revision", Models.DatasetRevision)

/**
 * A URL parameter for the unique job identifier.
 */
const jobIdParam = HttpApiSchema.param(
  "jobId",
  Schema.NumberFromString.annotations({
    identifier: "JobId",
    description: "The unique identifier for a job."
  })
)

// =============================================================================
// Admin API Schemas
// =============================================================================

export class GetDatasetsResponse extends Schema.Class<GetDatasetsResponse>(
  "Amp/AdminApi/GetDatasetsResponse"
)({
  datasets: Schema.Array(Schema.Struct({
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    versions: Schema.Array(Models.DatasetVersion),
    latestVersion: Models.DatasetVersion.pipe(
      Schema.optional,
      Schema.fromKey("latest_version")
    )
  }))
}, { identifier: "GetDatasetsResponse" }) {}

export class RegisterDatasetPayload extends Schema.Class<RegisterDatasetPayload>(
  "Amp/AdminApi/RegisterDatasetPayload"
)({
  namespace: Schema.String,
  name: Schema.String,
  version: Schema.optional(Schema.String),
  manifest: Models.DatasetManifest
}, { identifier: "RegisterDatasetPayload" }) {}

export class GetDatasetVersionResponse extends Schema.Class<GetDatasetVersionResponse>(
  "Amp/AdminApi/GetDatasetVersionResponse"
)({
  kind: Models.DatasetKind,
  namespace: Models.DatasetNamespace,
  name: Models.DatasetName,
  revision: Models.DatasetRevision,
  manifestHash: Models.DatasetHash.pipe(
    Schema.propertySignature,
    Schema.fromKey("manifest_hash")
  )
}, { identifier: "GetDatasetVersionResponse" }) {}

export class GetDatasetVersionsResponse extends Schema.Class<GetDatasetVersionsResponse>(
  "Amp/AdminApi/GetDatasetVersionsResponse"
)({
  versions: Schema.Array(Models.DatasetVersion)
}, { identifier: "GetDatasetVersionsResponse" }) {}

export class DeployDatasetPayload extends Schema.Class<DeployDatasetPayload>(
  "Amp/AdminApi/DeployRequest"
)({
  endBlock: Schema.NullOr(Schema.String).pipe(
    Schema.optional,
    Schema.fromKey("end_block")
  ),
  parallelism: Schema.optional(Schema.Number)
}, { identifier: "DeployDatasetPayload" }) {}

export class DeployDatasetResponse extends Schema.Class<DeployDatasetResponse>(
  "Amp/AdminApi/DeployResponse"
)({
  jobId: Models.JobId.pipe(
    Schema.propertySignature,
    Schema.fromKey("job_id")
  )
}, { identifier: "DeployDatasetResponse" }) {}

// =============================================================================
// Admin API Errors
// =============================================================================

/**
 * CatalogQualifiedTable - Table reference includes a catalog qualifier.
 *
 * Causes:
 * - SQL query contains a catalog-qualified table reference (catalog.schema.table)
 * - Only dataset-qualified tables are supported (dataset.table)
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with catalog-qualified table references
 */
export class CatalogQualifiedTable extends Schema.Class<CatalogQualifiedTable>(
  "Amp/AdminApi/Errors/CatalogQualifiedTable"
)({
  code: Schema.Literal("CATALOG_QUALIFIED_TABLE").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "CatalogQualifiedTable",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "CatalogQualifiedTable"
}

/**
 * DatasetNotFound - The requested dataset does not exist.
 *
 * Causes:
 * - Dataset ID does not exist in the system
 * - Dataset has been deleted
 * - Dataset not yet registered
 *
 * Applies to:
 * - GET /datasets/{id} - When dataset ID doesn't exist
 * - POST /datasets/{id}/dump - When attempting to dump non-existent dataset
 * - Query operations referencing non-existent datasets
 */
export class DatasetNotFound extends Schema.Class<DatasetNotFound>(
  "Amp/AdminApi/Errors/DatasetNotFound"
)({
  code: Schema.Literal("DATASET_NOT_FOUND").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "DatasetNotFound",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "DatasetNotFound"
}

/**
 * DatasetStoreError - Failure in dataset storage operations.
 *
 * Causes:
 * - File/object store retrieval failures
 * - Manifest parsing errors (TOML/JSON)
 * - Unsupported dataset kind
 * - Dataset name validation failures
 * - Schema validation errors (missing or mismatched)
 * - Provider configuration not found
 * - SQL parsing failures in dataset definitions
 *
 * Applies to:
 * - GET /datasets - Listing datasets
 * - GET /datasets/{id} - Retrieving specific dataset
 * - POST /datasets/{id}/dump - When loading dataset definitions
 * - Query operations that access dataset metadata
 */
export class DatasetStoreError extends Schema.Class<DatasetStoreError>(
  "Amp/AdminApi/Errors/DatasetStoreError"
)({
  code: Schema.Literal("DATASET_STORE_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "DatasetStoreError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "DatasetStoreError"
}

/**
 * DependencyAliasNotFound - Dependency alias not found in dependencies map.
 *
 * Causes:
 * - Table reference uses an alias not provided in dependencies
 * - Function reference uses an alias not provided in dependencies
 *
 * Applies to:
 * - POST /schema - When looking up dependency aliases
 */
export class DependencyAliasNotFound extends Schema.Class<DependencyAliasNotFound>(
  "Amp/AdminApi/Errors/DependencyAliasNotFound"
)({
  code: Schema.Literal("DEPENDENCY_ALIAS_NOT_FOUND").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "DependencyAliasNotFound",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "DependencyAliasNotFound"
}

/**
 * DependencyNotFound - Dependency not found in dataset store.
 *
 * Causes:
 * - Referenced dependency does not exist in dataset store
 * - Specified version or hash cannot be found
 *
 * Applies to:
 * - POST /schema - When resolving dependencies
 */
export class DependencyNotFound extends Schema.Class<DependencyNotFound>(
  "Amp/AdminApi/Errors/DependencyNotFound"
)({
  code: Schema.Literal("DEPENDENCY_NOT_FOUND").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "DependencyNotFound",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "DependencyNotFound"
}

/**
 * DependencyResolution - Failed to resolve dependency.
 *
 * Causes:
 * - Database query fails during resolution
 *
 * Applies to:
 * - POST /schema - When resolving dependencies
 */
export class DependencyResolution extends Schema.Class<DependencyResolution>(
  "Amp/AdminApi/Errors/DependencyResolution"
)({
  code: Schema.Literal("DEPENDENCY_RESOLUTION").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "DependencyResolution",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "DependencyResolution"
}

/**
 * EmptyTablesAndFunctions - No tables or functions provided.
 *
 * Causes:
 * - At least one table or function is required for schema analysis
 *
 * Applies to:
 * - POST /schema - When both tables and functions fields are empty
 */
export class EmptyTablesAndFunctions extends Schema.Class<EmptyTablesAndFunctions>(
  "Amp/AdminApi/Errors/EmptyTablesAndFunctions"
)({
  code: Schema.Literal("EMPTY_TABLES_AND_FUNCTIONS").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "EmptyTablesAndFunctions",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "EmptyTablesAndFunctions"
}

/**
 * EthCallNotAvailable - eth_call function not available for dataset.
 *
 * Causes:
 * - eth_call function is referenced in SQL but dataset doesn't support it
 * - Dataset is not an EVM RPC dataset
 *
 * Applies to:
 * - POST /schema - When checking eth_call availability
 */
export class EthCallNotAvailable extends Schema.Class<EthCallNotAvailable>(
  "Amp/AdminApi/Errors/EthCallNotAvailable"
)({
  code: Schema.Literal("ETH_CALL_NOT_AVAILABLE").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "EthCallNotAvailable",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "EthCallNotAvailable"
}

/**
 * EthCallUdfCreationError - Failed to create ETH call UDF.
 *
 * Causes:
 * - Invalid provider configuration for dataset
 * - Provider connection issues
 * - Dataset is not an EVM RPC dataset but eth_call was requested
 *
 * Applies to:
 * - POST /schema - When creating ETH call UDFs
 */
export class EthCallUdfCreationError extends Schema.Class<EthCallUdfCreationError>(
  "Amp/AdminApi/Errors/EthCallUdfCreationError"
)({
  code: Schema.Literal("ETH_CALL_UDF_CREATION_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "EthCallUdfCreationError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "EthCallUdfCreationError"
}

/**
 * FunctionNotFoundInDataset - Function not found in referenced dataset.
 *
 * Causes:
 * - SQL query references a function that doesn't exist in the dataset
 * - Function name is misspelled or dataset doesn't define the function
 *
 * Applies to:
 * - POST /schema - When resolving function references
 */
export class FunctionNotFoundInDataset extends Schema.Class<FunctionNotFoundInDataset>(
  "Amp/AdminApi/Errors/FunctionNotFoundInDataset"
)({
  code: Schema.Literal("FUNCTION_NOT_FOUND_IN_DATASET").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "FunctionNotFoundInDataset",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "FunctionNotFoundInDataset"
}

/**
 * FunctionReferenceResolution - Failed to resolve function references from SQL.
 *
 * Causes:
 * - Unsupported DML statements encountered
 *
 * Applies to:
 * - POST /schema - When resolving function references
 */
export class FunctionReferenceResolution extends Schema.Class<FunctionReferenceResolution>(
  "Amp/AdminApi/Errors/FunctionReferenceResolution"
)({
  code: Schema.Literal("FUNCTION_REFERENCE_RESOLUTION").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "FunctionReferenceResolution",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "FunctionReferenceResolution"
}

/**
 * GetDatasetError - Failed to retrieve dataset from store.
 *
 * Causes:
 * - Dataset manifest is invalid or corrupted
 * - Unsupported dataset kind
 * - Storage backend errors when reading dataset
 *
 * Applies to:
 * - POST /schema - When loading dataset definitions
 */
export class GetDatasetError extends Schema.Class<GetDatasetError>(
  "Amp/AdminApi/Errors/GetDatasetError"
)({
  code: Schema.Literal("GET_DATASET_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "GetDatasetError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "GetDatasetError"
}

/**
 * InvalidJobId - The provided job ID is malformed or invalid.
 *
 * Causes:
 * - Job ID contains invalid characters
 * - Job ID format does not match expected pattern
 * - Empty or null job ID
 * - Job ID is not a valid integer
 *
 * Applies to:
 * - GET /jobs/{id} - When ID format is invalid
 * - DELETE /jobs/{id} - When ID format is invalid
 * - PUT /jobs/{id}/stop - When ID format is invalid
 */
export class InvalidJobId extends Schema.Class<InvalidJobId>(
  "Amp/AdminApi/Errors/InvalidJobId"
)({
  code: Schema.Literal("INVALID_JOB_ID").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidJobId",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidJobId"
}

/**
 * InvalidManifest - Dataset manifest is semantically invalid.
 *
 * Causes:
 * - Invalid dataset references in SQL views
 * - Circular dependencies between datasets
 * - Invalid provider references
 * - Schema validation failures
 * - Invalid dataset configuration
 *
 * Applies to:
 * - POST /datasets - During manifest validation
 * - Dataset initialization
 * - Different from ManifestParseError (syntax vs semantics)
 */
export class InvalidManifest extends Schema.Class<InvalidManifest>(
  "Amp/AdminApi/Errors/InvalidManifest"
)({
  code: Schema.Literal("INVALID_MANIFEST").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidManifest",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidManifest"
}

/**
 * InvalidPayloadFormat - Invalid request payload format.
 *
 * Causes:
 * - Request JSON is malformed or invalid
 * - Required fields are missing or have wrong types
 * - Dataset name or version format is invalid
 * - JSON deserialization failures
 *
 * Applies to:
 * - POST /datasets - When request body is invalid
 * - POST /schema - When request payload cannot be parsed
 */
export class InvalidPayloadFormat extends Schema.Class<InvalidPayloadFormat>(
  "Amp/AdminApi/Errors/InvalidPayloadFormat"
)({
  code: Schema.Literal("INVALID_PAYLOAD_FORMAT").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidPayloadFormat",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidPayloadFormat"
}

/**
 * InvalidRequest - The request is malformed or contains invalid parameters.
 *
 * Causes:
 * - Missing required request parameters
 * - Invalid parameter values
 * - Malformed request body
 * - Invalid content type
 * - Request validation failures
 *
 * Applies to:
 * - POST /datasets - Invalid registration request
 * - POST /datasets/{id}/dump - Invalid dump parameters
 * - Any endpoint with request validation
 */
export class InvalidRequest extends Schema.Class<InvalidRequest>(
  "Amp/AdminApi/Errors/InvalidRequest"
)({
  code: Schema.Literal("INVALID_REQUEST").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidRequest",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidRequest"
}

/**
 * InvalidSelector - The provided dataset selector (name/version) is malformed or invalid.
 *
 * Causes:
 * - Dataset name contains invalid characters or doesn't follow naming conventions
 * - Dataset name is empty or malformed
 * - Version syntax is invalid (e.g., malformed semver)
 * - Path parameter extraction fails for dataset selection
 *
 * Applies to:
 * - GET /datasets/{name} - When dataset name format is invalid
 * - GET /datasets/{name}/versions/{version} - When name or version format is invalid
 * - Any endpoint accepting dataset selector parameters
 */
export class InvalidSelector extends Schema.Class<InvalidSelector>(
  "Amp/AdminApi/Errors/InvalidSelector"
)({
  code: Schema.Literal("INVALID_SELECTOR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidSelector",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidSelector"
}

/**
 * InvalidTableName - Table name does not conform to SQL identifier rules.
 *
 * Causes:
 * - Table name contains invalid characters
 * - Table name doesn't follow naming conventions
 * - Table name exceeds maximum length
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with invalid table names
 */
export class InvalidTableName extends Schema.Class<InvalidTableName>(
  "Amp/AdminApi/Errors/InvalidTableName"
)({
  code: Schema.Literal("INVALID_TABLE_NAME").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidTableName",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidTableName"
}

/**
 * InvalidTableSql - SQL syntax error in table definition.
 *
 * Causes:
 * - Query parsing fails
 *
 * Applies to:
 * - POST /schema - When analyzing table SQL queries
 */
export class InvalidTableSql extends Schema.Class<InvalidTableSql>(
  "Amp/AdminApi/Errors/InvalidTableSql"
)({
  code: Schema.Literal("INVALID_TABLE_SQL").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "InvalidTableSql",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "InvalidTableSql"
}

/**
 * JobNotFound - The requested job does not exist.
 *
 * Causes:
 * - Job ID does not exist in the system
 * - Job has been deleted
 * - Job has completed and been cleaned up
 *
 * Applies to:
 * - GET /jobs/{id} - When job ID doesn't exist
 * - DELETE /jobs/{id} - When attempting to delete non-existent job
 * - PUT /jobs/{id}/stop - When attempting to stop non-existent job
 */
export class JobNotFound extends Schema.Class<JobNotFound>(
  "Amp/AdminApi/Errors/JobNotFound"
)({
  code: Schema.Literal("JOB_NOT_FOUND").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "JobNotFound",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "JobNotFound"
}

/**
 * ManifestLinkingError - Failed to link manifest to dataset.
 *
 * Causes:
 * - Error during manifest linking in metadata database
 * - Error updating dev tag
 * - Database transaction failure
 * - Foreign key constraint violations
 *
 * Applies to:
 * - POST /datasets - During manifest linking to dataset
 */
export class ManifestLinkingError extends Schema.Class<ManifestLinkingError>(
  "Amp/AdminApi/Errors/ManifestLinkingError"
)({
  code: Schema.Literal("MANIFEST_LINKING_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "ManifestLinkingError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "ManifestLinkingError"
}

/**
 * ManifestNotFound - Manifest with the provided hash not found.
 *
 * Causes:
 * - A manifest hash was provided but the manifest doesn't exist in the system
 * - The hash is valid format but no manifest is stored with that hash
 * - Manifest was deleted or never registered
 *
 * Applies to:
 * - POST /datasets - When linking to a manifest hash that doesn't exist
 */
export class ManifestNotFound extends Schema.Class<ManifestNotFound>(
  "Amp/AdminApi/Errors/ManifestNotFound"
)({
  code: Schema.Literal("MANIFEST_NOT_FOUND").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "ManifestNotFound",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "ManifestNotFound"
}

/**
 * ManifestRegistrationError - Failed to register manifest in the system.
 *
 * Causes:
 * - Internal error during manifest registration
 * - Registry service unavailable
 * - Manifest storage failure
 *
 * Applies to:
 * - POST /datasets - During manifest registration
 * - POST /datasets - During manifest registration
 */
export class ManifestRegistrationError extends Schema.Class<ManifestRegistrationError>(
  "Amp/AdminApi/Errors/ManifestRegistrationError"
)({
  code: Schema.Literal("MANIFEST_REGISTRATION_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "ManifestRegistrationError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "ManifestRegistrationError"
}

/**
 * ManifestValidationError - Manifest validation error.
 *
 * Causes:
 * - SQL queries contain non-incremental operations
 * - Invalid table references in SQL
 * - Schema validation failures
 * - Type inference errors
 *
 * Applies to:
 * - POST /datasets - During manifest validation
 */
export class ManifestValidationError extends Schema.Class<ManifestValidationError>(
  "Amp/AdminApi/Errors/ManifestValidationError"
)({
  code: Schema.Literal("MANIFEST_VALIDATION_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "ManifestValidationError",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "ManifestValidationError"
}

/**
 * MetadataDbError - Database operation failure in the metadata PostgreSQL database.
 *
 * Causes:
 * - Database connection failures
 * - SQL execution errors
 * - Database migration issues
 * - Worker notification send/receive failures
 * - Data consistency errors (e.g., multiple active locations)
 *
 * Applies to:
 * - Any operation that queries or updates metadata
 * - Worker coordination operations
 * - Dataset state tracking
 */
export class MetadataDbError extends Schema.Class<MetadataDbError>(
  "Amp/AdminApi/Errors/MetadataDbError"
)({
  code: Schema.Literal("METADATA_DB_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "MetadataDbError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "MetadataDbError"
}

/**
 * NonIncrementalQuery - SQL query contains non-incremental operations.
 *
 * Causes:
 * - SQL contains LIMIT, ORDER BY, GROUP BY, DISTINCT, window functions
 * - SQL uses outer joins (LEFT/RIGHT/FULL JOIN)
 * - SQL contains recursive queries
 *
 * Applies to:
 * - POST /schema - When validating SQL queries for incremental processing
 */
export class NonIncrementalQuery extends Schema.Class<NonIncrementalQuery>(
  "Amp/AdminApi/Errors/NonIncrementalQuery"
)({
  code: Schema.Literal("NON_INCREMENTAL_QUERY").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "NonIncrementalQuery",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "NonIncrementalQuery"
}

/**
 * SchedulerError - Indicates a failure in the job scheduling system.
 *
 * Causes:
 * - Failed to schedule a dump job
 * - Worker pool unavailable
 * - Internal scheduler state errors
 *
 * Applies to:
 * - POST /datasets/{name}/dump - When scheduling dataset dumps
 * - POST /datasets - When scheduling registration jobs
 */
export class SchedulerError extends Schema.Class<SchedulerError>(
  "Amp/AdminApi/Errors/SchedulerError"
)({
  code: Schema.Literal("SCHEDULER_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "SchedulerError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "SchedulerError"
}

/**
 * SchemaInference - Failed to infer output schema from query.
 *
 * Causes:
 * - Schema determination encounters errors
 *
 * Applies to:
 * - POST /schema - When inferring output schema
 */
export class SchemaInference extends Schema.Class<SchemaInference>(
  "Amp/AdminApi/Errors/SchemaInference"
)({
  code: Schema.Literal("SCHEMA_INFERENCE").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "SchemaInference",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "SchemaInference"
}

/**
 * StoreError - Dataset store operation error.
 *
 * Causes:
 * - Failed to load dataset from store
 * - Dataset store configuration errors
 * - Dataset store connectivity issues
 * - Object store access failures
 *
 * Applies to:
 * - POST /datasets - During dataset store operations
 */
export class StoreError extends Schema.Class<StoreError>(
  "Amp/AdminApi/Errors/StoreError"
)({
  code: Schema.Literal("STORE_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "StoreError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "StoreError"
}

/**
 * TableNotFoundInDataset - Table not found in dataset.
 *
 * Causes:
 * - Table name referenced in SQL query does not exist in the dataset
 * - Table name is misspelled
 * - Dataset does not contain the referenced table
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries with invalid table references
 */
export class TableNotFoundInDataset extends Schema.Class<TableNotFoundInDataset>(
  "Amp/AdminApi/Errors/TableNotFoundInDataset"
)({
  code: Schema.Literal("TABLE_NOT_FOUND_IN_DATASET").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "TableNotFoundInDataset",
  [HttpApiSchema.AnnotationStatus]: 404
}) {
  readonly _tag = "TableNotFoundInDataset"
}

/**
 * TableReferenceResolution - Failed to extract table references from SQL.
 *
 * Causes:
 * - Invalid table reference format encountered
 *
 * Applies to:
 * - POST /schema - When resolving table references
 */
export class TableReferenceResolution extends Schema.Class<TableReferenceResolution>(
  "Amp/AdminApi/Errors/TableReferenceResolution"
)({
  code: Schema.Literal("TABLE_REFERENCE_RESOLUTION").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "TableReferenceResolution",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "TableReferenceResolution"
}

/**
 * UnqualifiedTable - Table reference is not qualified with a dataset.
 *
 * Causes:
 * - SQL query contains a table reference without a schema/dataset qualifier
 * - All tables must be qualified with a dataset reference (e.g., dataset.table)
 *
 * Applies to:
 * - POST /schema - When analyzing SQL queries
 * - Query operations with unqualified table references
 */
export class UnqualifiedTable extends Schema.Class<UnqualifiedTable>(
  "Amp/AdminApi/Errors/UnqualifiedTable"
)({
  code: Schema.Literal("UNQUALIFIED_TABLE").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "UnqualifiedTable",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "UnqualifiedTable"
}

/**
 * UnsupportedDatasetKind - Dataset kind is not supported.
 *
 * Causes:
 * - Dataset kind is not one of the supported types (manifest, evm-rpc, firehose, eth-beacon)
 * - Invalid or unknown dataset kind value
 * - Legacy dataset kinds that are no longer supported
 *
 * Applies to:
 * - POST /datasets - During manifest validation
 */
export class UnsupportedDatasetKind extends Schema.Class<UnsupportedDatasetKind>(
  "Amp/AdminApi/Errors/UnsupportedDatasetKind"
)({
  code: Schema.Literal("UNSUPPORTED_DATASET_KIND").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "UnsupportedDatasetKind",
  [HttpApiSchema.AnnotationStatus]: 400
}) {
  readonly _tag = "UnsupportedDatasetKind"
}

/**
 * VersionTaggingError - Failed to tag version for the dataset.
 *
 * Causes:
 * - Error during version tagging in metadata database
 * - Invalid semantic version format
 * - Error updating latest tag
 * - Database constraint violations
 *
 * Applies to:
 * - POST /datasets - During version tagging
 */
export class VersionTaggingError extends Schema.Class<VersionTaggingError>(
  "Amp/AdminApi/Errors/VersionTaggingError"
)({
  code: Schema.Literal("VERSION_TAGGING_ERROR").pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code")
  ),
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}, {
  identifier: "VersionTaggingError",
  [HttpApiSchema.AnnotationStatus]: 500
}) {
  readonly _tag = "VersionTaggingError"
}

// =============================================================================
// Admin API Endpoints
// =============================================================================

// -----------------------------------------------------------------------------
// GET /datasets
// -----------------------------------------------------------------------------

/**
 * The get datasets endpoint (GET /datasets).
 */
const getDatasets = HttpApiEndpoint.get("getDatasets")`/datasets`
  .addError(DatasetStoreError)
  .addError(MetadataDbError)
  .addSuccess(GetDatasetsResponse)

/**
 * Error type for the `getDatasets` endpoint.
 *
 * - DatasetStoreError: Failed to retrieve datasets from the dataset store.
 * - MetadataDbError: Database error while retrieving active locations for tables.
 */
export type GetDatasetsError =
  | DatasetStoreError
  | MetadataDbError

// -----------------------------------------------------------------------------
// POST /datasets
// -----------------------------------------------------------------------------

const registerDataset = HttpApiEndpoint.post("registerDataset")`/datasets`
  .addError(InvalidPayloadFormat)
  .addError(InvalidManifest)
  .addError(ManifestLinkingError)
  .addError(ManifestNotFound)
  .addError(ManifestRegistrationError)
  .addError(ManifestValidationError)
  .addError(StoreError)
  .addError(UnsupportedDatasetKind)
  .addError(VersionTaggingError)
  .addSuccess(Schema.Void, { status: 201 })
  .setPayload(RegisterDatasetPayload)

/**
 * Error type for the `registerDataset` endpoint.
 *
 * - InvalidPayloadFormat: Request JSON is malformed or invalid.
 * - InvalidManifest: Manifest JSON is malformed or structurally invalid.
 * - ManifestLinkingError: Failed to link manifest to dataset.
 * - ManifestNotFound: Manifest hash provided but manifest doesn't exist.
 * - ManifestRegistrationError: Failed to register manifest in system.
 * - ManifestValidationError: Manifest validation error (e.g., non-incremental operations).
 * - StoreError: Dataset store operation error.
 * - UnsupportedDatasetKind: Dataset kind is not supported.
 * - VersionTaggingError: Failed to tag version for the dataset.
 */
export type RegisterDatasetError =
  | InvalidPayloadFormat
  | InvalidManifest
  | ManifestLinkingError
  | ManifestNotFound
  | ManifestRegistrationError
  | ManifestValidationError
  | StoreError
  | UnsupportedDatasetKind
  | VersionTaggingError

// -----------------------------------------------------------------------------
// GET /datasets/{namespace}/{name}/versions
// -----------------------------------------------------------------------------

const getDatasetVersions = HttpApiEndpoint.get(
  "getDatasetVersions"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions`
  .addError(DatasetStoreError)
  .addError(InvalidRequest)
  .addError(MetadataDbError)
  .addSuccess(GetDatasetVersionsResponse)

/**
 * Error type for the `getDatasetVersions` endpoint.
 *
 * - DatasetStoreError: Failed to list version tags from dataset store.
 * - InvalidRequest: Invalid namespace or name in path parameters.
 * - MetadataDbError: Database error while retrieving versions.
 */
export type GetDatasetVersionsError =
  | DatasetStoreError
  | InvalidRequest
  | MetadataDbError

// -----------------------------------------------------------------------------
// GET /datasets/{namespace}/{name}/versions/{revision}
// -----------------------------------------------------------------------------

const getDatasetVersion = HttpApiEndpoint.get(
  "getDatasetVersion"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}`
  .addError(DatasetNotFound)
  .addError(DatasetStoreError)
  .addError(InvalidRequest)
  .addError(MetadataDbError)
  .addSuccess(GetDatasetVersionResponse)

/**
 * Error type for the `getDatasetVersion` endpoint.
 *
 * - DatasetNotFound: The dataset or revision was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 * - InvalidRequest: Invalid namespace, name, or revision in path parameters.
 * - MetadataDbError: Database error while retrieving dataset information.
 */
export type GetDatasetVersionError =
  | DatasetNotFound
  | DatasetStoreError
  | InvalidRequest
  | MetadataDbError

// -----------------------------------------------------------------------------
// POST /datasets/{namespace}/{name}/versions/{revision}/deploy
// -----------------------------------------------------------------------------

const deployDataset = HttpApiEndpoint.post(
  "deployDataset"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/deploy`
  .addError(DatasetNotFound)
  .addError(DatasetStoreError)
  .addError(InvalidRequest)
  .addError(MetadataDbError)
  .addError(SchedulerError)
  .addSuccess(DeployDatasetResponse, { status: 202 })
  .setPayload(DeployDatasetPayload)

/**
 * Error type for the `deployDataset` endpoint.
 *
 * - DatasetNotFound: The dataset or revision was not found.
 * - DatasetStoreError: Failed to load dataset from store.
 * - InvalidRequest: Invalid path parameters or request body.
 * - MetadataDbError: Database error while scheduling job.
 * - SchedulerError: Failed to schedule the deployment job.
 */
export type DeployDatasetError =
  | DatasetNotFound
  | DatasetStoreError
  | InvalidRequest
  | MetadataDbError
  | SchedulerError

// -----------------------------------------------------------------------------
// GET /datasets/{namespace}/{name}/versions/{revision}/manifest
// -----------------------------------------------------------------------------

const getDatasetManifest = HttpApiEndpoint.get(
  "getDatasetManifest"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/manifest`
  .addError(DatasetNotFound)
  .addError(DatasetStoreError)
  .addError(InvalidRequest)
  .addError(MetadataDbError)
  .addSuccess(Models.DatasetManifest)

/**
 * Error type for the `getDatasetManifest` endpoint.
 *
 * - DatasetNotFound: The dataset, revision, or manifest was not found.
 * - DatasetStoreError: Failed to read manifest from store.
 * - InvalidRequest: Invalid namespace, name, or revision in path parameters.
 * - MetadataDbError: Database error while retrieving manifest path.
 */
export type GetDatasetManifestError =
  | DatasetNotFound
  | DatasetStoreError
  | InvalidRequest
  | MetadataDbError

// -----------------------------------------------------------------------------
// GET /jobs/{jobId}
// -----------------------------------------------------------------------------

const getJobById = HttpApiEndpoint.get("getJobById")`/jobs/${jobIdParam}`
  .addError(InvalidJobId)
  .addError(JobNotFound)
  .addError(MetadataDbError)
  .addSuccess(Models.JobInfo)

/**
 * Error type for the `getJobById` endpoint.
 *
 * - InvalidJobId: The provided ID is not a valid job identifier.
 * - JobNotFound: No job exists with the given ID.
 * - MetadataDbError: Internal database error occurred.
 */
export type GetJobByIdError =
  | InvalidJobId
  | JobNotFound
  | MetadataDbError

// -----------------------------------------------------------------------------
// POST /schema
// -----------------------------------------------------------------------------

export class GetOutputSchemaPayload extends Schema.Class<GetOutputSchemaPayload>("SchemaRequest")({
  tables: Schema.Record({
    key: Schema.String,
    value: Schema.String
  }),
  dependencies: Schema.Record({
    key: Schema.String,
    value: Models.DatasetReferenceFromString
  }).pipe(Schema.optional),
  functions: Schema.Record({
    key: Schema.String,
    value: Models.FunctionDefinition
  }).pipe(Schema.optional)
}) {}

export class GetOutputSchemaResponse extends Schema.Class<GetOutputSchemaResponse>("SchemaResponse")({
  schemas: Schema.Record({
    key: Schema.String,
    value: Models.TableSchemaWithNetworks
  })
}) {}

/**
 * The output schema endpoint (POST /schema).
 */
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema")`/schema`
  .addError(CatalogQualifiedTable)
  .addError(DatasetNotFound)
  .addError(DependencyAliasNotFound)
  .addError(DependencyNotFound)
  .addError(DependencyResolution)
  .addError(EmptyTablesAndFunctions)
  .addError(EthCallNotAvailable)
  .addError(EthCallUdfCreationError)
  .addError(FunctionNotFoundInDataset)
  .addError(FunctionReferenceResolution)
  .addError(GetDatasetError)
  .addError(InvalidPayloadFormat)
  .addError(InvalidTableName)
  .addError(InvalidTableSql)
  .addError(NonIncrementalQuery)
  .addError(SchemaInference)
  .addError(TableNotFoundInDataset)
  .addError(TableReferenceResolution)
  .addError(UnqualifiedTable)
  .addSuccess(GetOutputSchemaResponse)
  .setPayload(GetOutputSchemaPayload)

/**
 * Error type for the `getOutputSchema` endpoint.
 *
 * - CatalogQualifiedTable: Table reference includes catalog qualifier (not supported).
 * - DependencyAliasNotFound: Table or function reference uses undefined alias.
 * - DatasetNotFound: Referenced dataset does not exist in the store.
 * - DependencyNotFound: Referenced dependency does not exist.
 * - DependencyResolution: Failed to resolve dependency to hash.
 * - EmptyTablesAndFunctions: No tables or functions provided (at least one required).
 * - EthCallNotAvailable: eth_call function not available for dataset.
 * - EthCallUdfCreationError: Failed to create ETH call UDF.
 * - FunctionNotFoundInDataset: Referenced function does not exist in dataset.
 * - FunctionReferenceResolution: Failed to resolve function references in SQL.
 * - GetDatasetError: Failed to retrieve dataset from store.
 * - InvalidPayloadFormat: Request JSON is malformed or missing required fields.
 * - InvalidTableName: Table name does not conform to SQL identifier rules.
 * - InvalidTableSql: SQL query has invalid syntax.
 * - SchemaInference: Failed to infer schema for table.
 * - TableNotFoundInDataset: Referenced table does not exist in dataset.
 * - TableReferenceResolution: Failed to resolve table references in SQL.
 * - UnqualifiedTable: Table reference is not qualified with dataset.
 */
export type GetOutputSchemaError =
  | CatalogQualifiedTable
  | DatasetNotFound
  | DependencyAliasNotFound
  | DependencyNotFound
  | DependencyResolution
  | EmptyTablesAndFunctions
  | EthCallNotAvailable
  | EthCallUdfCreationError
  | FunctionNotFoundInDataset
  | FunctionReferenceResolution
  | GetDatasetError
  | InvalidPayloadFormat
  | InvalidTableName
  | InvalidTableSql
  | NonIncrementalQuery
  | SchemaInference
  | TableNotFoundInDataset
  | TableReferenceResolution
  | UnqualifiedTable

// =============================================================================
// Admin API Groups
// =============================================================================

/**
 * The api group for the dataset endpoints.
 */
export class DatasetGroup extends HttpApiGroup.make("dataset")
  .add(registerDataset)
  .add(getDatasets)
  .add(getDatasetVersions)
  .add(getDatasetVersion)
  .add(deployDataset)
  .add(getDatasetManifest)
{}

/**
 * The api group for the job endpoints.
 */
export class JobGroup extends HttpApiGroup.make("job").add(getJobById) {}

/**
 * The api group for the schema endpoints.
 */
export class SchemaGroup extends HttpApiGroup.make("schema").add(getOutputSchema) {}

// =============================================================================
// Admin API
// =============================================================================

/**
 * The api definition for the admin api.
 */
export class Api extends HttpApi.make("admin")
  .add(DatasetGroup)
  .add(JobGroup)
  .add(SchemaGroup)
  .addError(HttpApiError.Forbidden)
  .addError(HttpApiError.Unauthorized)
{}

// =============================================================================
// Admin API Service
// =============================================================================

/**
 * Options for dumping a dataset.
 */
export interface DumpDatasetOptions {
  /**
   * The version of the dataset to dump.
   */
  readonly version?: string | undefined
  /**
   * The block up to which to dump.
   */
  readonly endBlock?: number | undefined
}

/**
 * Represents possible errors that can occur when performing HTTP requests.
 */
export type HttpError =
  | HttpApiError.Forbidden
  | HttpApiError.Unauthorized
  | HttpClientError.HttpClientError

/**
 * A service which can be used to execute operations against the Amp admin API.
 */
export class AdminApi extends Context.Tag("Amp/AdminApi")<AdminApi, {
  /**
   * Register a dataset manifest.
   *
   * @param namespace The namespace of the dataset to register.
   * @param name The name of the dataset to register.
   * @param version Optional version of the dataset to register. If omitted, only the "dev" tag is updated.
   * @param manifest The dataset manifest to register.
   * @return Whether the registration was successful.
   */
  readonly registerDataset: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    manifest: Models.DatasetManifest,
    version?: Models.DatasetRevision | undefined
  ) => Effect.Effect<void, HttpError | RegisterDatasetError>

  /**
   * Get all datasets.
   *
   * @return The list of all datasets.
   */
  readonly getDatasets: () => Effect.Effect<GetDatasetsResponse, HttpError | GetDatasetsError>

  /**
   * Get all versions of a specific dataset.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @return The list of all dataset versions.
   */
  readonly getDatasetVersions: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName
  ) => Effect.Effect<GetDatasetVersionsResponse, HttpError | GetDatasetVersionsError>

  /**
   * Get a specific dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @param revision The version/revision of the dataset.
   * @return The dataset version information.
   */
  readonly getDatasetVersion: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    revision: Models.DatasetRevision
  ) => Effect.Effect<GetDatasetVersionResponse, HttpError | GetDatasetVersionError>

  /**
   * Deploy a dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset to deploy.
   * @param revision The version/revision to deploy.
   * @param options The deployment options.
   * @return The deployment response with job ID.
   */
  readonly deployDataset: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    revision: Models.DatasetRevision,
    options?: {
      endBlock?: string | null | undefined
      parallelism?: number | undefined
    } | undefined
  ) => Effect.Effect<DeployDatasetResponse, HttpError | DeployDatasetError>

  /**
   * Get the manifest for a dataset version.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @param revision The version/revision of the dataset.
   * @return The dataset manifest.
   */
  readonly getDatasetManifest: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    revision: Models.DatasetRevision
  ) => Effect.Effect<any, HttpError | GetDatasetManifestError>

  /**
   * Get a job by ID.
   *
   * @param jobId The ID of the job to get.
   * @return The job information.
   */
  readonly getJobById: (
    jobId: number
  ) => Effect.Effect<Models.JobInfo, HttpError | GetJobByIdError>

  /**
   * Gets the schema of a dataset.
   *
   * @param request - The schema request with tables and dependencies.
   * @returns An effect that resolves to the schema response.
   */
  readonly getOutputSchema: (
    request: GetOutputSchemaPayload
  ) => Effect.Effect<GetOutputSchemaResponse, HttpError | GetOutputSchemaError>
}>() {}

export interface MakeOptions {
  readonly url: string | URL
}

const make = Effect.fnUntraced(function*(options: MakeOptions) {
  type Service = typeof AdminApi.Service

  const auth = yield* Effect.serviceOption(Auth.Auth)

  const client = yield* HttpApiClient.make(Api, {
    baseUrl: options.url,
    transformClient: Option.match(auth, {
      onNone: constUndefined,
      onSome: (auth) =>
        HttpClient.mapRequestEffect(
          Effect.fnUntraced(function*(request) {
            const authInfo = yield* auth.getCachedAuthInfo
            if (Option.isNone(authInfo)) return request
            const token = authInfo.value.accessToken
            return HttpClientRequest.bearerToken(request, token)
          })
        )
    })
  })

  // Dataset Operations

  const deployDataset: Service["deployDataset"] = Effect.fn("AdminApi.deployDataset")(
    function*(namespace, name, revision, options) {
      const path = { namespace, name, revision }
      const payload = {
        endBlock: options?.endBlock,
        parallelism: options?.parallelism
      }
      yield* Effect.annotateCurrentSpan({ ...path, ...payload })
      return yield* client.dataset.deployDataset({ path, payload })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const getDatasetManifest: Service["getDatasetManifest"] = Effect.fn("AdminApi.getDatasetManifest")(
    function*(namespace, name, revision) {
      const path = { namespace, name, revision }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.dataset.getDatasetManifest({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const getDatasets: Service["getDatasets"] = Effect.fn("AdminApi.getDatasets")(
    function*() {
      return yield* client.dataset.getDatasets({})
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const getDatasetVersion: Service["getDatasetVersion"] = Effect.fn("AdminApi.getDatasetVersion")(
    function*(namespace, name, revision) {
      const path = { namespace, name, revision }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.dataset.getDatasetVersion({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const getDatasetVersions: Service["getDatasetVersions"] = Effect.fn("AdminApi.getDatasetVersions")(
    function*(namespace, name) {
      const path = { namespace, name }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.dataset.getDatasetVersions({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const registerDataset: Service["registerDataset"] = Effect.fn("AdminApi.registerDataset")(
    function*(namespace, name, manifest, version) {
      const payload = { namespace, name, version, manifest }
      yield* Effect.annotateCurrentSpan(payload)
      return yield* client.dataset.registerDataset({ payload })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  // Job Operations

  const getJobById: Service["getJobById"] = Effect.fn("AdminApi.getJobById")(
    function*(jobId) {
      const path = { jobId }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.job.getJobById({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  // Schema Operations

  const getOutputSchema: Service["getOutputSchema"] = Effect.fn("AdminApi.getOutputSchema")(
    function*(payload) {
      return yield* client.schema.getOutputSchema({ payload })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  return AdminApi.of({
    deployDataset,
    getDatasetManifest,
    getDatasets,
    getDatasetVersion,
    getDatasetVersions,
    registerDataset,
    getJobById,
    getOutputSchema
  })
})

/**
 * Creates a layer for the Admin API service.
 */
export const layer = (options: MakeOptions): Layer.Layer<
  AdminApi,
  never,
  HttpClient.HttpClient
> => Layer.effect(AdminApi, make(options))

/**
 * Creates a layer for the Admin API service with authentication provided by
 * default.
 */
export const layerAuth = (options: MakeOptions): Layer.Layer<
  AdminApi,
  never,
  HttpClient.HttpClient | KeyValueStore.KeyValueStore
> =>
  Layer.effect(AdminApi, make(options)).pipe(
    Layer.provide(Auth.layer)
  )
