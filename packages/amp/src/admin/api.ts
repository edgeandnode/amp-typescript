/**
 * This module contains the HttpApi definitions for the Amp Admin API.
 *
 * The Admin API provides operations for managing:
 * - Datasets (registration, versioning, manifests)
 * - Jobs (listing, stopping, deletion)
 * - Workers (listing)
 * - Providers (listing)
 * - Schema analysis
 * - Manifests (registration)
 */
import * as Schema from "effect/Schema"
import * as HttpApi from "effect/unstable/httpapi/HttpApi"
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint"
import * as HttpApiError from "effect/unstable/httpapi/HttpApiError"
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup"
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema"
import * as Models from "../core/domain.ts"
import * as Domain from "./domain.ts"
import * as Error from "./error.ts"

// =============================================================================
// Admin API Params
// =============================================================================

/**
 * A URL parameter for the dataset namespace.
 */
const DatasetNamespaceParam = Models.DatasetNamespace

/**
 * A URL parameter for the dataset name.
 */
const DatasetNameParam = Models.DatasetName

/**
 * A URL parameter for the dataset revision.
 */
const DatasetRevisionParam = Models.DatasetRevision

/**
 * A URL parameter for the unique job identifier.
 */
const JobIdParam = Schema.NumberFromString.annotate({
  identifier: "JobId",
  description: "The unique identifier for a job."
})

/**
 * Query parameters for listing jobs.
 */
const JobsQueryParams = Schema.Struct({
  limit: Schema.NumberFromString.pipe(Schema.optional),
  lastJobId: Schema.optional(Schema.NumberFromString),
  status: Schema.String.pipe(Schema.optional)
}).pipe(Schema.encodeKeys({ lastJobId: "last_job_id" }))

// =============================================================================
// Dataset Endpoints
// =============================================================================

// GET /datasets - List all datasets
const getDatasets = HttpApiEndpoint.get("getDatasets", "/datasets", {
  error: [
    Error.ListAllDatasetsError,
    Error.InvalidStoredDatasetNamespaceError,
    Error.InvalidStoredDatasetNameError,
    Error.InvalidStoredDatasetVersionError,
    HttpApiError.Forbidden,
    HttpApiError.Unauthorized
  ],
  success: Domain.GetDatasetsResponse
})

export type GetDatasetsError = (typeof getDatasets)["~Error"]["Type"]

// POST /datasets - Register a dataset
const registerDataset = HttpApiEndpoint.post("registerDataset", "/datasets", {
  payload: Domain.RegisterDatasetPayload,
  error: [
    Error.InvalidPayloadFormatError,
    Error.InvalidManifestError,
    Error.InvalidSortedByConfigError,
    Error.ManifestSerializationError,
    Error.ManifestValidationError,
    Error.SortedTableStatementNotFoundError,
    Error.SortedManifestTableNotFoundError,
    Error.ManifestRegistrationError,
    Error.ManifestLinkingError,
    Error.ManifestNotFoundError,
    Error.NamespaceNotFoundError,
    Error.VersionTaggingError,
    Error.GetDatasetError,
    Error.PhaserProviderNotFoundError,
    Error.PhaserProviderParseFailedError,
    Error.PhaserProviderNetworkMismatchError,
    Error.PhaserConnectionFailedError,
    Error.PhaserDiscoveryFailedError,
    Error.PhaserInvalidDiscoveryError,
    Error.PhaserNoTablesDiscoveredError
  ],
  success: Domain.RegisterDatasetResponse.pipe(HttpApiSchema.status(201))
})

export type RegisterDatasetError = (typeof registerDataset)["~Error"]["Type"]

// GET /datasets/{namespace}/{name}/versions - List versions
const getDatasetVersions = HttpApiEndpoint.get("getDatasetVersions", "/datasets/:namespace/:name/versions", {
  params: {
    namespace: DatasetNamespaceParam,
    name: DatasetNameParam
  },
  error: [
    Error.InvalidPathError,
    Error.NamespaceNotFoundError,
    Error.ListVersionTagsError,
    Error.ResolveRevisionError,
    Error.InvalidStoredDatasetVersionError
  ],
  success: Domain.GetDatasetVersionsResponse
})

export type GetDatasetVersionsError = (typeof getDatasetVersions)["~Error"]["Type"]

// GET /datasets/{namespace}/{name}/versions/{revision} - Get dataset version
const getDatasetVersion = HttpApiEndpoint.get("getDatasetVersion", "/datasets/:namespace/:name/versions/:revision", {
  params: {
    namespace: DatasetNamespaceParam,
    name: DatasetNameParam,
    revision: DatasetRevisionParam
  },
  error: [
    Error.InvalidPathError,
    Error.DatasetNotFoundError,
    Error.ResolveRevisionError,
    Error.GetDatasetError,
    Error.ListDatasetTagsError
  ],
  success: Domain.GetDatasetVersionResponse
})

export type GetDatasetVersionError = (typeof getDatasetVersion)["~Error"]["Type"]

// GET /datasets/{namespace}/{name}/versions/{revision}/manifest - Get manifest
const getDatasetManifest = HttpApiEndpoint.get(
  "getDatasetManifest",
  "/datasets/:namespace/:name/versions/:revision/manifest",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    error: [
      Error.InvalidPathError,
      Error.DatasetNotFoundError,
      Error.ManifestNotFoundError,
      Error.ResolveRevisionError,
      Error.GetManifestPathError,
      Error.ReadManifestError,
      Error.ParseManifestError
    ],
    success: Models.DatasetManifest
  }
)

export type GetDatasetManifestError = (typeof getDatasetManifest)["~Error"]["Type"]

// =============================================================================
// Job Endpoints
// =============================================================================

// GET /jobs - List jobs
const getJobs = HttpApiEndpoint.get("getJobs", "/jobs", {
  query: JobsQueryParams,
  error: [
    Error.InvalidQueryParametersError,
    Error.LimitTooLargeError,
    Error.LimitInvalidError,
    Error.ListJobsError,
    Error.ListJobDescriptorsError,
    Error.DeserializeJobDescriptorError
  ],
  success: Domain.GetJobsResponse
})

export type GetJobsError = (typeof getJobs)["~Error"]["Type"]

// GET /jobs/{jobId} - Get job by ID
const getJobById = HttpApiEndpoint.get("getJobById", "/jobs/:id", {
  params: {
    id: JobIdParam
  },
  error: [
    Error.InvalidJobIdError,
    Error.JobNotFoundError,
    Error.GetJobError,
    Error.GetDescriptorError,
    Error.DeserializeJobDescriptorError
  ],
  success: Models.JobInfo
})

export type GetJobByIdError = (typeof getJobById)["~Error"]["Type"]

// PUT /jobs/{jobId}/stop - Stop job
const stopJob = HttpApiEndpoint.put("stopJob", "/jobs/:id/stop", {
  params: {
    id: JobIdParam
  },
  error: [Error.InvalidJobIdError, Error.JobNotFoundError, Error.StopJobError, Error.UnexpectedStateConflictError],
  success: Schema.Void.pipe(HttpApiSchema.status(200))
})

export type StopJobError = (typeof stopJob)["~Error"]["Type"]

// DELETE /jobs/{jobId} - Delete job
const deleteJob = HttpApiEndpoint.delete("deleteJob", "/jobs/:id", {
  params: {
    id: JobIdParam
  },
  error: [Error.InvalidJobIdError, Error.JobConflictError, Error.GetJobError, Error.DeleteJobError],
  success: Schema.Void.pipe(HttpApiSchema.status(204))
})

export type DeleteJobError = (typeof deleteJob)["~Error"]["Type"]

// =============================================================================
// Worker Endpoints
// =============================================================================

// GET /workers - List workers
const getWorkers = HttpApiEndpoint.get("getWorkers", "/workers", {
  error: Error.SchedulerListWorkersError,
  success: Domain.GetWorkersResponse
})

export type GetWorkersError = (typeof getWorkers)["~Error"]["Type"]

// =============================================================================
// Schema Endpoints
// =============================================================================

// POST /schema - Analyze schema
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema", "/schema", {
  payload: Domain.GetOutputSchemaPayload,
  error: [
    Error.InvalidPayloadFormatError,
    Error.EmptyTablesAndFunctionsError,
    Error.InvalidTableSqlError,
    Error.NonIncrementalQueryError,
    Error.StaticSourceNotMaterializableError,
    Error.SelfReferencingTableError,
    Error.SelfRefTableNotFoundError,
    Error.CyclicDependencyError,
    Error.SortedTableStatementNotFoundError,
    Error.CatalogQualifiedTableError,
    Error.CatalogQualifiedTableInNetworkResolutionError,
    Error.InvalidTableNameError,
    Error.TableReferenceResolutionError,
    Error.DependencyNotFoundError,
    Error.DependencyManifestLinkCheckError,
    Error.DependencyVersionResolutionError,
    Error.NoTableReferencesError,
    Error.SessionConfigError,
    Error.InvalidPlanError,
    Error.SchemaInferenceError,
    Error.MissingBlockNumError,
    Error.MissingTsError,
    Error.DependencyAliasNotFoundError,
    Error.GetDatasetError,
    Error.TableNotFoundInDatasetError,
    Error.SelfRefNetworksNotResolvedError,
    Error.TableReferencesNotResolvedError
  ],
  success: Domain.GetOutputSchemaResponse
})

export type GetOutputSchemaError = (typeof getOutputSchema)["~Error"]["Type"]

// =============================================================================
// Manifest Endpoints
// =============================================================================

// POST /manifests - Register manifest
const registerManifest = HttpApiEndpoint.post("registerManifest", "/manifests", {
  payload: Schema.Any,
  error: [
    Error.InvalidPayloadFormatError,
    Error.InvalidManifestError,
    Error.InvalidSortedByConfigError,
    Error.ManifestSerializationError,
    Error.ManifestValidationError,
    Error.SortedTableStatementNotFoundError,
    Error.SortedManifestTableNotFoundError,
    Error.ManifestStorageError,
    Error.ManifestTransactionBeginError,
    Error.ManifestRegistrationError,
    Error.ManifestTransactionCommitError,
    Error.PhaserProviderNotFoundError,
    Error.PhaserProviderParseFailedError,
    Error.PhaserProviderNetworkMismatchError,
    Error.PhaserConnectionFailedError,
    Error.PhaserDiscoveryFailedError,
    Error.PhaserInvalidDiscoveryError,
    Error.PhaserNoTablesDiscoveredError
  ],
  success: Domain.RegisterManifestResponse.pipe(HttpApiSchema.status(201))
})

export type RegisterManifestError = (typeof registerManifest)["~Error"]["Type"]

// =============================================================================
// Provider Endpoints
// =============================================================================

// GET /providers - List providers
const getProviders = HttpApiEndpoint.get("getProviders", "/providers", {
  success: Domain.GetProvidersResponse
})

export type GetProvidersError = (typeof getProviders)["~Error"]["Type"]

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
  .add(getDatasetManifest) {}

/**
 * The api group for the job endpoints.
 */
export class JobGroup extends HttpApiGroup.make("job").add(getJobs).add(getJobById).add(stopJob).add(deleteJob) {}

/**
 * The api group for the worker endpoints.
 */
export class WorkerGroup extends HttpApiGroup.make("worker").add(getWorkers) {}

/**
 * The api group for the schema endpoints.
 */
export class SchemaGroup extends HttpApiGroup.make("schema").add(getOutputSchema) {}

/**
 * The api group for the manifest endpoints.
 */
export class ManifestGroup extends HttpApiGroup.make("manifest").add(registerManifest) {}

/**
 * The api group for the provider endpoints.
 */
export class ProviderGroup extends HttpApiGroup.make("provider").add(getProviders) {}

// =============================================================================
// Admin API
// =============================================================================

/**
 * The specification for the Amp administration API.
 */
export class Api extends HttpApi.make("AmpAdminApi")
  .add(DatasetGroup)
  .add(JobGroup)
  .add(WorkerGroup)
  .add(SchemaGroup)
  .add(ManifestGroup)
  .add(ProviderGroup) {}
