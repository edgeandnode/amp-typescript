/**
 * This module contains the HttpApi definitions for the Amp Admin API.
 *
 * The Admin API provides operations for managing:
 * - Datasets (registration, versioning, deployment)
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
    Error.DatasetStoreError,
    Error.MetadataDbError,
    Error.ListAllDatasetsError,
    HttpApiError.Forbidden,
    HttpApiError.Unauthorized
  ],
  success: Domain.GetDatasetsResponse
})

export type GetDatasetsError = typeof getDatasets["~Error"]["Type"]

// POST /datasets - Register a dataset
const registerDataset = HttpApiEndpoint.post("registerDataset", "/datasets", {
  payload: Domain.RegisterDatasetPayload,
  error: [
    Error.InvalidPayloadFormatError,
    Error.InvalidManifestError,
    Error.ManifestLinkingError,
    Error.ManifestNotFoundError,
    Error.ManifestRegistrationError,
    Error.ManifestValidationError,
    Error.StoreError,
    Error.UnsupportedDatasetKindError,
    Error.VersionTaggingError
  ],
  success: Schema.Void
})

export type RegisterDatasetError = typeof registerDataset["~Error"]["Type"]

// GET /datasets/{namespace}/{name}/versions - List versions
const getDatasetVersions = HttpApiEndpoint.get(
  "getDatasetVersions",
  "/datasets/:namespace/:name/versions",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    error: [
      Error.DatasetStoreError,
      Error.InvalidRequestError,
      Error.MetadataDbError,
      Error.ListVersionTagsError
    ],
    success: Domain.GetDatasetVersionsResponse
  }
)

export type GetDatasetVersionsError = typeof getDatasetVersions["~Error"]["Type"]

// GET /datasets/{namespace}/{name}/versions/{revision} - Get dataset version
const getDatasetVersion = HttpApiEndpoint.get(
  "getDatasetVersion",
  "/datasets/:namespace/:name/versions/:revision",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    error: [
      Error.DatasetNotFoundError,
      Error.DatasetStoreError,
      Error.InvalidPathError,
      Error.MetadataDbError,
      Error.ResolveRevisionError,
      Error.GetManifestPathError,
      Error.ReadManifestError,
      Error.ParseManifestError
    ],
    success: Domain.GetDatasetVersionResponse
  }
)

export type GetDatasetVersionError = typeof getDatasetVersion["~Error"]["Type"]

// POST /datasets/{namespace}/{name}/versions/{revision}/deploy - Deploy dataset
const deployDataset = HttpApiEndpoint.post(
  "deployDataset",
  "/datasets/:namespace/:name/versions/:revision/deploy",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    payload: Domain.DeployDatasetPayload,
    error: [
      Error.DatasetNotFoundError,
      Error.DatasetStoreError,
      Error.InvalidPathError,
      Error.InvalidBodyError,
      Error.MetadataDbError,
      Error.SchedulerError,
      Error.ResolveRevisionError,
      Error.GetDatasetError,
      Error.ListVersionTagsError,
      Error.WorkerNotAvailableError
    ],
    success: Domain.DeployDatasetResponse.pipe(HttpApiSchema.status(202))
  }
)

export type DeployDatasetError = typeof deployDataset["~Error"]["Type"]

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
      Error.DatasetNotFoundError,
      Error.DatasetStoreError,
      Error.InvalidPathError,
      Error.MetadataDbError,
      Error.GetManifestPathError,
      Error.ReadManifestError,
      Error.ParseManifestError
    ],
    success: Models.DatasetManifest
  }
)

export type GetDatasetManifestError = typeof getDatasetManifest["~Error"]["Type"]

// GET /datasets/{namespace}/{name}/versions/{revision}/sync-progress - Get sync progress
const getDatasetSyncProgress = HttpApiEndpoint.get(
  "getDatasetSyncProgress",
  "/datasets/:namespace/:name/versions/:revision/sync-progress",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    error: [
      Error.DatasetNotFoundError,
      Error.GetDatasetError,
      Error.GetSyncProgressError,
      Error.InvalidPathParamsError,
      Error.ResolveRevisionError,
      Error.PhysicalTableError
    ],
    success: Domain.GetDatasetSyncProgressResponse
  }
)

export type GetDatasetSyncProgressError = typeof getDatasetSyncProgress["~Error"]["Type"]

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
    Error.ListJobsError
  ],
  success: Domain.GetJobsResponse
})

export type GetJobsError = typeof getJobs["~Error"]["Type"]

// GET /jobs/{jobId} - Get job by ID
const getJobById = HttpApiEndpoint.get("getJobById", "/jobs/:id", {
  params: {
    id: JobIdParam
  },
  error: [
    Error.InvalidJobIdError,
    Error.JobNotFoundError,
    Error.GetJobError
  ],
  success: Models.JobInfo
})

export type GetJobByIdError = typeof getJobById["~Error"]["Type"]

// PUT /jobs/{jobId}/stop - Stop job
const stopJob = HttpApiEndpoint.put("stopJob", "/jobs/:id/stop", {
  params: {
    id: JobIdParam
  },
  error: [
    Error.InvalidJobIdError,
    Error.JobNotFoundError,
    Error.StopJobError,
    Error.UnexpectedStateConflictError
  ],
  success: Schema.Void.pipe(HttpApiSchema.status(200))
})

export type StopJobError = typeof stopJob["~Error"]["Type"]

// DELETE /jobs/{jobId} - Delete job
const deleteJob = HttpApiEndpoint.delete("deleteJob", "/jobs/:id", {
  params: {
    id: JobIdParam
  },
  error: [
    Error.InvalidJobIdError,
    Error.JobConflictError,
    Error.GetJobError,
    Error.DeleteJobError
  ],
  success: Schema.Void.pipe(HttpApiSchema.status(204))
})

export type DeleteJobError = typeof deleteJob["~Error"]["Type"]

// =============================================================================
// Worker Endpoints
// =============================================================================

// GET /workers - List workers
const getWorkers = HttpApiEndpoint.get("getWorkers", "/workers", {
  error: Error.SchedulerListWorkersError,
  success: Domain.GetWorkersResponse
})

export type GetWorkersError = typeof getWorkers["~Error"]["Type"]

// =============================================================================
// Schema Endpoints
// =============================================================================

// POST /schema - Analyze schema
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema", "/schema", {
  payload: Domain.GetOutputSchemaPayload,
  error: [
    Error.CatalogQualifiedTableError,
    Error.CatalogQualifiedFunctionError,
    Error.DatasetNotFoundError,
    Error.DependencyAliasNotFoundError,
    Error.DependencyNotFoundError,
    Error.DependencyResolutionError,
    Error.EmptyTablesAndFunctionsError,
    Error.EthCallNotAvailableError,
    Error.EthCallUdfCreationError,
    Error.FunctionNotFoundInDatasetError,
    Error.FunctionReferenceResolutionError,
    Error.GetDatasetError,
    Error.InvalidPayloadFormatError,
    Error.InvalidTableNameError,
    Error.InvalidTableSqlError,
    Error.InvalidDependencyAliasForTableRefError,
    Error.InvalidDependencyAliasForFunctionRefError,
    Error.NonIncrementalQueryError,
    Error.SchemaInferenceError,
    Error.TableNotFoundInDatasetError,
    Error.TableReferenceResolutionError,
    Error.UnqualifiedTableError
  ],
  success: Domain.GetOutputSchemaResponse
})

export type GetOutputSchemaError = typeof getOutputSchema["~Error"]["Type"]

// =============================================================================
// Manifest Endpoints
// =============================================================================

// POST /manifests - Register manifest
const registerManifest = HttpApiEndpoint.post("registerManifest", "/manifests", {
  payload: Schema.Any,
  error: [
    Error.InvalidPayloadFormatError,
    Error.InvalidManifestError,
    Error.ManifestValidationError,
    Error.ManifestStorageError,
    Error.ManifestRegistrationError,
    Error.UnsupportedDatasetKindError
  ],
  success: Domain.RegisterManifestResponse.pipe(HttpApiSchema.status(201))
})

export type RegisterManifestError = typeof registerManifest["~Error"]["Type"]

// =============================================================================
// Provider Endpoints
// =============================================================================

// GET /providers - List providers
const getProviders = HttpApiEndpoint.get("getProviders", "/providers", {
  success: Domain.GetProvidersResponse
})

export type GetProvidersError = typeof getProviders["~Error"]["Type"]

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
  .add(getDatasetSyncProgress)
{}

/**
 * The api group for the job endpoints.
 */
export class JobGroup extends HttpApiGroup.make("job")
  .add(getJobs)
  .add(getJobById)
  .add(stopJob)
  .add(deleteJob)
{}

/**
 * The api group for the worker endpoints.
 */
export class WorkerGroup extends HttpApiGroup.make("worker")
  .add(getWorkers)
{}

/**
 * The api group for the schema endpoints.
 */
export class SchemaGroup extends HttpApiGroup.make("schema")
  .add(getOutputSchema)
{}

/**
 * The api group for the manifest endpoints.
 */
export class ManifestGroup extends HttpApiGroup.make("manifest")
  .add(registerManifest)
{}

/**
 * The api group for the provider endpoints.
 */
export class ProviderGroup extends HttpApiGroup.make("provider")
  .add(getProviders)
{}

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
  .add(ProviderGroup)
{}
