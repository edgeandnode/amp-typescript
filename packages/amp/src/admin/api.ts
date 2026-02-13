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
import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import * as Schema from "effect/Schema"
import * as Models from "../core/domain.ts"
import * as Domain from "./domain.ts"
import * as Error from "./error.ts"

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

/**
 * Query parameters for listing jobs.
 */
const jobsQueryParams = Schema.Struct({
  limit: Schema.NumberFromString.pipe(Schema.optional),
  lastJobId: Schema.NumberFromString.pipe(
    Schema.optional,
    Schema.fromKey("last_job_id")
  ),
  status: Schema.String.pipe(Schema.optional)
})

// =============================================================================
// Dataset Endpoints
// =============================================================================

// GET /datasets - List all datasets
const getDatasets = HttpApiEndpoint.get("getDatasets")`/datasets`
  .addError(Error.DatasetStoreError)
  .addError(Error.MetadataDbError)
  .addError(Error.ListAllDatasetsError)
  .addSuccess(Domain.GetDatasetsResponse)

/**
 * Error type for the `getDatasets` endpoint.
 */
export type GetDatasetsError =
  | Error.DatasetStoreError
  | Error.MetadataDbError
  | Error.ListAllDatasetsError

// POST /datasets - Register a dataset
const registerDataset = HttpApiEndpoint.post("registerDataset")`/datasets`
  .addError(Error.InvalidPayloadFormatError)
  .addError(Error.InvalidManifestError)
  .addError(Error.ManifestLinkingError)
  .addError(Error.ManifestNotFoundError)
  .addError(Error.ManifestRegistrationError)
  .addError(Error.ManifestValidationError)
  .addError(Error.StoreError)
  .addError(Error.UnsupportedDatasetKindError)
  .addError(Error.VersionTaggingError)
  .addSuccess(Schema.Void, { status: 201 })
  .setPayload(Domain.RegisterDatasetPayload)

/**
 * Error type for the `registerDataset` endpoint.
 */
export type RegisterDatasetError =
  | Error.InvalidPayloadFormatError
  | Error.InvalidManifestError
  | Error.ManifestLinkingError
  | Error.ManifestNotFoundError
  | Error.ManifestRegistrationError
  | Error.ManifestValidationError
  | Error.StoreError
  | Error.UnsupportedDatasetKindError
  | Error.VersionTaggingError

// GET /datasets/{namespace}/{name}/versions - List versions
const getDatasetVersions = HttpApiEndpoint.get(
  "getDatasetVersions"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions`
  .addError(Error.DatasetStoreError)
  .addError(Error.InvalidRequestError)
  .addError(Error.MetadataDbError)
  .addError(Error.ListVersionTagsError)
  .addSuccess(Domain.GetDatasetVersionsResponse)

/**
 * Error type for the `getDatasetVersions` endpoint.
 */
export type GetDatasetVersionsError =
  | Error.DatasetStoreError
  | Error.InvalidRequestError
  | Error.MetadataDbError
  | Error.ListVersionTagsError

// GET /datasets/{namespace}/{name}/versions/{revision} - Get dataset version
const getDatasetVersion = HttpApiEndpoint.get(
  "getDatasetVersion"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}`
  .addError(Error.DatasetNotFoundError)
  .addError(Error.DatasetStoreError)
  .addError(Error.InvalidPathError)
  .addError(Error.MetadataDbError)
  .addError(Error.ResolveRevisionError)
  .addError(Error.GetManifestPathError)
  .addError(Error.ReadManifestError)
  .addError(Error.ParseManifestError)
  .addSuccess(Domain.GetDatasetVersionResponse)

/**
 * Error type for the `getDatasetVersion` endpoint.
 */
export type GetDatasetVersionError =
  | Error.DatasetNotFoundError
  | Error.DatasetStoreError
  | Error.InvalidPathError
  | Error.MetadataDbError
  | Error.ResolveRevisionError
  | Error.GetManifestPathError
  | Error.ReadManifestError
  | Error.ParseManifestError

// POST /datasets/{namespace}/{name}/versions/{revision}/deploy - Deploy dataset
const deployDataset = HttpApiEndpoint.post(
  "deployDataset"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/deploy`
  .addError(Error.DatasetNotFoundError)
  .addError(Error.DatasetStoreError)
  .addError(Error.InvalidPathError)
  .addError(Error.InvalidBodyError)
  .addError(Error.MetadataDbError)
  .addError(Error.SchedulerError)
  .addError(Error.ResolveRevisionError)
  .addError(Error.GetDatasetError)
  .addError(Error.ListVersionTagsError)
  .addError(Error.WorkerNotAvailableError)
  .addSuccess(Domain.DeployDatasetResponse, { status: 202 })
  .setPayload(Domain.DeployDatasetPayload)

/**
 * Error type for the `deployDataset` endpoint.
 */
export type DeployDatasetError =
  | Error.DatasetNotFoundError
  | Error.DatasetStoreError
  | Error.InvalidPathError
  | Error.InvalidBodyError
  | Error.MetadataDbError
  | Error.SchedulerError
  | Error.ResolveRevisionError
  | Error.GetDatasetError
  | Error.ListVersionTagsError
  | Error.WorkerNotAvailableError

// GET /datasets/{namespace}/{name}/versions/{revision}/manifest - Get manifest
const getDatasetManifest = HttpApiEndpoint.get(
  "getDatasetManifest"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/manifest`
  .addError(Error.DatasetNotFoundError)
  .addError(Error.DatasetStoreError)
  .addError(Error.InvalidPathError)
  .addError(Error.MetadataDbError)
  .addError(Error.GetManifestPathError)
  .addError(Error.ReadManifestError)
  .addError(Error.ParseManifestError)
  .addSuccess(Models.DatasetManifest)

/**
 * Error type for the `getDatasetManifest` endpoint.
 */
export type GetDatasetManifestError =
  | Error.DatasetNotFoundError
  | Error.DatasetStoreError
  | Error.InvalidPathError
  | Error.MetadataDbError
  | Error.GetManifestPathError
  | Error.ReadManifestError
  | Error.ParseManifestError

// GET /datasets/{namespace}/{name}/versions/{revision}/sync-progress - Get sync progress
const getDatasetSyncProgress = HttpApiEndpoint.get(
  "getDatasetSyncProgress"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/sync-progress`
  .addError(Error.DatasetNotFoundError)
  .addError(Error.GetDatasetError)
  .addError(Error.GetSyncProgressError)
  .addError(Error.InvalidPathParamsError)
  .addError(Error.ResolveRevisionError)
  .addError(Error.PhysicalTableError)
  .addSuccess(Domain.GetDatasetSyncProgressResponse)

/**
 * Error type for the `getDatasetSyncProgress` endpoint.
 */
export type GetDatasetSyncProgressError =
  | Error.DatasetNotFoundError
  | Error.GetDatasetError
  | Error.GetSyncProgressError
  | Error.InvalidPathParamsError
  | Error.ResolveRevisionError
  | Error.PhysicalTableError

// =============================================================================
// Job Endpoints
// =============================================================================

// GET /jobs - List jobs
const getJobs = HttpApiEndpoint.get("getJobs")`/jobs`
  .addError(Error.InvalidQueryParametersError)
  .addError(Error.LimitTooLargeError)
  .addError(Error.LimitInvalidError)
  .addError(Error.ListJobsError)
  .addSuccess(Domain.GetJobsResponse)
  .setUrlParams(jobsQueryParams)

/**
 * Error type for the `getJobs` endpoint.
 */
export type GetJobsError =
  | Error.InvalidQueryParametersError
  | Error.LimitTooLargeError
  | Error.LimitInvalidError
  | Error.ListJobsError

// GET /jobs/{jobId} - Get job by ID
const getJobById = HttpApiEndpoint.get("getJobById")`/jobs/${jobIdParam}`
  .addError(Error.InvalidJobIdError)
  .addError(Error.JobNotFoundError)
  .addError(Error.GetJobError)
  .addSuccess(Models.JobInfo)

/**
 * Error type for the `getJobById` endpoint.
 */
export type GetJobByIdError =
  | Error.InvalidJobIdError
  | Error.JobNotFoundError
  | Error.GetJobError

// PUT /jobs/{jobId}/stop - Stop job
const stopJob = HttpApiEndpoint.put("stopJob")`/jobs/${jobIdParam}/stop`
  .addError(Error.InvalidJobIdError)
  .addError(Error.JobNotFoundError)
  .addError(Error.StopJobError)
  .addError(Error.UnexpectedStateConflictError)
  .addSuccess(Schema.Void, { status: 200 })

/**
 * Error type for the `stopJob` endpoint.
 */
export type StopJobError =
  | Error.InvalidJobIdError
  | Error.JobNotFoundError
  | Error.StopJobError
  | Error.UnexpectedStateConflictError

// DELETE /jobs/{jobId} - Delete job
const deleteJob = HttpApiEndpoint.del("deleteJob")`/jobs/${jobIdParam}`
  .addError(Error.InvalidJobIdError)
  .addError(Error.JobConflictError)
  .addError(Error.GetJobError)
  .addError(Error.DeleteJobError)
  .addSuccess(Schema.Void, { status: 204 })

/**
 * Error type for the `deleteJob` endpoint.
 */
export type DeleteJobError =
  | Error.InvalidJobIdError
  | Error.JobConflictError
  | Error.GetJobError
  | Error.DeleteJobError

// =============================================================================
// Worker Endpoints
// =============================================================================

// GET /workers - List workers
const getWorkers = HttpApiEndpoint.get("getWorkers")`/workers`
  .addError(Error.SchedulerListWorkersError)
  .addSuccess(Domain.GetWorkersResponse)

/**
 * Error type for the `getWorkers` endpoint.
 */
export type GetWorkersError = Error.SchedulerListWorkersError

// =============================================================================
// Schema Endpoints
// =============================================================================

// POST /schema - Analyze schema
const getOutputSchema = HttpApiEndpoint.post("getOutputSchema")`/schema`
  .addError(Error.CatalogQualifiedTableError)
  .addError(Error.CatalogQualifiedFunctionError)
  .addError(Error.DatasetNotFoundError)
  .addError(Error.DependencyAliasNotFoundError)
  .addError(Error.DependencyNotFoundError)
  .addError(Error.DependencyResolutionError)
  .addError(Error.EmptyTablesAndFunctionsError)
  .addError(Error.EthCallNotAvailableError)
  .addError(Error.EthCallUdfCreationError)
  .addError(Error.FunctionNotFoundInDatasetError)
  .addError(Error.FunctionReferenceResolutionError)
  .addError(Error.GetDatasetError)
  .addError(Error.InvalidPayloadFormatError)
  .addError(Error.InvalidTableNameError)
  .addError(Error.InvalidTableSqlError)
  .addError(Error.InvalidDependencyAliasForTableRefError)
  .addError(Error.InvalidDependencyAliasForFunctionRefError)
  .addError(Error.NonIncrementalQueryError)
  .addError(Error.SchemaInferenceError)
  .addError(Error.TableNotFoundInDatasetError)
  .addError(Error.TableReferenceResolutionError)
  .addError(Error.UnqualifiedTableError)
  .addSuccess(Domain.GetOutputSchemaResponse)
  .setPayload(Domain.GetOutputSchemaPayload)

/**
 * Error type for the `getOutputSchema` endpoint.
 */
export type GetOutputSchemaError =
  | Error.CatalogQualifiedTableError
  | Error.CatalogQualifiedFunctionError
  | Error.DatasetNotFoundError
  | Error.DependencyAliasNotFoundError
  | Error.DependencyNotFoundError
  | Error.DependencyResolutionError
  | Error.EmptyTablesAndFunctionsError
  | Error.EthCallNotAvailableError
  | Error.EthCallUdfCreationError
  | Error.FunctionNotFoundInDatasetError
  | Error.FunctionReferenceResolutionError
  | Error.GetDatasetError
  | Error.InvalidPayloadFormatError
  | Error.InvalidTableNameError
  | Error.InvalidTableSqlError
  | Error.InvalidDependencyAliasForTableRefError
  | Error.InvalidDependencyAliasForFunctionRefError
  | Error.NonIncrementalQueryError
  | Error.SchemaInferenceError
  | Error.TableNotFoundInDatasetError
  | Error.TableReferenceResolutionError
  | Error.UnqualifiedTableError

// =============================================================================
// Manifest Endpoints
// =============================================================================

// POST /manifests - Register manifest
const registerManifest = HttpApiEndpoint.post("registerManifest")`/manifests`
  .addError(Error.InvalidPayloadFormatError)
  .addError(Error.InvalidManifestError)
  .addError(Error.ManifestValidationError)
  .addError(Error.ManifestStorageError)
  .addError(Error.ManifestRegistrationError)
  .addError(Error.UnsupportedDatasetKindError)
  .addSuccess(Domain.RegisterManifestResponse, { status: 201 })
  .setPayload(Schema.Any)

/**
 * Error type for the `registerManifest` endpoint.
 */
export type RegisterManifestError =
  | Error.InvalidPayloadFormatError
  | Error.InvalidManifestError
  | Error.ManifestValidationError
  | Error.ManifestStorageError
  | Error.ManifestRegistrationError
  | Error.UnsupportedDatasetKindError

// =============================================================================
// Provider Endpoints
// =============================================================================

// GET /providers - List providers
const getProviders = HttpApiEndpoint.get("getProviders")`/providers`
  .addSuccess(Domain.GetProvidersResponse)

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
  .addError(HttpApiError.Forbidden)
  .addError(HttpApiError.Unauthorized)
{}
