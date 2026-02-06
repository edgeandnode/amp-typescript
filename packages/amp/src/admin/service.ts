/**
 * This module provides the AdminApi service for interacting with the Amp Admin API.
 *
 * The Admin API allows managing:
 * - Datasets (registration, versioning, deployment, sync progress)
 * - Jobs (listing, stopping, deletion)
 * - Workers (listing)
 * - Providers (listing)
 * - Schema analysis
 * - Manifests (registration)
 */
import * as HttpApiClient from "@effect/platform/HttpApiClient"
import type * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpClient from "@effect/platform/HttpClient"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import type * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import { constUndefined } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Auth from "../auth/service.ts"
import type * as Models from "../models.ts"
import * as Api from "./api.ts"
import type * as Domain from "./domain.ts"

// =============================================================================
// Admin API Service Types
// =============================================================================

/**
 * Represents possible errors that can occur when performing HTTP requests.
 */
export type HttpError =
  | HttpApiError.Forbidden
  | HttpApiError.Unauthorized
  | HttpClientError.HttpClientError

// =============================================================================
// Admin API Service
// =============================================================================

/**
 * A service which can be used to execute operations against the Amp admin API.
 */
export class AdminApi extends Context.Tag("Amp/AdminApi")<AdminApi, {
  /**
   * Register a dataset manifest.
   *
   * @param namespace The namespace of the dataset to register.
   * @param name The name of the dataset to register.
   * @param manifest The dataset manifest to register.
   * @param version Optional version of the dataset to register. If omitted, only the "dev" tag is updated.
   * @return Whether the registration was successful.
   */
  readonly registerDataset: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    manifest: Models.DatasetManifest,
    version?: Models.DatasetRevision | undefined
  ) => Effect.Effect<void, HttpError | Api.RegisterDatasetError>

  /**
   * Get all datasets.
   *
   * @return The list of all datasets.
   */
  readonly getDatasets: Effect.Effect<
    Domain.GetDatasetsResponse,
    HttpError | Api.GetDatasetsError
  >

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
  ) => Effect.Effect<
    Domain.GetDatasetVersionsResponse,
    HttpError | Api.GetDatasetVersionsError
  >

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
  ) => Effect.Effect<
    Domain.GetDatasetVersionResponse,
    HttpError | Api.GetDatasetVersionError
  >

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
      workerId?: string | undefined
    } | undefined
  ) => Effect.Effect<
    Domain.DeployDatasetResponse,
    HttpError | Api.DeployDatasetError
  >

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
  ) => Effect.Effect<Models.DatasetManifest, HttpError | Api.GetDatasetManifestError>

  /**
   * Retrieves sync progress information for a specific dataset revision,
   * including per-table current block numbers, job status, and file statistics.
   *
   * @param namespace The namespace of the dataset.
   * @param name The name of the dataset.
   * @param revision The version/revision of the dataset.
   * @return The dataset sync progress.
   */
  readonly getDatasetSyncProgress: (
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    revision: Models.DatasetRevision
  ) => Effect.Effect<
    Domain.GetDatasetSyncProgressResponse,
    HttpError | Api.GetDatasetSyncProgressError
  >

  /**
   * Get all jobs with optional pagination and filtering.
   *
   * @param options Pagination and filtering options.
   * @return The list of jobs with pagination cursor.
   */
  readonly getJobs: (options?: {
    limit?: number | undefined
    lastJobId?: number | undefined
    status?: string | undefined
  }) => Effect.Effect<
    Domain.GetJobsResponse,
    HttpError | Api.GetJobsError
  >

  /**
   * Get a job by ID.
   *
   * @param jobId The ID of the job to get.
   * @return The job information.
   */
  readonly getJobById: (
    jobId: number
  ) => Effect.Effect<Models.JobInfo, HttpError | Api.GetJobByIdError>

  /**
   * Stop a job by ID.
   *
   * @param jobId The ID of the job to stop.
   * @return Void on success.
   */
  readonly stopJob: (
    jobId: number
  ) => Effect.Effect<void, HttpError | Api.StopJobError>

  /**
   * Delete a job by ID.
   *
   * @param jobId The ID of the job to delete.
   * @return Void on success.
   */
  readonly deleteJob: (
    jobId: number
  ) => Effect.Effect<void, HttpError | Api.DeleteJobError>

  /**
   * Get all workers.
   *
   * @return The list of workers.
   */
  readonly getWorkers: Effect.Effect<
    Domain.GetWorkersResponse,
    HttpError | Api.GetWorkersError
  >

  /**
   * Get all providers.
   *
   * @return The list of providers.
   */
  readonly getProviders: Effect.Effect<Domain.GetProvidersResponse, HttpError>

  /**
   * Register a manifest.
   *
   * @param manifest The manifest to register.
   * @return The registered manifest response with hash.
   */
  readonly registerManifest: (
    manifest: unknown
  ) => Effect.Effect<
    Domain.RegisterManifestResponse,
    HttpError | Api.RegisterManifestError
  >

  /**
   * Gets the schema of a dataset.
   *
   * @param request - The schema request with tables and dependencies.
   * @returns An effect that resolves to the schema response.
   */
  readonly getOutputSchema: (
    request: Domain.GetOutputSchemaPayload
  ) => Effect.Effect<
    Domain.GetOutputSchemaResponse,
    HttpError | Api.GetOutputSchemaError
  >
}>() {}

export interface MakeOptions {
  readonly url: string | URL
}

const make = Effect.fnUntraced(function*(options: MakeOptions) {
  type Service = typeof AdminApi.Service

  const auth = yield* Effect.serviceOption(Auth.Auth)

  const client = yield* HttpApiClient.make(Api.Api, {
    baseUrl: options.url,
    transformClient: Option.match(auth, {
      onNone: constUndefined,
      onSome: (auth) =>
        HttpClient.mapRequestEffect(
          Effect.fnUntraced(function*(request) {
            const authInfo = yield* auth.getCachedAuthInfo.pipe(
              // Treat cache errors as "no auth available"
              Effect.catchAll(() => Effect.succeed(Option.none()))
            )
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
        parallelism: options?.parallelism,
        workerId: options?.workerId
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

  const getDatasets: Service["getDatasets"] = Effect.gen(function*() {
    return yield* client.dataset.getDatasets({})
  }).pipe(
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die),
    Effect.withSpan("AdminApi.getDatasets")
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

  const getDatasetSyncProgress: Service["getDatasetSyncProgress"] = Effect.fn("AdminApi.getDatasetSyncProgress")(
    function*(namespace, name, revision) {
      const path = { namespace, name, revision }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.dataset.getDatasetSyncProgress({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  // Job Operations

  const getJobs: Service["getJobs"] = Effect.fn("AdminApi.getJobs")(
    function*(options) {
      const urlParams = {
        limit: options?.limit,
        lastJobId: options?.lastJobId,
        status: options?.status
      }
      yield* Effect.annotateCurrentSpan(urlParams)
      return yield* client.job.getJobs({ urlParams })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const getJobById: Service["getJobById"] = Effect.fn("AdminApi.getJobById")(
    function*(jobId) {
      const path = { jobId }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.job.getJobById({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const stopJob: Service["stopJob"] = Effect.fn("AdminApi.stopJob")(
    function*(jobId) {
      const path = { jobId }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.job.stopJob({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  const deleteJob: Service["deleteJob"] = Effect.fn("AdminApi.deleteJob")(
    function*(jobId) {
      const path = { jobId }
      yield* Effect.annotateCurrentSpan(path)
      return yield* client.job.deleteJob({ path })
    },
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die)
  )

  // Worker Operations

  const getWorkers: Service["getWorkers"] = Effect.gen(function*() {
    return yield* client.worker.getWorkers({})
  }).pipe(
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die),
    Effect.withSpan("AdminApi.getWorkers")
  )

  // Provider Operations

  const getProviders: Service["getProviders"] = Effect.gen(function*() {
    return yield* client.provider.getProviders({})
  }).pipe(
    Effect.catchTag("HttpApiDecodeError", "ParseError", Effect.die),
    Effect.withSpan("AdminApi.getProviders")
  )

  // Manifest Operations

  const registerManifest: Service["registerManifest"] = Effect.fn("AdminApi.registerManifest")(
    function*(manifest) {
      return yield* client.manifest.registerManifest({ payload: manifest })
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
    getDatasetSyncProgress,
    registerDataset,
    getJobs,
    getJobById,
    stopJob,
    deleteJob,
    getWorkers,
    getProviders,
    registerManifest,
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
