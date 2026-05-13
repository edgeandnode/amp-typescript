/**
 * This module contains domain models and schemas for the Admin API requests
 * and responses.
 */
import * as Schema from "effect/Schema"
import * as Models from "../core/domain.ts"

// =============================================================================
// Dataset Request/Response Schemas
// =============================================================================

/**
 * Response schema for listing datasets.
 */
export const GetDatasetsResponse = Schema.Struct({
  datasets: Schema.Array(
    Schema.Struct({
      namespace: Models.DatasetNamespace,
      name: Models.DatasetName,
      versions: Schema.Array(Models.DatasetVersion),
      latestVersion: Schema.optional(Models.DatasetVersion)
    }).pipe(Schema.encodeKeys({ latestVersion: "latest_version" }))
  )
}).annotate({ identifier: "GetDatasetsResponse" })

export type GetDatasetsResponse = typeof GetDatasetsResponse.Type

/**
 * Request payload for registering a dataset.
 */
export const RegisterDatasetPayload = Schema.Struct({
  namespace: Schema.String,
  name: Schema.String,
  version: Schema.optional(Schema.String),
  manifest: Models.DatasetManifest
}).annotate({ identifier: "RegisterDatasetPayload" })

export type RegisterDatasetPayload = typeof RegisterDatasetPayload.Type

/**
 * Response schema for getting a dataset version.
 */
export const GetDatasetVersionResponse = Schema.Struct({
  kind: Models.DatasetKind,
  namespace: Models.DatasetNamespace,
  name: Models.DatasetName,
  revision: Models.DatasetRevision,
  manifestHash: Models.DatasetHash
}).pipe(Schema.encodeKeys({ manifestHash: "manifest_hash" })).annotate({ identifier: "GetDatasetVersionResponse" })

export type GetDatasetVersionResponse = typeof GetDatasetVersionResponse.Type

/**
 * Response schema for listing dataset versions.
 */
export const GetDatasetVersionsResponse = Schema.Struct({
  versions: Schema.Array(Models.DatasetVersion)
}).annotate({ identifier: "GetDatasetVersionsResponse" })

export type GetDatasetVersionsResponse = typeof GetDatasetVersionsResponse.Type

/**
 * Request payload for deploying a dataset.
 */
export const DeployDatasetPayload = Schema.Struct({
  endBlock: Schema.optional(Schema.NullOr(Schema.String)),
  parallelism: Schema.optional(Schema.Number),
  workerId: Schema.optional(Schema.String)
}).pipe(Schema.encodeKeys({
  endBlock: "end_block",
  workerId: "worker_id"
})).annotate({ identifier: "DeployDatasetPayload" })

export type DeployDatasetPayload = typeof DeployDatasetPayload.Type

/**
 * Response schema for deploying a dataset.
 */
export const DeployDatasetResponse = Schema.Struct({
  jobId: Models.JobId
}).pipe(Schema.encodeKeys({ jobId: "job_id" })).annotate({ identifier: "DeployDatasetResponse" })

export type DeployDatasetResponse = typeof DeployDatasetResponse.Type

/**
 * Table sync progress information.
 */
export const TableSyncProgress = Schema.Struct({
  tableName: Schema.String,
  currentBlock: Schema.optional(Schema.Int),
  startBlock: Schema.optional(Schema.Int),
  jobId: Schema.optional(Models.JobId),
  jobStatus: Schema.optional(Models.JobStatus),
  filesCount: Schema.Int,
  totalSizeBytes: Schema.Int
}).pipe(Schema.encodeKeys({
  tableName: "table_name",
  currentBlock: "current_block",
  startBlock: "start_block",
  jobId: "job_id",
  jobStatus: "job_status",
  filesCount: "files_count",
  totalSizeBytes: "total_size_bytes"
})).annotate({ identifier: "TableSyncProgress" })

export type TableSyncProgress = typeof TableSyncProgress.Type

/**
 * Response schema for getting dataset sync progress.
 */
export const GetDatasetSyncProgressResponse = Schema.Struct({
  namespace: Models.DatasetNamespace,
  name: Models.DatasetName,
  revision: Models.DatasetRevision,
  manifestHash: Models.DatasetHash,
  tables: Schema.Array(TableSyncProgress)
}).pipe(Schema.encodeKeys({
  namespace: "dataset_namespace",
  name: "dataset_name",
  manifestHash: "manifest_hash"
})).annotate({ identifier: "GetDatasetSyncProgressResponse" })

export type GetDatasetSyncProgressResponse = typeof GetDatasetSyncProgressResponse.Type

// =============================================================================
// Job Request/Response Schemas
// =============================================================================

/**
 * Response schema for listing jobs.
 */
export const GetJobsResponse = Schema.Struct({
  jobs: Schema.Array(Models.JobInfo),
  nextCursor: Schema.optional(Models.JobId)
}).pipe(Schema.encodeKeys({ nextCursor: "next_cursor" })).annotate({ identifier: "GetJobsResponse" })

export type GetJobsResponse = typeof GetJobsResponse.Type

// =============================================================================
// Schema Request/Response Schemas
// =============================================================================

/**
 * Request payload for schema analysis.
 */
export const GetOutputSchemaPayload = Schema.Struct({
  tables: Schema.Record(Schema.String, Schema.String),
  dependencies: Schema.optional(Schema.Record(
    Schema.String,
    Models.DatasetReferenceFromString
  )),
  functions: Schema.optional(Schema.Record(
    Schema.String,
    Models.FunctionDefinition
  ))
}).annotate({ identifier: "GetOutputSchemaPayload" })

export type GetOutputSchemaPayload = typeof GetOutputSchemaPayload.Type

/**
 * Response schema for schema analysis.
 */
export const GetOutputSchemaResponse = Schema.Struct({
  schemas: Schema.Record(
    Schema.String,
    Models.TableSchemaWithNetworks
  )
}).annotate({ identifier: "GetOutputSchemaResponse" })

export type GetOutputSchemaResponse = typeof GetOutputSchemaResponse.Type

// =============================================================================
// Worker Request/Response Schemas
// =============================================================================

/**
 * Worker information returned by the API.
 */
export const WorkerInfo = Schema.Struct({
  nodeId: Schema.String,
  heartbeatAt: Schema.String
}).pipe(Schema.encodeKeys({
  nodeId: "node_id",
  heartbeatAt: "heartbeat_at"
})).annotate({ identifier: "WorkerInfo" })

export type WorkerInfo = typeof WorkerInfo.Type

/**
 * Response schema for listing workers.
 */
export const GetWorkersResponse = Schema.Struct({
  workers: Schema.Array(WorkerInfo)
}).annotate({ identifier: "GetWorkersResponse" })

export type GetWorkersResponse = typeof GetWorkersResponse.Type

// =============================================================================
// Manifest Request/Response Schemas
// =============================================================================

/**
 * Response schema for registering a manifest.
 */
export const RegisterManifestResponse = Schema.Struct({
  hash: Models.DatasetHash
}).annotate({ identifier: "RegisterManifestResponse" })

export type RegisterManifestResponse = typeof RegisterManifestResponse.Type

// =============================================================================
// Provider Request/Response Schemas
// =============================================================================

/**
 * Provider information returned by the API.
 */
export const ProviderInfo = Schema.Struct({
  name: Schema.String,
  network: Models.Network,
  config: Schema.Any
}).annotate({ identifier: "ProviderInfo" })
export type ProviderInfo = typeof ProviderInfo.Type

/**
 * Response schema for listing providers.
 */
export const GetProvidersResponse = Schema.Struct({
  providers: Schema.Array(ProviderInfo)
}).annotate({ identifier: "GetProvidersResponse" })

export type GetProvidersResponse = typeof GetProvidersResponse.Type
