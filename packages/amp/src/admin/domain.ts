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

/**
 * Request payload for registering a dataset.
 */
export class RegisterDatasetPayload extends Schema.Class<RegisterDatasetPayload>(
  "Amp/AdminApi/RegisterDatasetPayload"
)({
  namespace: Schema.String,
  name: Schema.String,
  version: Schema.optional(Schema.String),
  manifest: Models.DatasetManifest
}, { identifier: "RegisterDatasetPayload" }) {}

/**
 * Response schema for getting a dataset version.
 */
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

/**
 * Response schema for listing dataset versions.
 */
export class GetDatasetVersionsResponse extends Schema.Class<GetDatasetVersionsResponse>(
  "Amp/AdminApi/GetDatasetVersionsResponse"
)({
  versions: Schema.Array(Models.DatasetVersion)
}, { identifier: "GetDatasetVersionsResponse" }) {}

/**
 * Request payload for deploying a dataset.
 */
export class DeployDatasetPayload extends Schema.Class<DeployDatasetPayload>(
  "Amp/AdminApi/DeployDatasetPayload"
)({
  endBlock: Schema.NullOr(Schema.String).pipe(
    Schema.optional,
    Schema.fromKey("end_block")
  ),
  parallelism: Schema.optional(Schema.Number),
  workerId: Schema.String.pipe(
    Schema.optional,
    Schema.fromKey("worker_id")
  )
}, { identifier: "DeployDatasetPayload" }) {}

/**
 * Response schema for deploying a dataset.
 */
export class DeployDatasetResponse extends Schema.Class<DeployDatasetResponse>(
  "Amp/AdminApi/DeployDatasetResponse"
)({
  jobId: Models.JobId.pipe(
    Schema.propertySignature,
    Schema.fromKey("job_id")
  )
}, { identifier: "DeployDatasetResponse" }) {}

/**
 * Table sync progress information.
 */
export const TableSyncProgress = Schema.Struct({
  tableName: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("table_name")
  ),
  currentBlock: Schema.Int.pipe(
    Schema.optional,
    Schema.fromKey("current_block")
  ),
  startBlock: Schema.Int.pipe(
    Schema.optional,
    Schema.fromKey("start_block")
  ),
  jobId: Models.JobId.pipe(
    Schema.optional,
    Schema.fromKey("job_id")
  ),
  jobStatus: Models.JobStatus.pipe(
    Schema.optional,
    Schema.fromKey("job_status")
  ),
  filesCount: Schema.Int.pipe(
    Schema.propertySignature,
    Schema.fromKey("files_count")
  ),
  totalSizeBytes: Schema.Int.pipe(
    Schema.propertySignature,
    Schema.fromKey("total_size_bytes")
  )
}).annotations({ identifier: "TableSyncProgress" })
export type TableSyncProgress = typeof TableSyncProgress.Type

/**
 * Response schema for getting dataset sync progress.
 */
export class GetDatasetSyncProgressResponse extends Schema.Class<GetDatasetSyncProgressResponse>(
  "Amp/AdminApi/GetDatasetSyncProgressResponse"
)({
  namespace: Models.DatasetNamespace.pipe(
    Schema.propertySignature,
    Schema.fromKey("dataset_namespace")
  ),
  name: Models.DatasetName.pipe(
    Schema.propertySignature,
    Schema.fromKey("dataset_name")
  ),
  revision: Models.DatasetRevision,
  manifestHash: Models.DatasetHash.pipe(
    Schema.propertySignature,
    Schema.fromKey("manifest_hash")
  ),
  tables: Schema.Array(TableSyncProgress)
}) {}

// =============================================================================
// Job Request/Response Schemas
// =============================================================================

/**
 * Response schema for listing jobs.
 */
export class GetJobsResponse extends Schema.Class<GetJobsResponse>(
  "Amp/AdminApi/GetJobsResponse"
)({
  jobs: Schema.Array(Models.JobInfo),
  nextCursor: Models.JobId.pipe(
    Schema.optional,
    Schema.fromKey("next_cursor")
  )
}, { identifier: "GetJobsResponse" }) {}

// =============================================================================
// Schema Request/Response Schemas
// =============================================================================

/**
 * Request payload for schema analysis.
 */
export class GetOutputSchemaPayload extends Schema.Class<GetOutputSchemaPayload>(
  "Amp/AdminApi/GetOutputSchemaPayload"
)({
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
}, { identifier: "GetOutputSchemaPayload" }) {}

/**
 * Response schema for schema analysis.
 */
export class GetOutputSchemaResponse extends Schema.Class<GetOutputSchemaResponse>(
  "Amp/AdminApi/GetOutputSchemaResponse"
)({
  schemas: Schema.Record({
    key: Schema.String,
    value: Models.TableSchemaWithNetworks
  })
}, { identifier: "GetOutputSchemaResponse" }) {}

// =============================================================================
// Worker Request/Response Schemas
// =============================================================================

/**
 * Worker information returned by the API.
 */
export const WorkerInfo = Schema.Struct({
  nodeId: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("node_id")
  ),
  heartbeatAt: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("heartbeat_at")
  )
}).annotations({ identifier: "WorkerInfo" })
export type WorkerInfo = typeof WorkerInfo.Type

/**
 * Response schema for listing workers.
 */
export class GetWorkersResponse extends Schema.Class<GetWorkersResponse>(
  "Amp/AdminApi/GetWorkersResponse"
)({
  workers: Schema.Array(WorkerInfo)
}, { identifier: "GetWorkersResponse" }) {}

// =============================================================================
// Manifest Request/Response Schemas
// =============================================================================

/**
 * Response schema for registering a manifest.
 */
export class RegisterManifestResponse extends Schema.Class<RegisterManifestResponse>(
  "Amp/AdminApi/RegisterManifestResponse"
)({
  hash: Models.DatasetHash
}, { identifier: "RegisterManifestResponse" }) {}

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
}).annotations({ identifier: "ProviderInfo" })
export type ProviderInfo = typeof ProviderInfo.Type

/**
 * Response schema for listing providers.
 */
export class GetProvidersResponse extends Schema.Class<GetProvidersResponse>(
  "Amp/AdminApi/GetProvidersResponse"
)({
  providers: Schema.Array(ProviderInfo)
}, { identifier: "GetProvidersResponse" }) {}
