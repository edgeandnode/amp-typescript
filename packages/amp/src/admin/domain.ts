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
 *
 * The manifest can either be the full manifest content, or the hash of a
 * manifest which was previously registered.
 */
export const RegisterDatasetPayload = Schema.Struct({
  namespace: Schema.String,
  name: Schema.String,
  version: Schema.optional(Schema.String),
  manifest: Schema.Union([Models.DatasetHash, Models.DatasetManifest])
}).annotate({ identifier: "RegisterDatasetPayload" })

export type RegisterDatasetPayload = typeof RegisterDatasetPayload.Type

/**
 * Response schema for registering a dataset.
 */
export const RegisterDatasetResponse = Schema.Struct({
  namespace: Models.DatasetNamespace,
  name: Models.DatasetName,
  version: Schema.optional(Models.DatasetVersion),
  manifestHash: Models.DatasetHash,
  kind: Models.DatasetKind,
  startBlock: Models.NonNegativeInt,
  finalizedBlocksOnly: Schema.Boolean,
  tables: Schema.Array(Schema.String)
})
  .pipe(
    Schema.encodeKeys({
      manifestHash: "manifest_hash",
      startBlock: "start_block",
      finalizedBlocksOnly: "finalized_blocks_only"
    })
  )
  .annotate({ identifier: "RegisterDatasetResponse" })

export type RegisterDatasetResponse = typeof RegisterDatasetResponse.Type

/**
 * Response schema for getting a dataset version.
 */
export const GetDatasetVersionResponse = Schema.Struct({
  kind: Models.DatasetKind,
  namespace: Models.DatasetNamespace,
  name: Models.DatasetName,
  revision: Models.DatasetRevision,
  manifestHash: Models.DatasetHash,
  startBlock: Models.NonNegativeInt,
  finalizedBlocksOnly: Schema.Boolean,
  tables: Schema.Array(Schema.String),
  /**
   * Tags pointing at this manifest, ordered versions first, then `"latest"`,
   * then `"dev"`.
   */
  tags: Schema.Array(Schema.String)
})
  .pipe(
    Schema.encodeKeys({
      manifestHash: "manifest_hash",
      startBlock: "start_block",
      finalizedBlocksOnly: "finalized_blocks_only"
    })
  )
  .annotate({ identifier: "GetDatasetVersionResponse" })

export type GetDatasetVersionResponse = typeof GetDatasetVersionResponse.Type

/**
 * Information about a single version of a dataset.
 */
export const DatasetVersionInfo = Schema.Struct({
  version: Models.DatasetVersion,
  manifestHash: Models.DatasetHash,
  createdAt: Schema.DateTimeUtc,
  updatedAt: Schema.DateTimeUtc
})
  .pipe(
    Schema.encodeKeys({
      manifestHash: "manifest_hash",
      createdAt: "created_at",
      updatedAt: "updated_at"
    })
  )
  .annotate({ identifier: "DatasetVersionInfo" })

export type DatasetVersionInfo = typeof DatasetVersionInfo.Type

/**
 * The special `latest` and `dev` tags of a dataset.
 */
export const DatasetSpecialTags = Schema.Struct({
  /**
   * The latest semantic version, if any.
   */
  latest: Schema.optional(Models.DatasetVersion),
  /**
   * The manifest hash the dev tag points to, if any.
   */
  dev: Schema.optional(Models.DatasetHash)
}).annotate({ identifier: "DatasetSpecialTags" })

export type DatasetSpecialTags = typeof DatasetSpecialTags.Type

/**
 * Response schema for listing dataset versions.
 */
export const GetDatasetVersionsResponse = Schema.Struct({
  namespace: Models.DatasetNamespace,
  name: Models.DatasetName,
  versions: Schema.Array(DatasetVersionInfo),
  specialTags: DatasetSpecialTags
})
  .pipe(Schema.encodeKeys({ specialTags: "special_tags" }))
  .annotate({ identifier: "GetDatasetVersionsResponse" })

export type GetDatasetVersionsResponse = typeof GetDatasetVersionsResponse.Type

/**
 * The direction in which to traverse the lineage graph of a dataset.
 *
 * - `upstream`: walks from the anchor dataset back toward its sources
 * - `downstream`: walks toward consumers of the anchor dataset
 * - `both`: walks in both directions from the anchor dataset
 */
export const LineageDirection = Schema.Literals(["upstream", "downstream", "both"]).annotate({
  identifier: "LineageDirection"
})

export type LineageDirection = typeof LineageDirection.Type

/**
 * The resolution status of a node in the lineage graph.
 */
export const LineageNodeStatus = Schema.Literals(["ok", "missing"]).annotate({ identifier: "LineageNodeStatus" })

export type LineageNodeStatus = typeof LineageNodeStatus.Type

/**
 * A single dataset node in the lineage graph.
 */
export const LineageNode = Schema.Struct({
  /**
   * The declared reference this node was reached by.
   */
  dataset: Models.DatasetReferenceFromString,
  /**
   * The dataset kind, or `null` when the node is missing.
   */
  kind: Schema.NullOr(Models.DatasetKind),
  status: LineageNodeStatus,
  /**
   * The manifest hash this reference currently resolves to, if any.
   */
  resolvedHash: Schema.NullOr(Models.DatasetHash),
  /**
   * The depth from the anchor dataset (the anchor has a depth of `0`).
   */
  depth: Models.NonNegativeInt
})
  .pipe(Schema.encodeKeys({ resolvedHash: "resolved_hash" }))
  .annotate({ identifier: "LineageNode" })

export type LineageNode = typeof LineageNode.Type

/**
 * A directed edge in the lineage graph, pointing from an upstream source to a
 * downstream consumer.
 */
export const LineageEdge = Schema.Struct({
  /**
   * The upstream source dataset, in its declared form.
   */
  source: Models.DatasetReferenceFromString,
  /**
   * The downstream consumer dataset.
   */
  target: Models.DatasetReferenceFromString,
  /**
   * The alias used by the consumer to reference the source.
   */
  alias: Schema.String,
  /**
   * The concrete source reference the declared form currently resolves to,
   * if any.
   */
  resolvedSource: Schema.NullOr(Models.DatasetReferenceFromString)
})
  .pipe(Schema.encodeKeys({ resolvedSource: "resolved_source" }))
  .annotate({ identifier: "LineageEdge" })

export type LineageEdge = typeof LineageEdge.Type

/**
 * Response schema for getting the lineage graph of a dataset.
 *
 * Edges always point from upstream sources to downstream consumers,
 * regardless of the traversal direction.
 */
export const GetDatasetLineageResponse = Schema.Struct({
  /**
   * The dataset the traversal was anchored at.
   */
  anchor: Models.DatasetReferenceFromString,
  /**
   * All nodes reached during traversal.
   */
  nodes: Schema.Array(LineageNode),
  /**
   * Directed edges pointing from upstream sources to downstream consumers.
   */
  edges: Schema.Array(LineageEdge),
  /**
   * The maximum path length from the anchor observed in the response.
   */
  depth: Models.NonNegativeInt,
  /**
   * Whether traversal was truncated because the maximum depth was reached.
   */
  truncated: Schema.Boolean,
  /**
   * Nodes participating in a dependency cycle. Empty when the graph is acyclic.
   */
  cycles: Schema.Array(Models.DatasetReferenceFromString)
}).annotate({ identifier: "GetDatasetLineageResponse" })

export type GetDatasetLineageResponse = typeof GetDatasetLineageResponse.Type

/**
 * A non-negative integer query parameter.
 */
export const NonNegativeIntFromString = Schema.NumberFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))

export const LineageQueryParams = Schema.Struct({
  direction: Schema.optional(LineageDirection),
  maxDepth: Schema.optional(NonNegativeIntFromString),
  maxNodes: Schema.optional(NonNegativeIntFromString)
}).pipe(Schema.encodeKeys({ maxDepth: "max_depth", maxNodes: "max_nodes" }))

export type LineageQueryParams = typeof LineageQueryParams.Type

// =============================================================================
// Job Request/Response Schemas
// =============================================================================

/**
 * Response schema for listing jobs.
 */
export const GetJobsResponse = Schema.Struct({
  jobs: Schema.Array(Models.JobInfo),
  nextCursor: Schema.optional(Models.JobId)
})
  .pipe(Schema.encodeKeys({ nextCursor: "next_cursor" }))
  .annotate({ identifier: "GetJobsResponse" })

export type GetJobsResponse = typeof GetJobsResponse.Type

// =============================================================================
// Schema Request/Response Schemas
// =============================================================================

/**
 * Request payload for schema analysis.
 */
export const GetOutputSchemaPayload = Schema.Struct({
  tables: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  dependencies: Schema.optional(Schema.Record(Schema.String, Models.DatasetReferenceFromString)),
  functions: Schema.optional(Schema.Record(Schema.String, Models.FunctionDefinition))
}).annotate({ identifier: "GetOutputSchemaPayload" })

export type GetOutputSchemaPayload = typeof GetOutputSchemaPayload.Type

/**
 * Response schema for schema analysis.
 */
export const GetOutputSchemaResponse = Schema.Struct({
  schemas: Schema.Record(Schema.String, Models.TableSchema)
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
})
  .pipe(
    Schema.encodeKeys({
      nodeId: "node_id",
      heartbeatAt: "heartbeat_at"
    })
  )
  .annotate({ identifier: "WorkerInfo" })

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
 *
 * Contains the provider name and kind, along with any additional
 * provider-specific configuration fields.
 */
export const ProviderInfo = Schema.StructWithRest(
  Schema.Struct({
    name: Schema.String,
    kind: Schema.String
  }),
  [Schema.Record(Schema.String, Schema.Unknown)]
).annotate({ identifier: "ProviderInfo" })
export type ProviderInfo = typeof ProviderInfo.Type

/**
 * Response schema for listing providers.
 */
export const GetProvidersResponse = Schema.Struct({
  providers: Schema.Array(ProviderInfo)
}).annotate({ identifier: "GetProvidersResponse" })

export type GetProvidersResponse = typeof GetProvidersResponse.Type
