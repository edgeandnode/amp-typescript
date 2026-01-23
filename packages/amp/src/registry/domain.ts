import * as Schema from "effect/Schema"
import * as Models from "../Models.ts"

// TODO(Chris/Max/Sebastian): Should we consider moving these "general" schemas
// to the top-level models module (Which I'm considering moving into a /domain
// directory to separate things out a bit more)?

// =============================================================================
// General Schemas
// =============================================================================

export const PositiveIntFromString = Schema.NumberFromString.pipe(
  Schema.int(),
  Schema.positive()
).annotations({ identifier: "PositiveIntFromString" })
export type PositiveIntFromString = typeof PositiveIntFromString.Type

/**
 * Represents a service status.
 */
export const ServiceStatus = Schema.Struct({
  "error": Schema.optionalWith(Schema.String, { nullable: true }),
  "status": Schema.String
}).annotations({ identifier: "ServiceStatus" })
export type ServiceStatus = typeof ServiceStatus.Type

/**
 * Time-based buckets for grouping datasets by last updated time
 */
export const LastUpdatedBucket = Schema.Literal(
  "last_day",
  "last_week",
  "last_month",
  "last_year"
).annotations({ identifier: "LastUpdatedBucket" })
export type LastUpdatedBucket = typeof LastUpdatedBucket.Type

export const DatasetSortBy = Schema.Literal(
  "namespace",
  "name",
  "owner",
  "created_at",
  "updated_at"
).annotations({ identifier: "DatasetSortBy" })
export type DatasetSortBy = typeof DatasetSortBy.Type

export const DatasetSortDirection = Schema.Literal(
  "asc",
  "desc"
).annotations({ identifier: "DatasetSortDirection" })
export type DatasetSortDirection = typeof DatasetSortDirection.Type

/**
 * Represents a dataset version ancestry reference.
 */
export const DatasetVersionAncestry = Schema.Struct({
  /**
   * Dataset reference in the format: {namespace}/{name}@{version_tag}. Points
   * to the DatasetVersion.dataset_reference. This allows version-pinned
   * dependencies.
   */
  "dataset_reference": Models.DatasetReferenceFromString
}).annotations({ identifier: "DatasetVersionAncestry" })
export type DatasetVersionAncestry = typeof DatasetVersionAncestry.Type

/**
 * Represents the status of a dataset version.
 */
export const DatasetVersionStatus = Schema.Literal(
  "draft",
  "published",
  "deprecated",
  "archived"
).annotations({ identifier: "DatasetVersionStatus" })
export type DatasetVersionStatus = typeof DatasetVersionStatus.Type

/**
 * Represents a dataset version.
 */
export const DatasetVersion = Schema.Struct({
  /**
   * Array of ancestor DatasetVersion references that this version extends from
   * (version-pinned dependencies).
   */
  "ancestors": Schema.optionalWith(Schema.Array(DatasetVersionAncestry), { nullable: true }),
  /**
   * A description of what changed with this version. Allows developers of the
   * Dataset to communicate to downstream consumers what has changed with this
   * version from previous versions. Migration guides, etc.
   */
  "changelog": Schema.optionalWith(Schema.String, { nullable: true }),
  /**
   * Timestamp when the DatasetVersion record was created (immutable).
   */
  "created_at": Schema.String,
  /**
   * Dataset reference in the format: {namespace}/{name}@{version_tag}. This value is globally unique and is a pointer to a tagged and published Manifest.
   */
  "dataset_reference": Models.DatasetReferenceFromString,
  /**
   * Array of descendant DatasetVersion references that extend from this version.
   */
  "descendants": Schema.optionalWith(Schema.Array(DatasetVersionAncestry), { nullable: true }),
  "status": DatasetVersionStatus,
  /**
   * The published version tag. This is basically the version label. Can be semver, a commit hash, or 'latest'.
   */
  "version_tag": Models.DatasetRevision
}).annotations({ identifier: "DatasetVersion" })
export type DatasetVersion = typeof DatasetVersion.Type

/**
 * Top-level container for a user-defined, tagged, and published Dataset.
 *
 * Contains metadata and discovery information for datasets.
 */
export const Dataset = Schema.Struct({
  /**
   * The dataset name. Lowercase, alphanumeric with underscores. Cannot start
   * with a number.
   */
  "name": Models.DatasetName,
  /**
   * The dataset namespace. Logical grouping mechanism for datasets. Can be a
   * user 0x address, username, or organization.
   */
  "namespace": Models.DatasetNamespace,
  /**
   * Timestamp when the Dataset record was created (immutable).
   */
  "created_at": Schema.String,
  /**
   * Computed link to the latest DatasetVersion reference in PURL format.
   */
  "dataset_reference": Schema.optionalWith(Models.DatasetReferenceFromString, { nullable: true }),
  /**
   * Description of the dataset, its intended use, and purpose.
   */
  "description": Schema.optionalWith(Models.DatasetDescription, { nullable: true }),
  /**
   * Chains being indexed by the Dataset. Used for discovery by chain.
   */
  "indexing_chains": Schema.Array(Schema.String),
  /**
   * User-defined or derived keywords defining the usage of the dataset.
   */
  "keywords": Schema.optionalWith(Schema.Array(Models.DatasetKeyword), { nullable: true }),
  "latest_version": Schema.optionalWith(DatasetVersion, { nullable: true }),
  /**
   * Usage license covering the Dataset.
   */
  "license": Schema.optionalWith(Models.DatasetLicense, { nullable: true }),
  /**
   * Owner of the Dataset. Can be an organization or user 0x address.
   */
  "owner": Schema.String,
  /**
   * User-defined README for the Dataset providing usage examples and documentation.
   */
  "readme": Schema.optionalWith(Models.DatasetReadme, { nullable: true }),
  /**
   * VCS repository URL containing the Dataset source code.
   */
  "repository_url": Schema.optionalWith(Models.DatasetRepository, { nullable: true }),
  /**
   * Source of data being materialized by the Dataset (e.g., contract addresses,
   * logs, transactions).
   */
  "source": Schema.optionalWith(Schema.Array(Models.DatasetSource), { nullable: true }),
  /**
   * Timestamp when the Dataset record was last updated.
   */
  "updated_at": Schema.String,
  /**
   * Link to all DatasetVersion records that this Dataset is a parent of.
   */
  "versions": Schema.optionalWith(Schema.Array(DatasetVersion), { nullable: true }),
  "visibility": Models.DatasetVisibility
}).annotations({ identifier: "Dataset" })
export type Dataset = typeof Dataset.Type

/**
 * Dataset with search relevance score. Extends the base Dataset with a weighted score indicating how well it matches the search query. Higher scores indicate better relevance.
 */
export const DatasetWithScore = Schema.Struct({
  ...Dataset.fields,
  /**
   * Weighted relevance score indicating how well this dataset matches the search query. Higher scores indicate better relevance. Score is calculated based on matches in description, keywords, source, and indexing chains fields.
   */
  "score": Schema.Number
}).annotations({ identifier: "DatasetWithScore" })
export type DatasetWithScore = typeof DatasetWithScore.Type

/**
 * Count of datasets by indexing chain.
 *
 * Returns the chain name and the number of datasets indexing that chain.
 */
export const DatasetCountByChain = Schema.Struct({
  /**
   * The indexing chain name (e.g., "mainnet", "arbitrum-one", "base-mainnet")
   */
  "chain": Schema.String,
  /**
   * The count of Dataset records indexing this chain
   */
  "count": Schema.Int
}).annotations({ identifier: "DatasetCountByChain" })
export type DatasetCountByChain = typeof DatasetCountByChain.Type

/**
 * Count of datasets by keyword (tag).
 *
 * Returns the keyword and the number of datasets with that keyword.
 */
export const DatasetCountByKeyword = Schema.Struct({
  /**
   * The count of Dataset records with this keyword
   */
  "count": Schema.Int,
  /**
   * The keyword (e.g., "DeFi", "NFT", "logs")
   */
  "keyword": Schema.String
}).annotations({ identifier: "DatasetCountByKeyword" })
export type DatasetCountByKeyword = typeof DatasetCountByKeyword.Type

/**
 * Cumulative count of datasets by last updated time bucket.
 *
 * Counts are cumulative - a dataset updated 1 hour ago will be counted in
 * all four buckets (last_day, last_week, last_month, and last_year).
 */
export const DatasetCountByLastUpdated = Schema.Struct({
  /**
   * The time bucket
   */
  "bucket": LastUpdatedBucket,
  /**
   * The count of Dataset records updated within this time period
   */
  "count": Schema.Int
}).annotations({ identifier: "DatasetCountByLastUpdatedBucket" })
export type DatasetCountByLastUpdated = typeof DatasetCountByLastUpdated.Type

/**
 * Count of datasets by version status.
 *
 * Returns the version status and the number of datasets that have at least one version with that status.
 */
export const DatasetCountByStatus = Schema.Struct({
  /**
   * The count of Dataset records with at least one version in this status
   */
  "count": Schema.Int,
  /**
   * The version status (Draft, Published, Deprecated, or Archived)
   */
  "status": DatasetVersionStatus
}).annotations({ identifier: "DatasetCountByStatus" })
export type DatasetCountByStatus = typeof DatasetCountByStatus.Type

/**
 * Count of datasets by visibility.
 *
 * Returns the visibility and the number of datasets with that visibility.
 */
export const DatasetCountByVisibility = Schema.Struct({
  /**
   * The count of Dataset records with this visibility
   */
  "count": Schema.Int,
  /**
   * The visibility (Public or Private)
   */
  "visibility": Models.DatasetVisibility
}).annotations({ identifier: "DatasetCountByVisibility" })
export type DatasetCountByVisibility = typeof DatasetCountByVisibility.Type

/**
 * Represents a saved query.
 */
export const SavedQuery = Schema.Struct({
  /**
   * Timestamp when the SavedQuery was created
   */
  "created_at": Schema.String,
  /**
   * Creator/owner of the saved query (ethereum address or user_id)
   */
  "creator": Schema.String,
  /**
   * Optional description of what the query does
   */
  "description": Schema.optionalWith(Schema.String, { nullable: true }),
  /**
   * Unique identifier for the saved query (UUID)
   */
  "id": Schema.String.pipe(
    Schema.pattern(new RegExp("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"))
  ),
  /**
   * Name of the saved query
   */
  "name": Schema.String,
  /**
   * The SQL query string
   */
  "query": Schema.String,
  /**
   * Timestamp when the SavedQuery was last updated
   */
  "updated_at": Schema.String,
  "visibility": Models.DatasetVisibility
}).annotations({ identifier: "SavedQuery" })
export type SavedQuery = typeof SavedQuery.Type

// =============================================================================
// URL Parameter Schemas
// =============================================================================

/**
 * Parameters for listing datasets.
 */
export const ListDatasetsParams = Schema.Struct({
  "limit": Schema.optional(PositiveIntFromString),
  "page": Schema.optional(PositiveIntFromString),
  "sort_by": Schema.optional(DatasetSortBy),
  "direction": Schema.optional(DatasetSortDirection),
  "indexing_chains": Schema.optional(Schema.StringFromUriComponent),
  "keywords": Schema.optional(Schema.StringFromUriComponent),
  "last_updated": Schema.optional(LastUpdatedBucket)
}).annotations({ identifier: "ListDatasetsParams" })
export type ListDatasetsParams = typeof ListDatasetsParams.Type

/**
 * Parameters for searching datasets.
 */
export const SearchDatasetsParams = Schema.Struct({
  "search": Schema.String,
  "limit": Schema.optional(PositiveIntFromString),
  "page": Schema.optional(PositiveIntFromString),
  "indexing_chains": Schema.optional(Schema.StringFromUriComponent),
  "keywords": Schema.optional(Schema.StringFromUriComponent),
  "last_updated": Schema.optional(LastUpdatedBucket)
}).annotations({ identifier: "SearchDatasetsParams" })
export type SearchDatasetsParams = typeof SearchDatasetsParams.Type

/**
 * Parameters for AI search of datasets.
 */
export const AiSearchDatasetsParams = Schema.Struct({
  "search": Schema.String
}).annotations({ identifier: "AiSearchDatasetsParams" })
export type AiSearchDatasetsParams = typeof AiSearchDatasetsParams.Type

/**
 * Parameters for listing owned datasets.
 */
export const ListOwnedDatasetsParams = Schema.Struct({
  "limit": Schema.optional(PositiveIntFromString),
  "page": Schema.optional(PositiveIntFromString),
  "sort_by": Schema.optional(DatasetSortBy),
  "direction": Schema.optional(DatasetSortDirection),
  "indexing_chains": Schema.optional(Schema.StringFromUriComponent),
  "keywords": Schema.optional(Schema.StringFromUriComponent),
  "last_updated": Schema.optional(LastUpdatedBucket)
}).annotations({ identifier: "ListOwnedDatasetsParams" })
export type ListOwnedDatasetsParams = typeof ListOwnedDatasetsParams.Type

/**
 * Parameters for listing owned datasets by dataset FQDN.
 */
export const GetOwnedDatasetsByFqdnParams = Schema.Struct({
  "namespace": Models.DatasetNamespace,
  "name": Models.DatasetName
}).annotations({ identifier: "GetOwnedDatasetsByFqdnParams" })
export type GetOwnedDatasetsByFqdnParams = typeof GetOwnedDatasetsByFqdnParams.Type

/**
 * Parameters for searching owned datasets.
 */
export const SearchOwnedDatasetsParams = Schema.Struct({
  "search": Schema.String,
  "limit": Schema.optional(PositiveIntFromString),
  "page": Schema.optional(PositiveIntFromString),
  "indexing_chains": Schema.optional(Schema.StringFromUriComponent),
  "keywords": Schema.optional(Schema.StringFromUriComponent),
  "last_updated": Schema.optional(LastUpdatedBucket)
}).annotations({ identifier: "SearchOwnedDatasetsParams" })
export type SearchOwnedDatasetsParams = typeof SearchOwnedDatasetsParams.Type

/**
 * Parameters for listing datasets by owner.
 */
export const ListMyDatasetsParams = Schema.Struct({
  "limit": Schema.optional(PositiveIntFromString),
  "page": Schema.optional(PositiveIntFromString),
  "sort_by": Schema.optional(DatasetSortBy),
  "direction": Schema.optional(DatasetSortDirection),
  "indexing_chains": Schema.optional(Schema.StringFromUriComponent),
  "keywords": Schema.optional(Schema.StringFromUriComponent),
  "last_updated": Schema.optional(LastUpdatedBucket)
}).annotations({ identifier: "ListMyDatasetsParams" })
export type ListMyDatasetsParams = typeof ListMyDatasetsParams.Type

/**
 * Parameters for searching datasets by owner.
 */
export const SearchMyDatasetsParams = Schema.Struct({
  "search": Schema.String,
  "limit": Schema.optional(PositiveIntFromString),
  "page": Schema.optional(PositiveIntFromString),
  "indexing_chains": Schema.optional(Schema.StringFromUriComponent),
  "keywords": Schema.optional(Schema.StringFromUriComponent),
  "last_updated": Schema.optional(LastUpdatedBucket)
}).annotations({ identifier: "SearchMyDatasetsParams" })
export type SearchMyDatasetsParams = typeof SearchMyDatasetsParams.Type

// =============================================================================
// Request Header Schemas
// =============================================================================

/**
 * Represents a bearer token header.
 */
export const BearerAuthHeader = Schema.Struct({
  Authorization: Schema.String.pipe(
    Schema.startsWith("Bearer")
  )
}).annotations({ identifier: "BearerAuthHeader" })
export type BearerAuthHeader = typeof BearerAuthHeader.Type

// =============================================================================
// Request Payload Schemas
// =============================================================================

/**
 * Input for creating a new DatasetVersion. Contains the version tag, manifest hash, and manifest content.
 */
export const InsertDatasetVersion = Schema.Struct({
  /**
   * Optional changelog describing what changed in this version.
   */
  "changelog": Schema.optionalWith(Schema.String, { nullable: true }),
  "kind": Models.DatasetKind,
  /**
   * Manifest JSON content. This should be a valid datasets_derived::Manifest structure. The SHA256 hash will be calculated server-side.
   */
  "manifest": Schema.Record({ key: Schema.String, value: Schema.Unknown }),
  "status": DatasetVersionStatus,
  /**
   * Version tag (e.g., '1.0.0', 'latest', '8e0acc0'). Pattern: lowercase, numbers, dots, underscores, hyphens.
   */
  "version_tag": Models.DatasetVersion
}).annotations({ identifier: "InsertDatasetVersion" })
export type InsertDatasetVersion = typeof InsertDatasetVersion.Type

/**
 * Input for creating a new Dataset. Contains metadata, discovery information,
 * and the initial version to create. The owner will be automatically set to the
 * authenticated user.
 */
export const InsertDatasetPayload = Schema.Struct({
  /**
   * Description of the dataset, its intended use, and purpose.
   */
  "description": Schema.optionalWith(Models.DatasetDescription, { nullable: true }),
  /**
   * Chains being indexed by the Dataset. Used for discovery by chain.
   */
  "indexing_chains": Schema.Array(Schema.String),
  /**
   * User-defined keywords defining the usage of the dataset.
   */
  "keywords": Schema.optionalWith(Schema.Array(Models.DatasetKeyword), { nullable: true }),
  /**
   * Usage license covering the Dataset.
   */
  "license": Schema.optionalWith(Models.DatasetLicense, { nullable: true }),
  /**
   * The dataset name. Pattern: lowercase, alphanumeric with underscores, cannot start with a number.
   */
  "name": Models.DatasetName,
  /**
   * The dataset namespace. Pattern: lowercase, numbers, underscores.
   */
  "namespace": Models.DatasetNamespace,
  /**
   * User-defined README for the Dataset providing usage examples and documentation.
   */
  "readme": Schema.optionalWith(Models.DatasetReadme, { nullable: true }),
  /**
   * VCS repository URL containing the Dataset source code.
   */
  "repository_url": Schema.optionalWith(Models.DatasetRepository, { nullable: true }),
  /**
   * Source of data being materialized by the Dataset (e.g., contract addresses).
   */
  "source": Schema.optionalWith(Schema.Array(Models.DatasetSource), { nullable: true }),
  "version": InsertDatasetVersion,
  "visibility": Models.DatasetVisibility
}).annotations({ identifier: "InsertDataset" })
export type InsertDatasetPayload = typeof InsertDatasetPayload.Type

/**
 * Input for update the Datasets metadata fields:
 * - keywords
 * - README
 * - sources
 * - repository_url
 * - license
 * - description
 */
export const UpdateDatasetMetadataPayload = Schema.Struct({
  /**
   * Dataset description
   */
  "description": Schema.optionalWith(Models.DatasetDescription, { nullable: true }),
  /**
   * Chains being indexed by the dataset
   */
  "indexing_chains": Schema.Array(Schema.String),
  /**
   * Keywords for dataset discovery
   */
  "keywords": Schema.optionalWith(Schema.Array(Models.DatasetKeyword), { nullable: true }),
  /**
   * License covering the dataset
   */
  "license": Schema.optionalWith(Models.DatasetLicense, { nullable: true }),
  /**
   * User-defined README for the dataset
   */
  "readme": Schema.optionalWith(Models.DatasetReadme, { nullable: true }),
  /**
   * VCS repository URL
   */
  "repository_url": Schema.optionalWith(Models.DatasetRepository, { nullable: true }),
  /**
   * Source of data being materialized
   */
  "source": Schema.optionalWith(Schema.Array(Models.DatasetSource), { nullable: true })
}).annotations({ identifier: "UpdateDatasetMetadataPayload" })
export type UpdateDatasetMetadataPayload = typeof UpdateDatasetMetadataPayload.Type

/**
 * Input for updating a DatasetVersion's status
 */
export const UpdateDatasetVersionStatusPayload = Schema.Struct({
  /**
   * The new status for the dataset version (Draft or Published)
   * Note: Use the DELETE endpoint to archive a version
   */
  "status": DatasetVersionStatus
}).annotations({ identifier: "UpdateDatasetVersionStatusPayload" })
export type UpdateDatasetVersionStatusPayload = typeof UpdateDatasetVersionStatusPayload.Type

/**
 * Input for updating a Dataset's visibility
 */
export const UpdateDatasetVisibilityPayload = Schema.Struct({
  /**
   * The new visibility level for the dataset
   */
  "visibility": Models.DatasetVisibility
}).annotations({ identifier: "UpdateDatasetVisibilityPayload" })
export type UpdateDatasetVisibilityPayload = typeof UpdateDatasetVisibilityPayload.Type

// =============================================================================
// Response Schemas
// =============================================================================

/**
 * Represents a healthcheck response.
 */
export const HealthcheckResponse = Schema.Struct({
  "status": Schema.String,
  "version": Schema.String
}).annotations({ identifier: "HealthcheckResponse" })
export type HealthcheckResponse = typeof HealthcheckResponse.Type

/**
 * Response for listing datasets.
 */
export const DatasetListResponse = Schema.Struct({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": Schema.Array(Dataset),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": Schema.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": Schema.Int
}).annotations({ identifier: "DatasetListResponse" })
export type DatasetListResponse = typeof DatasetListResponse.Type

/**
 * Response for datasets count by chain.
 */
export const DatasetCountsByChainResponse = Schema.Array(DatasetCountByChain).annotations({
  identifier: "DatasetCountsByChainResponse"
})
export type DatasetCountsByChainResponse = typeof DatasetCountsByChainResponse.Type

/**
 * Response for datasets count by keyword.
 */
export const DatasetCountsByKeywordResponse = Schema.Array(DatasetCountByKeyword).annotations({
  identifier: "DatasetCountsByKeywordResponse"
})
export type DatasetCountsByKeywordResponse = typeof DatasetCountsByKeywordResponse.Type

/**
 * Response for datasets count by last updated.
 */
export const DatasetCountsByLastUpdatedResponse = Schema.Array(DatasetCountByLastUpdated).annotations({
  identifier: "DatasetCountsByLastUpdatedResponse"
})
export type DatasetCountsByLastUpdatedResponse = typeof DatasetCountsByLastUpdatedResponse.Type

/**
 * Response for searching datasets.
 */
export const DatasetSearchResponse = Schema.Struct({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": Schema.Array(DatasetWithScore),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": Schema.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": Schema.Int
}).annotations({ identifier: "DatasetSearchResponse" })
export type DatasetSearchResponse = typeof DatasetSearchResponse.Type

/**
 * Response for AI search of datasets.
 */
export const DatasetAiSearchResponse = Schema.Array(DatasetWithScore).annotations({
  identifier: "DatasetAiSearchResponse"
})
export type DatasetAiSearchResponse = typeof DatasetAiSearchResponse.Type

/**
 * Response for listing dataset versions.
 */
export const DatasetListVersionsResponse = Schema.Array(DatasetVersion).annotations({
  identifier: "DatasetListVersionsResponse"
})
export type DatasetListVersionsResponse = typeof DatasetListVersionsResponse.Type

/**
 * Response for getting latest manifest.
 */
export const DatasetGetLatestManifestResponse = Schema.String.annotations({
  identifier: "DatasetGetLatestManifestResponse"
})
export type DatasetGetLatestManifestResponse = typeof DatasetGetLatestManifestResponse.Type

/**
 * Response for getting a manifest.
 */
export const DatasetGetManifestResponse = Schema.String.annotations({
  identifier: "DatasetGetManifestResponse"
})
export type DatasetGetManifestResponse = typeof DatasetGetManifestResponse.Type

/**
 * Response for listing latest queries.
 */
export const DatasetListLatestQueriesResponse = Schema.Array(SavedQuery).annotations({
  identifier: "DatasetListLatestQueriesResponse"
})
export type DatasetsListLatestQueries = typeof DatasetListLatestQueriesResponse.Type

/**
 * Response for listing queries.
 */
export const DatasetListQueriesResponse = Schema.Array(SavedQuery).annotations({
  identifier: "DatasetListQueriesResponse"
})
export type DatasetListQueriesResponse = typeof DatasetListQueriesResponse.Type

/**
 * Response for listing authenticated user's owned datasets.
 */
export const AuthUserOwnedDatasetListResponse = Schema.Struct({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": Schema.Array(Dataset),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": Schema.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": Schema.Int
}).annotations({ identifier: "AuthUserOwnedDatasetListResponse" })
export type AuthUserOwnedDatasetListResponse = typeof AuthUserOwnedDatasetListResponse.Type

/**
 * Response for owned datasets count by chain.
 */
export const OwnedDatasetCountsByChainResponse = Schema.Array(DatasetCountByChain).annotations({
  identifier: "OwnedDatasetCountsByChainResponse"
})
export type OwnedDatasetCountsByChainResponse = typeof OwnedDatasetCountsByChainResponse.Type

/**
 * Response for owned datasets count by keyword.
 */
export const OwnedDatasetCountsByKeywordResponse = Schema.Array(DatasetCountByKeyword).annotations({
  identifier: "OwnedDatasetCountsByKeywordResponse"
})
export type OwnedDatasetCountsByKeywordResponse = typeof OwnedDatasetCountsByKeywordResponse.Type

/**
 * Response for owned datasets count by last updated.
 */
export const OwnedDatasetsCountByLastUpdatedResponse = Schema.Array(DatasetCountByLastUpdated).annotations({
  identifier: "OwnedDatasetsCountByLastUpdatedResponse"
})
export type OwnedDatasetsCountByLastUpdatedResponse = typeof OwnedDatasetsCountByLastUpdatedResponse.Type

/**
 * Response for owned datasets count by status.
 */
export const OwnedDatasetCountsByStatusResponse = Schema.Array(DatasetCountByStatus).annotations({
  identifier: "OwnedDatasetCountsByStatusResponse"
})
export type OwnedDatasetCountsByStatusResponse = typeof OwnedDatasetCountsByStatusResponse.Type

/**
 * Response for owned datasets count by visibility.
 */
export const OwnedDatasetCountsByVisibilityResponse = Schema.Array(DatasetCountByVisibility).annotations({
  identifier: "wnedDatasetCountsByVisibilityResponse"
})
export type OwnedDatasetCountsByVisibilityResponse = typeof OwnedDatasetCountsByVisibilityResponse.Type

/**
 * Response for archiving a dataset version
 */
export const ArchiveDatasetVersionResponse = Schema.Struct({
  /**
   * The reference of the archived dataset version
   */
  "reference": Schema.String
}).annotations({ identifier: "ArchiveDatasetVersionResponse" })
export type ArchiveDatasetVersionResponse = typeof ArchiveDatasetVersionResponse.Type

/**
 * Response for listing owned queries.
 */
export const OwnedDatasetListQueriesResponse = Schema.Array(SavedQuery).annotations({
  identifier: "OwnedDatasetListQueriesResponse"
})
export type OwnedDatasetListQueriesResponse = typeof OwnedDatasetListQueriesResponse.Type

/**
 * Response for listing datasets by owner.
 */
export const ListMyDatasetsResponse = Schema.Struct({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": Schema.Array(Dataset),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": Schema.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": Schema.Int
}).annotations({ identifier: "ListMyDatasetsResponse" })
export type ListMyDatasetsResponse = typeof ListMyDatasetsResponse.Type

/**
 * Represents a liveness response.
 */
export const LivenessResponse = Schema.Struct({
  "status": Schema.String
}).annotations({ identifier: "LivenessResponse" })
export type LivenessResponse = typeof LivenessResponse.Type

/**
 * Represents readiness checks.
 */
export const ReadinessChecks = Schema.Struct({
  "database": ServiceStatus
}).annotations({ identifier: "ReadinessChecks" })
export type ReadinessChecks = typeof ReadinessChecks.Type

/**
 * Represents a readiness response.
 */
export const ReadinessResponse = Schema.Struct({
  "checks": ReadinessChecks,
  "status": Schema.String
}).annotations({ identifier: "ReadinessResponse" })
export type ReadinessResponse = typeof ReadinessResponse.Type
