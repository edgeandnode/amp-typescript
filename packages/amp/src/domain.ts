import * as S from "effect/Schema"

export class HealthcheckResponse extends S.Class<HealthcheckResponse>("HealthcheckResponse")({
  "status": S.String,
  "version": S.String
}) {}

/**
 * Time-based buckets for grouping datasets by last updated time
 */
export class LastUpdatedBucket extends S.Literal("last_day", "last_week", "last_month", "last_year") {}

export class DatasetsListParams extends S.Struct({
  "limit": S.optionalWith(S.Int, { nullable: true }),
  "page": S.optionalWith(S.Int, { nullable: true }),
  "sort_by": S.optionalWith(S.String, { nullable: true }),
  "direction": S.optionalWith(S.String, { nullable: true }),
  "indexing_chains": S.optionalWith(S.Array(S.String), { nullable: true }),
  "keywords": S.optionalWith(S.Array(S.String), { nullable: true }),
  "last_updated": S.optionalWith(LastUpdatedBucket, { nullable: true })
}) {}

export class DatasetVersionAncestry extends S.Class<DatasetVersionAncestry>("DatasetVersionAncestry")({
  /**
   * Dataset reference in the format: {namespace}/{name}@{version_tag}. Points to the DatasetVersion.dataset_reference. This allows version-pinned dependencies.
   */
  "dataset_reference": S.String.pipe(S.pattern(new RegExp("^[a-z0-9_]+/[a-z_][a-z0-9_]*@[a-z0-9._-]+$")))
}) {}

export class DatasetVersionStatus extends S.Literal("draft", "published", "deprecated", "archived") {}

export class DatasetVersion extends S.Class<DatasetVersion>("DatasetVersion")({
  /**
   * Array of ancestor DatasetVersion references that this version extends from (version-pinned dependencies).
   */
  "ancestors": S.optionalWith(S.Array(DatasetVersionAncestry), { nullable: true }),
  /**
   * A description of what changed with this version. Allows developers of the Dataset to communicate to downstream consumers what has changed with this version from previous versions. Migration guides, etc.
   */
  "changelog": S.optionalWith(S.String, { nullable: true }),
  /**
   * Timestamp when the DatasetVersion record was created (immutable).
   */
  "created_at": S.String,
  /**
   * Dataset reference in the format: {namespace}/{name}@{version_tag}. This value is globally unique and is a pointer to a tagged and published Manifest.
   */
  "dataset_reference": S.String.pipe(S.pattern(new RegExp("^[a-z0-9_]+/[a-z_][a-z0-9_]*@[a-z0-9._-]+$"))),
  /**
   * Array of descendant DatasetVersion references that extend from this version.
   */
  "descendants": S.optionalWith(S.Array(DatasetVersionAncestry), { nullable: true }),
  "status": DatasetVersionStatus,
  /**
   * The published version tag. This is basically the version label. Can be semver, a commit hash, or 'latest'.
   */
  "version_tag": S.String.pipe(S.pattern(new RegExp("^[a-z0-9._-]+$")))
}) {}

export class DatasetVisibility extends S.Literal("private", "public") {}

/**
 * Top-level container for a user-defined, tagged, and published Dataset. Contains metadata and discovery information for datasets.
 */
export class Dataset extends S.Class<Dataset>("Dataset")({
  /**
   * Timestamp when the Dataset record was created (immutable).
   */
  "created_at": S.String,
  /**
   * Computed link to the latest DatasetVersion reference in PURL format.
   */
  "dataset_reference": S.optionalWith(
    S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z0-9_]+/[a-z_][a-z0-9_]*@[a-z0-9._-]+$"))),
    { nullable: true }
  ),
  /**
   * Description of the dataset, its intended use, and purpose.
   */
  "description": S.optionalWith(S.String.pipe(S.maxLength(1024)), { nullable: true }),
  /**
   * Chains being indexed by the Dataset. Used for discovery by chain.
   */
  "indexing_chains": S.Array(S.String),
  /**
   * User-defined or derived keywords defining the usage of the dataset.
   */
  "keywords": S.optionalWith(S.Array(S.String), { nullable: true }),
  "latest_version": S.optionalWith(DatasetVersion, { nullable: true }),
  /**
   * Usage license covering the Dataset.
   */
  "license": S.optionalWith(S.String, { nullable: true }),
  /**
   * The dataset name. Lowercase, alphanumeric with underscores. Cannot start with a number.
   */
  "name": S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z_][a-z0-9_]*$"))),
  /**
   * The dataset namespace. Logical grouping mechanism for datasets. Can be a user 0x address, username, or organization.
   */
  "namespace": S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z0-9_]*$"))),
  /**
   * Owner of the Dataset. Can be an organization or user 0x address.
   */
  "owner": S.String,
  /**
   * User-defined README for the Dataset providing usage examples and documentation.
   */
  "readme": S.optionalWith(S.String, { nullable: true }),
  /**
   * VCS repository URL containing the Dataset source code.
   */
  "repository_url": S.optionalWith(S.String, { nullable: true }),
  /**
   * Source of data being materialized by the Dataset (e.g., contract addresses, logs, transactions).
   */
  "source": S.optionalWith(S.Array(S.String), { nullable: true }),
  /**
   * Timestamp when the Dataset record was last updated.
   */
  "updated_at": S.String,
  /**
   * Link to all DatasetVersion records that this Dataset is a parent of.
   */
  "versions": S.optionalWith(S.Array(DatasetVersion), { nullable: true }),
  "visibility": DatasetVisibility
}) {}

export class DatasetListResponse extends S.Class<DatasetListResponse>("DatasetListResponse")({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": S.Array(Dataset),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": S.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": S.Int
}) {}

/**
 * Standard error response returned by the API
 *
 * This struct represents error information returned in HTTP error responses.
 * It provides structured error details including a machine-readable error code
 * and human-readable message.
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
 *   "error_message": "dataset 'eth_mainnet' version '1.0.0' not found",
 *   "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
 * }
 * ```
 */
export class ErrorResponse extends S.Class<ErrorResponse>("ErrorResponse")({
  /**
   * Machine-readable error code in SCREAMING_SNAKE_CASE format
   *
   * Error codes are stable across API versions and should be used
   * for programmatic error handling. Examples: `INVALID_SELECTOR`,
   * `DATASET_NOT_FOUND`, `REGISTRY_DB_ERROR`
   */
  "error_code": S.String,
  /**
   * Human-readable error message
   *
   * Messages provide detailed context about the error but may change
   * over time. Use `error_code` for programmatic decisions.
   */
  "error_message": S.String,
  /**
   * Request ID for tracing and correlation
   *
   * This ID can be used to correlate error responses with server logs
   * for debugging and support purposes. The ID is generated per-request
   * and appears in both logs and error responses.
   */
  "request_id": S.optionalWith(S.String, { nullable: true })
}) {}

/**
 * Count of datasets by indexing chain.
 *
 * Returns the chain name and the number of datasets indexing that chain.
 */
export class DatasetCountByChainDto extends S.Class<DatasetCountByChainDto>("DatasetCountByChainDto")({
  /**
   * The indexing chain name (e.g., "mainnet", "arbitrum-one", "base-mainnet")
   */
  "chain": S.String,
  /**
   * The count of Dataset records indexing this chain
   */
  "count": S.Int
}) {}

export class DatasetsCountByChain200 extends S.Array(DatasetCountByChainDto) {}

/**
 * Count of datasets by keyword (tag).
 *
 * Returns the keyword and the number of datasets with that keyword.
 */
export class DatasetCountByKeywordDto extends S.Class<DatasetCountByKeywordDto>("DatasetCountByKeywordDto")({
  /**
   * The count of Dataset records with this keyword
   */
  "count": S.Int,
  /**
   * The keyword (e.g., "DeFi", "NFT", "logs")
   */
  "keyword": S.String
}) {}

export class DatasetsCountByKeyword200 extends S.Array(DatasetCountByKeywordDto) {}

/**
 * Cumulative count of datasets by last updated time bucket.
 *
 * Counts are cumulative - a dataset updated 1 hour ago will be counted in
 * all four buckets (last_day, last_week, last_month, and last_year).
 */
export class DatasetCountByLastUpdatedBucketDto
  extends S.Class<DatasetCountByLastUpdatedBucketDto>("DatasetCountByLastUpdatedBucketDto")({
    /**
     * The time bucket
     */
    "bucket": LastUpdatedBucket,
    /**
     * The count of Dataset records updated within this time period
     */
    "count": S.Int
  })
{}

export class DatasetsCountByLastUpdated200 extends S.Array(DatasetCountByLastUpdatedBucketDto) {}

export class DatasetsSearchParams extends S.Struct({
  "search": S.String,
  "limit": S.optionalWith(S.Int, { nullable: true }),
  "page": S.optionalWith(S.Int, { nullable: true }),
  "indexing_chains": S.optionalWith(S.String, { nullable: true }),
  "keywords": S.optionalWith(S.String, { nullable: true }),
  "last_updated": S.optionalWith(LastUpdatedBucket, { nullable: true })
}) {}

/**
 * Dataset with search relevance score. Extends the base Dataset with a weighted score indicating how well it matches the search query. Higher scores indicate better relevance.
 */
export class DatasetWithScore extends S.Class<DatasetWithScore>("DatasetWithScore")({
  /**
   * Timestamp when the Dataset record was created (immutable).
   */
  "created_at": S.String,
  /**
   * Computed link to the latest DatasetVersion reference in PURL format.
   */
  "dataset_reference": S.optionalWith(
    S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z0-9_]+/[a-z_][a-z0-9_]*@[a-z0-9._-]+$"))),
    { nullable: true }
  ),
  /**
   * Description of the dataset, its intended use, and purpose.
   */
  "description": S.optionalWith(S.String.pipe(S.maxLength(1024)), { nullable: true }),
  /**
   * Chains being indexed by the Dataset. Used for discovery by chain.
   */
  "indexing_chains": S.Array(S.String),
  /**
   * User-defined or derived keywords defining the usage of the dataset.
   */
  "keywords": S.optionalWith(S.Array(S.String), { nullable: true }),
  "latest_version": S.optionalWith(DatasetVersion, { nullable: true }),
  /**
   * Usage license covering the Dataset.
   */
  "license": S.optionalWith(S.String, { nullable: true }),
  /**
   * The dataset name. Lowercase, alphanumeric with underscores. Cannot start with a number.
   */
  "name": S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z_][a-z0-9_]*$"))),
  /**
   * The dataset namespace. Logical grouping mechanism for datasets. Can be a user 0x address, username, or organization.
   */
  "namespace": S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z0-9_]*$"))),
  /**
   * Owner of the Dataset. Can be an organization or user 0x address.
   */
  "owner": S.String,
  /**
   * User-defined README for the Dataset providing usage examples and documentation.
   */
  "readme": S.optionalWith(S.String, { nullable: true }),
  /**
   * VCS repository URL containing the Dataset source code.
   */
  "repository_url": S.optionalWith(S.String, { nullable: true }),
  /**
   * Weighted relevance score indicating how well this dataset matches the search query. Higher scores indicate better relevance. Score is calculated based on matches in description, keywords, source, and indexing chains fields.
   */
  "score": S.Number,
  /**
   * Source of data being materialized by the Dataset (e.g., contract addresses, logs, transactions).
   */
  "source": S.optionalWith(S.Array(S.String), { nullable: true }),
  /**
   * Timestamp when the Dataset record was last updated.
   */
  "updated_at": S.String,
  /**
   * Link to all DatasetVersion records that this Dataset is a parent of.
   */
  "versions": S.optionalWith(S.Array(DatasetVersion), { nullable: true }),
  "visibility": DatasetVisibility
}) {}

export class DatasetSearchResponse extends S.Class<DatasetSearchResponse>("DatasetSearchResponse")({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": S.Array(DatasetWithScore),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": S.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": S.Int
}) {}

export class DatasetsAiSearchParams extends S.Struct({
  "search": S.String
}) {}

export class DatasetsAiSearch200 extends S.Array(DatasetWithScore) {}

export class DatasetsListVersions200 extends S.Array(DatasetVersion) {}

export class DatasetsGetLatestManifest200 extends S.String {}

export class SavedQuery extends S.Class<SavedQuery>("SavedQuery")({
  /**
   * Timestamp when the SavedQuery was created
   */
  "created_at": S.String,
  /**
   * Creator/owner of the saved query (ethereum address or user_id)
   */
  "creator": S.String,
  /**
   * Optional description of what the query does
   */
  "description": S.optionalWith(S.String, { nullable: true }),
  /**
   * Unique identifier for the saved query (UUID)
   */
  "id": S.String.pipe(
    S.pattern(new RegExp("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"))
  ),
  /**
   * Name of the saved query
   */
  "name": S.String,
  /**
   * The SQL query string
   */
  "query": S.String,
  /**
   * Timestamp when the SavedQuery was last updated
   */
  "updated_at": S.String,
  "visibility": DatasetVisibility
}) {}

export class DatasetsListLatestQueries200 extends S.Array(SavedQuery) {}

export class DatasetsGetManifest200 extends S.String {}

export class DatasetsListQueries200 extends S.Array(SavedQuery) {}

export class DatasetsOwnedListParams extends S.Struct({
  "limit": S.optionalWith(S.Int, { nullable: true }),
  "page": S.optionalWith(S.Int, { nullable: true }),
  "sort_by": S.optionalWith(S.String, { nullable: true }),
  "direction": S.optionalWith(S.String, { nullable: true }),
  "indexing_chains": S.optionalWith(S.String, { nullable: true }),
  "keywords": S.optionalWith(S.String, { nullable: true }),
  "last_updated": S.optionalWith(LastUpdatedBucket, { nullable: true })
}) {}

export class AuthUserOwnedDatasetListResponse
  extends S.Class<AuthUserOwnedDatasetListResponse>("AuthUserOwnedDatasetListResponse")({
    /**
     * List of the datasets being returned in this page
     */
    "datasets": S.Array(Dataset),
    /**
     * If true, there are more datasets that can be fetched
     */
    "has_next_page": S.Boolean,
    /**
     * Total number of datasets matching the query filters
     */
    "total_count": S.Int
  })
{}

export class OwnedDatasetsCountByChain200 extends S.Array(DatasetCountByChainDto) {}

export class OwnedDatasetsCountByKeyword200 extends S.Array(DatasetCountByKeywordDto) {}

export class OwnedDatasetsCountByLastUpdated200 extends S.Array(DatasetCountByLastUpdatedBucketDto) {}

/**
 * Count of datasets by version status.
 *
 * Returns the version status and the number of datasets that have at least one version with that status.
 */
export class DatasetCountByStatusDto extends S.Class<DatasetCountByStatusDto>("DatasetCountByStatusDto")({
  /**
   * The count of Dataset records with at least one version in this status
   */
  "count": S.Int,
  /**
   * The version status (Draft, Published, Deprecated, or Archived)
   */
  "status": DatasetVersionStatus
}) {}

export class OwnedDatasetsCountByStatus200 extends S.Array(DatasetCountByStatusDto) {}

/**
 * Count of datasets by visibility.
 *
 * Returns the visibility and the number of datasets with that visibility.
 */
export class DatasetCountByVisibilityDto extends S.Class<DatasetCountByVisibilityDto>("DatasetCountByVisibilityDto")({
  /**
   * The count of Dataset records with this visibility
   */
  "count": S.Int,
  /**
   * The visibility (Public or Private)
   */
  "visibility": DatasetVisibility
}) {}

export class OwnedDatasetsCountByVisibility200 extends S.Array(DatasetCountByVisibilityDto) {}

export class ManifestKind extends S.Literal("manifest", "evm-rpc", "eth-beacon", "firehose") {}

/**
 * Input for creating a new DatasetVersion. Contains the version tag, manifest hash, and manifest content.
 */
export class InsertDatasetVersion extends S.Class<InsertDatasetVersion>("InsertDatasetVersion")({
  /**
   * Optional changelog describing what changed in this version.
   */
  "changelog": S.optionalWith(S.String, { nullable: true }),
  "kind": ManifestKind,
  /**
   * Manifest JSON content. This should be a valid datasets_derived::Manifest structure. The SHA256 hash will be calculated server-side.
   */
  "manifest": S.Record({ key: S.String, value: S.Unknown }),
  "status": DatasetVersionStatus,
  /**
   * Version tag (e.g., '1.0.0', 'latest', '8e0acc0'). Pattern: lowercase, numbers, dots, underscores, hyphens.
   */
  "version_tag": S.String.pipe(S.pattern(new RegExp("^[a-z0-9._-]+$")))
}) {}

/**
 * Input for creating a new Dataset. Contains metadata, discovery information, and the initial version to create. The owner will be automatically set to the authenticated user.
 */
export class InsertDataset extends S.Class<InsertDataset>("InsertDataset")({
  /**
   * Description of the dataset, its intended use, and purpose.
   */
  "description": S.optionalWith(S.String.pipe(S.maxLength(1024)), { nullable: true }),
  /**
   * Chains being indexed by the Dataset. Used for discovery by chain.
   */
  "indexing_chains": S.Array(S.String),
  /**
   * User-defined keywords defining the usage of the dataset.
   */
  "keywords": S.optionalWith(S.Array(S.String), { nullable: true }),
  /**
   * Usage license covering the Dataset.
   */
  "license": S.optionalWith(S.String, { nullable: true }),
  /**
   * The dataset name. Pattern: lowercase, alphanumeric with underscores, cannot start with a number.
   */
  "name": S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z_][a-z0-9_]*$"))),
  /**
   * The dataset namespace. Pattern: lowercase, numbers, underscores.
   */
  "namespace": S.String.pipe(S.minLength(1), S.pattern(new RegExp("^[a-z0-9_]*$"))),
  /**
   * User-defined README for the Dataset providing usage examples and documentation.
   */
  "readme": S.optionalWith(S.String, { nullable: true }),
  /**
   * VCS repository URL containing the Dataset source code.
   */
  "repository_url": S.optionalWith(
    S.String.pipe(
      S.pattern(
        new RegExp(
          "^https?://[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\\\\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*(/.*)?$"
        )
      )
    ),
    { nullable: true }
  ),
  /**
   * Source of data being materialized by the Dataset (e.g., contract addresses).
   */
  "source": S.optionalWith(S.Array(S.String), { nullable: true }),
  "version": InsertDatasetVersion,
  "visibility": DatasetVisibility
}) {}

export class DatasetsOwnedSearchParams extends S.Struct({
  "search": S.String,
  "limit": S.optionalWith(S.Int, { nullable: true }),
  "page": S.optionalWith(S.Int, { nullable: true }),
  "indexing_chains": S.optionalWith(S.String, { nullable: true }),
  "keywords": S.optionalWith(S.String, { nullable: true }),
  "last_updated": S.optionalWith(LastUpdatedBucket, { nullable: true })
}) {}

/**
 * Input for update the Datasets metadata fields:
 * - keywords
 * - README
 * - sources
 * - repository_url
 * - license
 * - description
 */
export class UpdateDatasetMetadataDto extends S.Class<UpdateDatasetMetadataDto>("UpdateDatasetMetadataDto")({
  /**
   * Dataset description
   */
  "description": S.optionalWith(S.String, { nullable: true }),
  /**
   * Chains being indexed by the dataset
   */
  "indexing_chains": S.Array(S.String),
  /**
   * Keywords for dataset discovery
   */
  "keywords": S.optionalWith(S.Array(S.String), { nullable: true }),
  /**
   * License covering the dataset
   */
  "license": S.optionalWith(S.String, { nullable: true }),
  /**
   * User-defined README for the dataset
   */
  "readme": S.optionalWith(S.String, { nullable: true }),
  /**
   * VCS repository URL
   */
  "repository_url": S.optionalWith(S.String, { nullable: true }),
  /**
   * Source of data being materialized
   */
  "source": S.optionalWith(S.Array(S.String), { nullable: true })
}) {}

/**
 * Response for archiving a dataset version
 */
export class ArchiveDatasetVersionResponse
  extends S.Class<ArchiveDatasetVersionResponse>("ArchiveDatasetVersionResponse")({
    /**
     * The reference of the archived dataset version
     */
    "reference": S.String
  })
{}

/**
 * Input for updating a DatasetVersion's status
 */
export class UpdateDatasetVersionStatusDto
  extends S.Class<UpdateDatasetVersionStatusDto>("UpdateDatasetVersionStatusDto")({
    /**
     * The new status for the dataset version (Draft or Published)
     * Note: Use the DELETE endpoint to archive a version
     */
    "status": DatasetVersionStatus
  })
{}

export class DatasetsOwnedListQueries200 extends S.Array(SavedQuery) {}

/**
 * Input for updating a Dataset's visibility
 */
export class UpdateDatasetVisibilityDto extends S.Class<UpdateDatasetVisibilityDto>("UpdateDatasetVisibilityDto")({
  /**
   * The new visibility level for the dataset
   */
  "visibility": DatasetVisibility
}) {}

export class DatasetsOwnerListParams extends S.Struct({
  "limit": S.optionalWith(S.Int, { nullable: true }),
  "page": S.optionalWith(S.Int, { nullable: true }),
  "sort_by": S.optionalWith(S.String, { nullable: true }),
  "direction": S.optionalWith(S.String, { nullable: true }),
  "indexing_chains": S.optionalWith(S.String, { nullable: true }),
  "keywords": S.optionalWith(S.String, { nullable: true }),
  "last_updated": S.optionalWith(LastUpdatedBucket, { nullable: true })
}) {}

export class OwnerDatasetListResponse extends S.Class<OwnerDatasetListResponse>("OwnerDatasetListResponse")({
  /**
   * List of the datasets being returned in this page
   */
  "datasets": S.Array(Dataset),
  /**
   * If true, there are more datasets that can be fetched
   */
  "has_next_page": S.Boolean,
  /**
   * Total number of datasets matching the query filters
   */
  "total_count": S.Int
}) {}

export class DatasetsOwnerSearchParams extends S.Struct({
  "search": S.String,
  "limit": S.optionalWith(S.Int, { nullable: true }),
  "page": S.optionalWith(S.Int, { nullable: true }),
  "indexing_chains": S.optionalWith(S.String, { nullable: true }),
  "keywords": S.optionalWith(S.String, { nullable: true }),
  "last_updated": S.optionalWith(LastUpdatedBucket, { nullable: true })
}) {}

export class LivenessResponse extends S.Class<LivenessResponse>("LivenessResponse")({
  "status": S.String
}) {}

export class ServiceStatus extends S.Class<ServiceStatus>("ServiceStatus")({
  "error": S.optionalWith(S.String, { nullable: true }),
  "status": S.String
}) {}

export class ReadinessChecks extends S.Class<ReadinessChecks>("ReadinessChecks")({
  "database": ServiceStatus
}) {}

export class ReadinessResponse extends S.Class<ReadinessResponse>("ReadinessResponse")({
  "checks": ReadinessChecks,
  "status": S.String
}) {}
