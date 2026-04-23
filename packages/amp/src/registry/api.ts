import * as Schema from "effect/Schema"
import * as HttpApi from "effect/unstable/httpapi/HttpApi"
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint"
import * as HttpApiError from "effect/unstable/httpapi/HttpApiError"
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup"
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema"
import * as Models from "../core/domain.ts"
import * as Domain from "./domain.ts"
import * as Errors from "./error.ts"

// =============================================================================
// Registry API Params
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
 * A URL parameter for the dataset owners.
 */
const DatasetOwnerParam = Schema.String

// =============================================================================
// Health
// =============================================================================

// -----------------------------------------------------------------------------
// GET /
// -----------------------------------------------------------------------------

const getHealth = HttpApiEndpoint.get("getHealth", "/", {
  success: Domain.HealthcheckResponse
})

// -----------------------------------------------------------------------------
// GET /health/live
// -----------------------------------------------------------------------------

const getLiveness = HttpApiEndpoint.get("getLiveness", "/health/live", {
  success: Domain.LivenessResponse
})

// -----------------------------------------------------------------------------
// GET /health/ready
// -----------------------------------------------------------------------------

const getReadiness = HttpApiEndpoint.get("getReadiness", "/health/ready", {
  success: Domain.ReadinessResponse,
  error: Errors.ServiceUnavailableError
})

// -----------------------------------------------------------------------------
// Group Definition
// -----------------------------------------------------------------------------

/**
 * Api group for checking API health.
 */
export class HealthApiGroup extends HttpApiGroup.make("health")
  .add(getHealth)
  .add(getLiveness)
  .add(getReadiness)
{}

// =============================================================================
// Datasets
// =============================================================================

// -----------------------------------------------------------------------------
// GET /api/vX/datasets
// -----------------------------------------------------------------------------

const listDatasets = HttpApiEndpoint.get("listDatasets", "/datasets", {
  query: Domain.ListDatasetsParams,
  success: Domain.DatasetListResponse,
  error: [
    Errors.DatasetConversionError,
    Errors.InvalidQueryParametersError,
    Errors.LimitInvalidError,
    Errors.LimitTooLargeError,
    Errors.RegistryDatabaseError
  ]
})

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}
// -----------------------------------------------------------------------------

const getDatasetByFqdn = HttpApiEndpoint.get("getDatasetByFqdn", "/datasets/:namespace/:name", {
  params: {
    namespace: DatasetNamespaceParam,
    name: DatasetNameParam
  },
  success: Domain.Dataset,
  error: [
    Errors.DatasetConversionError,
    Errors.DatasetNotFoundError,
    Errors.InvalidDatasetSelectorError,
    Errors.RegistryDatabaseError
  ]
})

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/counts/by-chain
// -----------------------------------------------------------------------------

const getDatasetCountsByChain = HttpApiEndpoint.get("getDatasetCountsByChain", "/datasets/counts/by-chain", {
  success: Domain.DatasetCountsByChainResponse,
  error: Errors.RegistryDatabaseError
})

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/counts/by-keyword
// -----------------------------------------------------------------------------

const getDatasetCountsByKeyword = HttpApiEndpoint.get("getDatasetCountsByKeyword", "/datasets/counts/by-keyword", {
  success: Domain.DatasetCountsByKeywordResponse,
  error: Errors.RegistryDatabaseError
})

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/counts/by-last-updated
// -----------------------------------------------------------------------------

const getDatasetCountsByLastUpdated = HttpApiEndpoint.get(
  "getDatasetCountsByLastUpdated",
  "/datasets/counts/by-last-updated",
  {
    success: Domain.DatasetCountsByLastUpdatedResponse,
    error: Errors.RegistryDatabaseError
  }
)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/search
// -----------------------------------------------------------------------------

const searchDatasets = HttpApiEndpoint.get("searchDatasets", "/datasets/search", {
  query: Domain.SearchDatasetsParams,
  success: Domain.DatasetSearchResponse,
  error: [
    Errors.DatasetConversionError,
    Errors.InvalidQueryParametersError,
    Errors.LimitInvalidError,
    Errors.LimitTooLargeError,
    Errors.RegistryDatabaseError
  ]
})

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/search/ai
// -----------------------------------------------------------------------------

const aiSearchDatasets = HttpApiEndpoint.get("aiSearchDatasets", "/datasets/search/ai", {
  query: Domain.AiSearchDatasetsParams,
  success: Domain.DatasetAiSearchResponse,
  error: [
    Errors.DatasetConversionError,
    Errors.InvalidQueryParametersError,
    Errors.RegistryDatabaseError
  ]
})

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}/versions
// -----------------------------------------------------------------------------

const listDatasetVersions = HttpApiEndpoint.get("listDatasetVersions", "/datasets/:namespace/:name/versions", {
  params: {
    namespace: DatasetNamespaceParam,
    name: DatasetNameParam
  },
  success: Domain.DatasetListVersionsResponse,
  error: [
    Errors.DatasetVersionConversionError,
    Errors.InvalidSelectorError,
    Errors.RegistryDatabaseError
  ]
})

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}/versions/latest
// -----------------------------------------------------------------------------

const getLatestDatasetVersion = HttpApiEndpoint.get(
  "getLatestDatasetVersion",
  "/datasets/:namespace/:name/versions/latest",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    success: Domain.DatasetVersion,
    error: [
      Errors.DatasetVersionConversionError,
      Errors.LatestDatasetVersionNotFoundError,
      Errors.InvalidSelectorError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}/versions/{version}
// -----------------------------------------------------------------------------

const getDatasetVersionByRevision = HttpApiEndpoint.get(
  "getDatasetVersionByRevision",
  "/datasets/:namespace/:name/versions/:revision",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    success: Domain.DatasetVersion,
    error: [
      Errors.DatasetVersionConversionError,
      Errors.DatasetVersionNotFoundError,
      Errors.InvalidSelectorError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/latest/queries
// -----------------------------------------------------------------------------

const listLatestDatasetQueries = HttpApiEndpoint.get(
  "listLatestDatasetQueries",
  "/datasets/:namespace/:name/versions/latest/queries",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    success: Domain.DatasetListLatestQueriesResponse,
    error: [
      Errors.DatasetNotFoundError,
      Errors.InvalidDatasetReferenceError,
      Errors.InvalidSelectorError,
      Errors.RegistryDatabaseError,
      Errors.SavedQueryConversionError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/{version}/queries
// -----------------------------------------------------------------------------

const listDatasetQueries = HttpApiEndpoint.get(
  "listDatasetQueries",
  "/datasets/:namespace/:name/versions/:revision/queries",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    success: Domain.DatasetListQueriesResponse,
    error: [
      Errors.InvalidSelectorError,
      Errors.RegistryDatabaseError,
      Errors.SavedQueryConversionError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/latest/manifest
// -----------------------------------------------------------------------------

const getLatestDatasetManifest = HttpApiEndpoint.get(
  "getLatestDatasetManifest",
  "/datasets/:namespace/:name/versions/latest/manifest",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    success: Domain.DatasetGetLatestManifestResponse,
    error: [
      Errors.InvalidSelectorError,
      Errors.InvalidManifestHashError,
      Errors.ManifestNotFoundError,
      Errors.ManifestRetrievalError,
      Errors.ManifestDeserializationError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/{version}/manifest
// -----------------------------------------------------------------------------

const getDatasetManifest = HttpApiEndpoint.get(
  "getDatasetManifest",
  "/datasets/:namespace/:name/versions/:revision/manifest",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    success: Domain.DatasetGetManifestResponse,
    error: [
      Errors.InvalidSelectorError,
      Errors.InvalidManifestHashError,
      Errors.ManifestNotFoundError,
      Errors.ManifestRetrievalError,
      Errors.ManifestDeserializationError
    ]
  }
)

// -----------------------------------------------------------------------------
// Group Definition
// -----------------------------------------------------------------------------

// TODO: implement the SSE endpoint (not yet possible to implement streaming endpoints with HttpApi)

/**
 * Api group for all dataset endpoints.
 */
export class DatasetsApiGroup extends HttpApiGroup.make("datasets")
  .add(listDatasets)
  .add(getDatasetByFqdn)
  .add(getDatasetCountsByChain)
  .add(getDatasetCountsByKeyword)
  .add(getDatasetCountsByLastUpdated)
  .add(searchDatasets)
  .add(aiSearchDatasets)
  .add(listDatasetVersions)
  .add(getLatestDatasetVersion)
  .add(getDatasetVersionByRevision)
  .add(listLatestDatasetQueries)
  .add(listDatasetQueries)
  .add(getLatestDatasetManifest)
  .add(getDatasetManifest)
{}

// =============================================================================
// Owned Datasets
// =============================================================================

// -----------------------------------------------------------------------------
// GET /api/vX/owners/{owner}/datasets
// -----------------------------------------------------------------------------

const listOwnedDatasets = HttpApiEndpoint.get(
  "listOwnedDatasets",
  "/owners/:owner/datasets",
  {
    params: {
      owner: DatasetOwnerParam
    },
    query: Domain.ListOwnedDatasetsParams,
    success: Domain.ListMyDatasetsResponse,
    error: [
      Errors.DatasetConversionError,
      Errors.InvalidDatasetOwnerPathError,
      Errors.InvalidQueryParametersError,
      Errors.LimitInvalidError,
      Errors.LimitTooLargeError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/vX/owners/{owner}/datasets/search
// -----------------------------------------------------------------------------

const searchOwnedDatasets = HttpApiEndpoint.get(
  "searchOwnedDatasets",
  "/owners/:owner/datasets/search",
  {
    params: {
      owner: DatasetOwnerParam
    },
    query: Domain.SearchOwnedDatasetsParams,
    success: Domain.DatasetSearchResponse,
    error: [
      Errors.DatasetConversionError,
      Errors.InvalidDatasetOwnerPathError,
      Errors.InvalidQueryParametersError,
      Errors.LimitInvalidError,
      Errors.LimitTooLargeError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// Group Definition
// -----------------------------------------------------------------------------

/**
 * Api group for all owned dataset endpoints.
 */
export class OwnedDatasetsApiGroup extends HttpApiGroup.make("ownedDatasets")
  .add(listOwnedDatasets)
  .add(searchOwnedDatasets)
{}

// =============================================================================
// My Datasets
// =============================================================================

// -----------------------------------------------------------------------------
// GET /api/vX/owners/@me/datasets
// -----------------------------------------------------------------------------

const listMyDatasets = HttpApiEndpoint.get(
  "listMyDatasets",
  "/datasets",
  {
    query: Domain.ListOwnedDatasetsParams,
    headers: Domain.BearerAuthHeader,
    success: Domain.AuthUserOwnedDatasetListResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetConversionError,
      Errors.InvalidQueryParametersError,
      Errors.LimitInvalidError,
      Errors.LimitTooLargeError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/vX/owners/@me/datasets/{namespace}/{name}
// -----------------------------------------------------------------------------

const getMyDatasetByFqdn = HttpApiEndpoint.get(
  "getMyDatasetByFqdn",
  "/datasets/:namespace/:name",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    headers: Domain.BearerAuthHeader,
    success: Domain.Dataset,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetConversionError,
      Errors.DatasetNotFoundError,
      Errors.InvalidDatasetSelectorError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-chain
// -----------------------------------------------------------------------------

const getMyDatasetCountsByChain = HttpApiEndpoint.get(
  "getMyDatasetCountsByChain",
  "/datasets/counts/by-chain",
  {
    headers: Domain.BearerAuthHeader,
    success: Domain.OwnedDatasetCountsByChainResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-keyword
// -----------------------------------------------------------------------------

const getMyDatasetCountsByKeyword = HttpApiEndpoint.get(
  "getMyDatasetCountsByKeyword",
  "/datasets/counts/by-keyword",
  {
    headers: Domain.BearerAuthHeader,
    success: Domain.OwnedDatasetCountsByKeywordResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-last-updated
// -----------------------------------------------------------------------------

const getMyDatasetCountsByLastUpdated = HttpApiEndpoint.get(
  "getMyDatasetCountsByLastUpdated",
  "/datasets/counts/by-last-updated",
  {
    headers: Domain.BearerAuthHeader,
    success: Domain.OwnedDatasetsCountByLastUpdatedResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-status
// -----------------------------------------------------------------------------

const getMyDatasetCountsByStatus = HttpApiEndpoint.get(
  "getMyDatasetCountsByStatus",
  "/datasets/counts/by-status",
  {
    headers: Domain.BearerAuthHeader,
    success: Domain.OwnedDatasetCountsByStatusResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-visibility
// -----------------------------------------------------------------------------

const getMyDatasetCountsByVisibility = HttpApiEndpoint.get(
  "getMyDatasetCountsByVisibility",
  "/datasets/counts/by-visibility",
  {
    headers: Domain.BearerAuthHeader,
    success: Domain.OwnedDatasetCountsByVisibilityResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/vX/owners/@me/datasets/search
// -----------------------------------------------------------------------------

const searchMyDatasets = HttpApiEndpoint.get(
  "searchMyDatasets",
  "/datasets/search",
  {
    query: Domain.SearchOwnedDatasetsParams,
    headers: Domain.BearerAuthHeader,
    success: Domain.DatasetSearchResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetConversionError,
      Errors.InvalidQueryParametersError,
      Errors.LimitInvalidError,
      Errors.LimitTooLargeError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}/queries
// -----------------------------------------------------------------------------

const listMyDatasetQueries = HttpApiEndpoint.get(
  "listMyDatasetQueries",
  "/datasets/:namespace/:name/versions/:revision/queries",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    headers: Domain.BearerAuthHeader,
    success: Domain.OwnedDatasetListQueriesResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.InvalidSelectorError,
      Errors.ForbiddenError,
      Errors.RegistryDatabaseError,
      Errors.SavedQueryConversionError
    ]
  }
)

// -----------------------------------------------------------------------------
// POST /api/v1/owners/@me/datasets/publish
// -----------------------------------------------------------------------------

const publishMyDataset = HttpApiEndpoint.post(
  "publishMyDataset",
  "/datasets/publish",
  {
    headers: Domain.BearerAuthHeader,
    payload: Domain.InsertDatasetPayload,
    success: Schema.Void.pipe(HttpApiSchema.status(201)),
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetConversionError,
      Errors.InvalidManifestError,
      Errors.InvalidRequestBodyError,
      Errors.InvalidNamespaceError,
      Errors.NamespaceAccessDeniedError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish
// -----------------------------------------------------------------------------

const publishMyDatasetVersion = HttpApiEndpoint.post(
  "publishMyDatasetVersion",
  "/datasets/:namespace/:name/versions/publish",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    headers: Domain.BearerAuthHeader,
    payload: Domain.InsertDatasetVersion,
    success: Schema.Void.pipe(HttpApiSchema.status(201)),
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetVersionConversionError,
      Errors.InvalidManifestError,
      Errors.InvalidPathParametersError,
      Errors.InvalidRequestBodyError,
      Errors.NamespaceAccessDeniedError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// PUT /api/v1/owners/@me/datasets/{namespace}/{name}
// -----------------------------------------------------------------------------

const updateMyDatasetMetadata = HttpApiEndpoint.put(
  "updateMyDatasetMetadata",
  "/datasets/:namespace/:name",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    headers: Domain.BearerAuthHeader,
    payload: Domain.UpdateDatasetMetadataPayload,
    success: Domain.Dataset,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetConversionError,
      Errors.InvalidPathParametersError,
      Errors.InvalidRequestBodyError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// PATCH /api/v1/owners/@me/datasets/{namespace}/{name}/visibility
// -----------------------------------------------------------------------------

const updateMyDatasetVisibility = HttpApiEndpoint.patch(
  "updateMyDatasetVisibility",
  "/datasets/:namespace/:name/visibility",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam
    },
    headers: Domain.BearerAuthHeader,
    payload: Domain.UpdateDatasetVisibilityPayload,
    success: Domain.Dataset,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetNotFoundError,
      Errors.InvalidPathParametersError,
      Errors.InvalidRequestBodyError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// PATCH /api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}
// -----------------------------------------------------------------------------

const updateMyDatasetVersionStatus = HttpApiEndpoint.patch(
  "updateMyDatasetVersionStatus",
  "/datasets/:namespace/:name/versions/:revision",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    headers: Domain.BearerAuthHeader,
    payload: Domain.UpdateDatasetVersionStatusPayload,
    success: Domain.DatasetVersion,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetNotFoundError,
      Errors.DatasetVersionConversionError,
      Errors.InvalidPathParametersError,
      Errors.InvalidRequestBodyError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// DELETE /api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}
// -----------------------------------------------------------------------------

const archiveMyDatasetVersion = HttpApiEndpoint.delete(
  "archiveMyDatasetVersion",
  "/datasets/:namespace/:name/versions/:revision",
  {
    params: {
      namespace: DatasetNamespaceParam,
      name: DatasetNameParam,
      revision: DatasetRevisionParam
    },
    headers: Domain.BearerAuthHeader,
    success: Domain.ArchiveDatasetVersionResponse,
    error: [
      HttpApiError.Unauthorized,
      Errors.DatasetNotFoundError,
      Errors.InvalidPathParametersError,
      Errors.RegistryDatabaseError
    ]
  }
)

// -----------------------------------------------------------------------------
// Group Definition
// -----------------------------------------------------------------------------

/**
 * Api group for all dataset endpoints for the currently authenticated user.
 */
export class MyDatasetsApiGroup extends HttpApiGroup.make("myDatasets")
  .add(listMyDatasets)
  .add(getMyDatasetByFqdn)
  .add(getMyDatasetCountsByChain)
  .add(getMyDatasetCountsByKeyword)
  .add(getMyDatasetCountsByLastUpdated)
  .add(getMyDatasetCountsByStatus)
  .add(getMyDatasetCountsByVisibility)
  .add(listMyDatasetQueries)
  .add(searchMyDatasets)
  .add(publishMyDataset)
  .add(publishMyDatasetVersion)
  .add(updateMyDatasetVisibility)
  .add(updateMyDatasetMetadata)
  .add(updateMyDatasetVersionStatus)
  .add(archiveMyDatasetVersion)
  .prefix("/owners/@me")
{}

// =============================================================================
// API Definition
// =============================================================================

/**
 * The specification for the Amp Registry API (v1).
 */
export class ApiV1 extends HttpApi.make("AmpRegistryApiV1")
  .add(HealthApiGroup)
  .add(DatasetsApiGroup)
  .add(OwnedDatasetsApiGroup)
  .add(MyDatasetsApiGroup)
  .prefix("/api/v1")
{}
