import * as HttpApi from "@effect/platform/HttpApi"
import * as HttpApiEndpoint from "@effect/platform/HttpApiEndpoint"
import * as HttpApiError from "@effect/platform/HttpApiError"
import * as HttpApiGroup from "@effect/platform/HttpApiGroup"
import * as HttpApiSchema from "@effect/platform/HttpApiSchema"
import * as Schema from "effect/Schema"
import * as Models from "../models.ts"
import * as Domain from "./domain.ts"
import * as Errors from "./error.ts"

// =============================================================================
// Registry API Params
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
 * A URL parameter for the dataset owners.
 */
const datasetOwnerParam = HttpApiSchema.param("owner", Schema.String)

// =============================================================================
// Health
// =============================================================================

// -----------------------------------------------------------------------------
// GET /
// -----------------------------------------------------------------------------

const getHealth = HttpApiEndpoint.get("getHealth")`/`.addSuccess(Domain.HealthcheckResponse)

// -----------------------------------------------------------------------------
// GET /health/live
// -----------------------------------------------------------------------------

const getLiveness = HttpApiEndpoint.get("getLiveness")`/health/live`.addSuccess(Domain.LivenessResponse)

// -----------------------------------------------------------------------------
// GET /health/ready
// -----------------------------------------------------------------------------

const getReadiness = HttpApiEndpoint.get("getReadiness")`/health/ready`
  .addSuccess(Domain.ReadinessResponse)
  .addError(Errors.ServiceUnavailableError)

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

const listDatasets = HttpApiEndpoint.get(
  "listDatasets"
)`/datasets`
  .setUrlParams(Domain.ListDatasetsParams)
  .addSuccess(Domain.DatasetListResponse)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.LimitInvalidError)
  .addError(Errors.LimitTooLargeError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}
// -----------------------------------------------------------------------------

const getDatasetByFqdn = HttpApiEndpoint.get(
  "getDatasetByFqdn"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}`
  .addSuccess(Domain.Dataset)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.DatasetNotFoundError)
  .addError(Errors.InvalidDatasetSelectorError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/counts/by-chain
// -----------------------------------------------------------------------------

const getDatasetCountsByChain = HttpApiEndpoint.get(
  "getDatasetCountsByChain"
)`/datasets/counts/by-chain`
  .addSuccess(Domain.DatasetCountsByChainResponse)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/counts/by-keyword
// -----------------------------------------------------------------------------

const getDatasetCountsByKeyword = HttpApiEndpoint.get(
  "getDatasetCountsByKeyword"
)`/datasets/counts/by-keyword`
  .addSuccess(Domain.DatasetCountsByKeywordResponse)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/counts/by-last-updated
// -----------------------------------------------------------------------------

const getDatasetCountsByLastUpdated = HttpApiEndpoint.get(
  "getDatasetCountsByLastUpdated"
)`/datasets/counts/by-last-updated`
  .addSuccess(Domain.DatasetCountsByLastUpdatedResponse)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/search
// -----------------------------------------------------------------------------

const searchDatasets = HttpApiEndpoint.get(
  "searchDatasets"
)`/datasets/search`
  .setUrlParams(Domain.SearchDatasetsParams)
  .addSuccess(Domain.DatasetSearchResponse)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.LimitInvalidError)
  .addError(Errors.LimitTooLargeError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/search/ai
// -----------------------------------------------------------------------------

const aiSearchDatasets = HttpApiEndpoint.get(
  "aiSearchDatasets"
)`/datasets/search/ai`
  .setUrlParams(Domain.AiSearchDatasetsParams)
  .addSuccess(Domain.DatasetAiSearchResponse)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}/versions
// -----------------------------------------------------------------------------

const listDatasetVersions = HttpApiEndpoint.get(
  "listDatasetVersions"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions`
  .addSuccess(Domain.DatasetListVersionsResponse)
  .addError(Errors.DatasetVersionConversionError)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}/versions/latest
// -----------------------------------------------------------------------------

const getLatestDatasetVersion = HttpApiEndpoint.get(
  "getLatestDatasetVersion"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/latest`
  .addSuccess(Domain.DatasetVersion)
  .addError(Errors.DatasetVersionConversionError)
  .addError(Errors.LatestDatasetVersionNotFoundError)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/datasets/{namespace}/{name}/versions/{version}
// -----------------------------------------------------------------------------

const getDatasetVersionByRevision = HttpApiEndpoint.get(
  "getDatasetVersionByRevision"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}`
  .addSuccess(Domain.DatasetVersion)
  .addError(Errors.DatasetVersionConversionError)
  .addError(Errors.DatasetVersionNotFoundError)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/latest/queries
// -----------------------------------------------------------------------------

const listLatestDatasetQueries = HttpApiEndpoint.get(
  "listLatestDatasetQueries"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/latest/queries`
  .addSuccess(Domain.DatasetListLatestQueriesResponse)
  .addError(Errors.DatasetNotFoundError)
  .addError(Errors.InvalidDatasetReferenceError)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.RegistryDatabaseError)
  .addError(Errors.SavedQueryConversionError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/{version}/queries
// -----------------------------------------------------------------------------

const listDatasetQueries = HttpApiEndpoint.get(
  "listDatasetQueries"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/queries`
  .addSuccess(Domain.DatasetListQueriesResponse)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.RegistryDatabaseError)
  .addError(Errors.SavedQueryConversionError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/latest/manifest
// -----------------------------------------------------------------------------

const getLatestDatasetManifest = HttpApiEndpoint.get(
  "getLatestDatasetManifest"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/latest/manifest`
  .addSuccess(Domain.DatasetGetLatestManifestResponse)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.InvalidManifestHashError)
  .addError(Errors.ManifestNotFoundError)
  .addError(Errors.ManifestRetrievalError)
  .addError(Errors.ManifestDeserializationError)

// -----------------------------------------------------------------------------
// GET /api/v1/datasets/{namespace}/{name}/versions/{version}/manifest
// -----------------------------------------------------------------------------

const getDatasetManifest = HttpApiEndpoint.get(
  "getDatasetManifest"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/manifest`
  .addSuccess(Domain.DatasetGetManifestResponse)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.InvalidManifestHashError)
  .addError(Errors.ManifestNotFoundError)
  .addError(Errors.ManifestRetrievalError)
  .addError(Errors.ManifestDeserializationError)

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
  "listOwnedDatasets"
)`/owners/${datasetOwnerParam}/datasets`
  .setUrlParams(Domain.ListOwnedDatasetsParams)
  .addSuccess(Domain.ListMyDatasetsResponse)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidDatasetOwnerPathError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.LimitInvalidError)
  .addError(Errors.LimitTooLargeError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/owners/{owner}/datasets/search
// -----------------------------------------------------------------------------

const searchOwnedDatasets = HttpApiEndpoint.get(
  "searchOwnedDatasets"
)`/owners/${datasetOwnerParam}/datasets/search`
  .setUrlParams(Domain.SearchOwnedDatasetsParams)
  .addSuccess(Domain.DatasetSearchResponse)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidDatasetOwnerPathError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.LimitInvalidError)
  .addError(Errors.LimitTooLargeError)
  .addError(Errors.RegistryDatabaseError)

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
  "listMyDatasets"
)`/datasets`
  .setUrlParams(Domain.ListOwnedDatasetsParams)
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.AuthUserOwnedDatasetListResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.LimitInvalidError)
  .addError(Errors.LimitTooLargeError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/owners/@me/datasets/{namespace}/{name}
// -----------------------------------------------------------------------------

const getMyDatasetByFqdn = HttpApiEndpoint.get(
  "getMyDatasetByFqdn"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}`
  .setUrlParams(Domain.GetOwnedDatasetsByFqdnParams)
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.Dataset)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.DatasetNotFoundError)
  .addError(Errors.InvalidDatasetSelectorError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-chain
// -----------------------------------------------------------------------------

const getMyDatasetCountsByChain = HttpApiEndpoint.get(
  "getMyDatasetCountsByChain"
)`/datasets/counts/by-chain`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.OwnedDatasetCountsByChainResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-keyword
// -----------------------------------------------------------------------------

const getMyDatasetCountsByKeyword = HttpApiEndpoint.get(
  "getMyDatasetCountsByKeyword"
)`/datasets/counts/by-keyword`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.OwnedDatasetCountsByKeywordResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-last-updated
// -----------------------------------------------------------------------------

const getMyDatasetCountsByLastUpdated = HttpApiEndpoint.get(
  "getMyDatasetCountsByLastUpdated"
)`/datasets/counts/by-last-updated`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.OwnedDatasetsCountByLastUpdatedResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-status
// -----------------------------------------------------------------------------

const getMyDatasetCountsByStatus = HttpApiEndpoint.get(
  "getMyDatasetCountsByStatus"
)`/datasets/counts/by-status`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.OwnedDatasetCountsByStatusResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/counts/by-visibility
// -----------------------------------------------------------------------------

const getMyDatasetCountsByVisibility = HttpApiEndpoint.get(
  "getMyDatasetCountsByVisibility"
)`/datasets/counts/by-visibility`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.OwnedDatasetCountsByVisibilityResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/vX/owners/@me/datasets/search
// -----------------------------------------------------------------------------

const searchMyDatasets = HttpApiEndpoint.get(
  "searchMyDatasets"
)`/datasets/search`
  .setUrlParams(Domain.SearchOwnedDatasetsParams)
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.DatasetSearchResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidQueryParametersError)
  .addError(Errors.LimitInvalidError)
  .addError(Errors.LimitTooLargeError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// GET /api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}/queries
// -----------------------------------------------------------------------------

const listMyDatasetQueries = HttpApiEndpoint.get(
  "listMyDatasetQueries"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}/queries`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.OwnedDatasetListQueriesResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.InvalidSelectorError)
  .addError(Errors.ForbiddenError)
  .addError(Errors.RegistryDatabaseError)
  .addError(Errors.SavedQueryConversionError)

// -----------------------------------------------------------------------------
// POST /api/v1/owners/@me/datasets/publish
// -----------------------------------------------------------------------------

const publishMyDataset = HttpApiEndpoint.post(
  "publishMyDataset"
)`/datasets/publish`
  .setHeaders(Domain.BearerAuthHeader)
  .setPayload(Domain.InsertDatasetPayload)
  .addSuccess(HttpApiSchema.Created)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidManifestError)
  .addError(Errors.InvalidRequestBodyError)
  .addError(Errors.InvalidNamespaceError)
  .addError(Errors.NamespaceAccessDeniedError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// POST /api/v1/owners/@me/datasets/{namespace}/{name}/versions/publish
// -----------------------------------------------------------------------------

const publishMyDatasetVersion = HttpApiEndpoint.post(
  "publishMyDatasetVersion"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/publish`
  .setHeaders(Domain.BearerAuthHeader)
  .setPayload(Domain.InsertDatasetVersion)
  .addSuccess(HttpApiSchema.Created)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetVersionConversionError)
  .addError(Errors.InvalidManifestError)
  .addError(Errors.InvalidPathParametersError)
  .addError(Errors.InvalidRequestBodyError)
  .addError(Errors.NamespaceAccessDeniedError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// PUT /api/v1/owners/@me/datasets/{namespace}/{name}
// -----------------------------------------------------------------------------

const updateMyDatasetMetadata = HttpApiEndpoint.put(
  "updateMyDatasetMetadata"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}`
  .setHeaders(Domain.BearerAuthHeader)
  .setPayload(Domain.UpdateDatasetMetadataPayload)
  .addSuccess(Domain.Dataset)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetConversionError)
  .addError(Errors.InvalidPathParametersError)
  .addError(Errors.InvalidRequestBodyError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// PATCH /api/v1/owners/@me/datasets/{namespace}/{name}/visibility
// -----------------------------------------------------------------------------

const updateMyDatasetVisibility = HttpApiEndpoint.patch(
  "updateMyDatasetVisibility"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/visibility`
  .setHeaders(Domain.BearerAuthHeader)
  .setPayload(Domain.UpdateDatasetVisibilityPayload)
  .addSuccess(Domain.Dataset)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetNotFoundError)
  .addError(Errors.InvalidPathParametersError)
  .addError(Errors.InvalidRequestBodyError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// PATCH /api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}
// -----------------------------------------------------------------------------

const updateMyDatasetVersionStatus = HttpApiEndpoint.patch(
  "updateMyDatasetVersionStatus"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}`
  .setHeaders(Domain.BearerAuthHeader)
  .setPayload(Domain.UpdateDatasetVersionStatusPayload)
  .addSuccess(Domain.DatasetVersion)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetNotFoundError)
  .addError(Errors.DatasetVersionConversionError)
  .addError(Errors.InvalidPathParametersError)
  .addError(Errors.InvalidRequestBodyError)
  .addError(Errors.RegistryDatabaseError)

// -----------------------------------------------------------------------------
// DELETE /api/v1/owners/@me/datasets/{namespace}/{name}/versions/{version}
// -----------------------------------------------------------------------------

const archiveMyDatasetVersion = HttpApiEndpoint.del(
  "archiveMyDatasetVersion"
)`/datasets/${datasetNamespaceParam}/${datasetNameParam}/versions/${datasetRevisionParam}`
  .setHeaders(Domain.BearerAuthHeader)
  .addSuccess(Domain.ArchiveDatasetVersionResponse)
  .addError(HttpApiError.Unauthorized)
  .addError(Errors.DatasetNotFoundError)
  .addError(Errors.InvalidPathParametersError)
  .addError(Errors.RegistryDatabaseError)

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
