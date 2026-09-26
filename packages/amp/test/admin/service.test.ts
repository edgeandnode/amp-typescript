import * as AdminApi from "@edgeandnode/amp/admin/service"
import * as Auth from "@edgeandnode/amp/auth/service"
import type * as Models from "@edgeandnode/amp/core/domain"
import { assert, describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore"

// Response payloads mirror the serialized shapes of the Amp admin API
// (see `docs/schemas/openapi/admin.spec.json` in the Amp repository).

interface StubResponse {
  readonly status: number
  readonly body?: unknown
}

const makeLayer = (routes: Record<string, StubResponse>) => {
  const client = HttpClient.make((request, url) => {
    const route = routes[`${request.method} ${url.pathname}`]
    const response =
      route === undefined
        ? new Response(null, { status: 404 })
        : new Response(route.body === undefined ? null : JSON.stringify(route.body), {
            status: route.status,
            headers: { "content-type": "application/json" }
          })
    return Effect.succeed(HttpClientResponse.fromWeb(request, response))
  })
  return AdminApi.layer({ url: "http://localhost:1610" }).pipe(
    Layer.provide(Layer.succeed(HttpClient.HttpClient, client))
  )
}

const hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
const arrowSchema = { arrow: { fields: [{ name: "block_num", type: "UInt64", nullable: false }] } }

describe("AdminApi", () => {
  it.effect("registerDataset decodes the 201 registration response", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.registerDataset(
        "edgeandnode" as Models.DatasetNamespace,
        "transfers" as Models.DatasetName,
        hash as Models.DatasetHash
      )
      assert.strictEqual(response.manifestHash, hash)
      assert.strictEqual(response.kind, "manifest")
      assert.strictEqual(response.version, undefined)
      assert.deepStrictEqual(response.tables, ["transfers"])
    }).pipe(
      Effect.provide(
        makeLayer({
          "POST /datasets": {
            status: 201,
            body: {
              namespace: "edgeandnode",
              name: "transfers",
              manifest_hash: hash,
              kind: "manifest",
              start_block: 0,
              finalized_blocks_only: false,
              tables: ["transfers"]
            }
          }
        })
      )
    )
  )

  it.effect("registerDataset decodes a namespace-not-found error", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const error = yield* admin
        .registerDataset(
          "missing" as Models.DatasetNamespace,
          "transfers" as Models.DatasetName,
          hash as Models.DatasetHash
        )
        .pipe(Effect.flip)
      assert.strictEqual(error._tag, "NamespaceNotFoundError")
    }).pipe(
      Effect.provide(
        makeLayer({
          "POST /datasets": {
            status: 404,
            body: { error_code: "NAMESPACE_NOT_FOUND", error_message: "namespace 'missing' not found" }
          }
        })
      )
    )
  )

  it.effect("getDatasetVersions decodes version details and special tags", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.getDatasetVersions(
        "edgeandnode" as Models.DatasetNamespace,
        "transfers" as Models.DatasetName
      )
      assert.strictEqual(response.versions.length, 1)
      assert.strictEqual(response.versions[0].version, "1.0.0")
      assert.strictEqual(response.versions[0].manifestHash, hash)
      assert.strictEqual(response.specialTags.latest, "1.0.0")
      assert.strictEqual(response.specialTags.dev, hash)
    }).pipe(
      Effect.provide(
        makeLayer({
          "GET /datasets/edgeandnode/transfers/versions": {
            status: 200,
            body: {
              namespace: "edgeandnode",
              name: "transfers",
              versions: [
                {
                  version: "1.0.0",
                  manifest_hash: hash,
                  created_at: "2026-09-01T12:00:00+00:00",
                  updated_at: "2026-09-02T12:00:00+00:00"
                }
              ],
              special_tags: { latest: "1.0.0", dev: hash }
            }
          }
        })
      )
    )
  )

  it.effect("getDatasetVersion decodes newer dataset kinds and tags", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.getDatasetVersion(
        "edgeandnode" as Models.DatasetNamespace,
        "solana_mainnet" as Models.DatasetName,
        "latest" as Models.DatasetRevision
      )
      assert.strictEqual(response.kind, "solana")
      assert.deepStrictEqual(response.tags, ["1.0.0", "latest"])
    }).pipe(
      Effect.provide(
        makeLayer({
          "GET /datasets/edgeandnode/solana_mainnet/versions/latest": {
            status: 200,
            body: {
              namespace: "edgeandnode",
              name: "solana_mainnet",
              revision: "latest",
              manifest_hash: hash,
              kind: "solana",
              start_block: 250000000,
              finalized_blocks_only: true,
              tables: ["blocks", "transactions"],
              tags: ["1.0.0", "latest"]
            }
          }
        })
      )
    )
  )

  it.effect("getDatasetManifest decodes a derived manifest without table networks or function names", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const manifest = yield* admin.getDatasetManifest(
        "edgeandnode" as Models.DatasetNamespace,
        "transfers" as Models.DatasetName,
        "dev" as Models.DatasetRevision
      )
      assert.strictEqual(manifest.kind, "manifest")
      if (manifest.kind !== "manifest") return
      assert.strictEqual(manifest.dependencies?.eth.name, "eth_mainnet")
      assert.strictEqual(manifest.tables?.transfers.input.sql, "SELECT * FROM eth.logs")
      assert.strictEqual(manifest.functions?.decode.outputType, "Utf8")
    }).pipe(
      Effect.provide(
        makeLayer({
          "GET /datasets/edgeandnode/transfers/versions/dev/manifest": {
            status: 200,
            body: {
              kind: "manifest",
              dependencies: { eth: "edgeandnode/eth_mainnet@1.0.0" },
              tables: {
                transfers: {
                  input: { sql: "SELECT * FROM eth.logs" },
                  schema: arrowSchema,
                  bloom_filter_columns: [{ column: "address" }]
                }
              },
              functions: {
                decode: {
                  inputTypes: ["Binary"],
                  outputType: "Utf8",
                  source: { source: "export default () => ''", filename: "decode.js" }
                }
              }
            }
          }
        })
      )
    )
  )

  it.effect("getDatasetManifest decodes raw and static manifests", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const raw = yield* admin.getDatasetManifest(
        "edgeandnode" as Models.DatasetNamespace,
        "solana_mainnet" as Models.DatasetName,
        "latest" as Models.DatasetRevision
      )
      assert.strictEqual(raw.kind, "solana")
      const fixture = yield* admin.getDatasetManifest(
        "edgeandnode" as Models.DatasetNamespace,
        "fixture" as Models.DatasetName,
        "latest" as Models.DatasetRevision
      )
      assert.strictEqual(fixture.kind, "static")
    }).pipe(
      Effect.provide(
        makeLayer({
          "GET /datasets/edgeandnode/solana_mainnet/versions/latest/manifest": {
            status: 200,
            body: {
              kind: "solana",
              network: "solana-mainnet",
              start_block: 250000000,
              finalized_blocks_only: true,
              state_snapshots: { selectors: ["accounts", { selector: "programs", block_interval: 100 }] },
              tables: {
                blocks: {
                  schema: arrowSchema,
                  network: "solana-mainnet",
                  sorted_by: ["block_num"],
                  segment_selection_policy: "ordered"
                }
              }
            }
          },
          "GET /datasets/edgeandnode/fixture/versions/latest/manifest": {
            status: 200,
            body: {
              kind: "static",
              tables: {
                tokens: { path: "data/tokens.csv", schema: arrowSchema, format: "csv", has_header: true }
              }
            }
          }
        })
      )
    )
  )

  it.effect("getJobs decodes jobs in the newer terminal states", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.getJobs({ status: "all" })
      assert.deepStrictEqual(
        response.jobs.map((job) => job.status),
        ["ERROR", "FATAL", "CANCELLED"]
      )
      assert.strictEqual(response.jobs[0].idempotencyKey, hash)
      assert.strictEqual(response.nextCursor, undefined)
    }).pipe(
      Effect.provide(
        makeLayer({
          "GET /jobs": {
            status: 200,
            body: {
              jobs: ["ERROR", "FATAL", "CANCELLED"].map((status, id) => ({
                id,
                idempotency_key: hash,
                created_at: "2026-09-01T12:00:00+00:00",
                updated_at: "2026-09-02T12:00:00+00:00",
                node_id: "worker-01",
                status,
                descriptor: null
              }))
            }
          }
        })
      )
    )
  )

  it.effect("getOutputSchema decodes table schemas keyed by table name", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.getOutputSchema({ tables: { transfers: "SELECT * FROM eth.logs" } })
      assert.strictEqual(response.schemas.transfers.arrow.fields[0].name, "block_num")
    }).pipe(
      Effect.provide(
        makeLayer({
          "POST /schema": { status: 200, body: { schemas: { transfers: arrowSchema } } }
        })
      )
    )
  )

  it.effect("getOutputSchema decodes a missing dependency table as a 400 error", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const error = yield* admin
        .getOutputSchema({ tables: { transfers: "SELECT * FROM eth.missing" } })
        .pipe(Effect.flip)
      assert.strictEqual(error._tag, "TableNotFoundInDatasetError")
    }).pipe(
      Effect.provide(
        makeLayer({
          "POST /schema": {
            status: 400,
            body: { error_code: "TABLE_NOT_FOUND_IN_DATASET", error_message: "table 'missing' not found" }
          }
        })
      )
    )
  )

  it.effect("getProviders keeps provider-specific configuration", () =>
    Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.getProviders
      const provider = response.providers[0]
      assert.strictEqual(provider.name, "mainnet-rpc")
      assert.strictEqual(provider.kind, "evm-rpc")
      assert.strictEqual(provider["url"], "http://localhost:8545")
    }).pipe(
      Effect.provide(
        makeLayer({
          "GET /providers": {
            status: 200,
            body: {
              providers: [{ name: "mainnet-rpc", kind: "evm-rpc", network: "mainnet", url: "http://localhost:8545" }]
            }
          }
        })
      )
    )
  )
})

describe("AdminApi authentication", () => {
  const makeRecordingClient = (authorization: Array<string | undefined>) =>
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) => {
        authorization.push(request.headers["authorization"])
        const body = JSON.stringify({ workers: [] })
        const response = new Response(body, { status: 200, headers: { "content-type": "application/json" } })
        return Effect.succeed(HttpClientResponse.fromWeb(request, response))
      })
    )

  // Uses `layerAuth` alongside a separate `Auth` instance sharing the same
  // in-memory store, so the test can seed the cached auth info.
  const makeAuthLayer = (options: AdminApi.MakeOptions, authorization: Array<string | undefined>) =>
    Layer.mergeAll(AdminApi.layerAuth(options), Auth.layer).pipe(
      Layer.provide(KeyValueStore.layerMemory),
      Layer.provide(makeRecordingClient(authorization))
    )

  const seedCachedAuthInfo = Effect.gen(function* () {
    const auth = yield* Auth.Auth
    yield* auth.setCachedAuthInfo({
      accessToken: Redacted.make("cached-token" as Models.AccessToken),
      refreshToken: Redacted.make("refresh-token" as Models.RefreshToken),
      userId: "c0123456789abcdefghijklmn" as Models.UserId,
      accounts: [],
      // Far enough in the future that the cached token is not refreshed
      expiry: Date.now() + 60 * 60 * 1000
    })
  })

  it.effect("sends the provided jwt without the Auth service", () => {
    const authorization: Array<string | undefined> = []
    return Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      yield* admin.getWorkers
      assert.deepStrictEqual(authorization, ["Bearer provided-jwt"])
    }).pipe(
      Effect.provide(
        AdminApi.layer({ url: "http://localhost:1610", jwt: "provided-jwt" }).pipe(
          Layer.provide(makeRecordingClient(authorization))
        )
      )
    )
  })

  it.effect("sends no authorization without a jwt or the Auth service", () => {
    const authorization: Array<string | undefined> = []
    return Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      yield* admin.getWorkers
      assert.deepStrictEqual(authorization, [undefined])
    }).pipe(
      Effect.provide(
        AdminApi.layer({ url: "http://localhost:1610", jwt: null }).pipe(
          Layer.provide(makeRecordingClient(authorization))
        )
      )
    )
  })

  it.effect("prefers the provided jwt over the cached auth info", () => {
    const authorization: Array<string | undefined> = []
    return Effect.gen(function* () {
      yield* seedCachedAuthInfo
      const admin = yield* AdminApi.AdminApi
      yield* admin.getWorkers
      assert.deepStrictEqual(authorization, ["Bearer provided-jwt"])
    }).pipe(Effect.provide(makeAuthLayer({ url: "http://localhost:1610", jwt: "provided-jwt" }, authorization)))
  })

  it.effect("falls back to the cached auth info without a jwt", () => {
    const authorization: Array<string | undefined> = []
    return Effect.gen(function* () {
      yield* seedCachedAuthInfo
      const admin = yield* AdminApi.AdminApi
      yield* admin.getWorkers
      assert.deepStrictEqual(authorization, ["Bearer cached-token"])
    }).pipe(Effect.provide(makeAuthLayer({ url: "http://localhost:1610" }, authorization)))
  })

  it.effect("sends no authorization when nothing is cached and no jwt is provided", () => {
    const authorization: Array<string | undefined> = []
    return Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      yield* admin.getWorkers
      assert.deepStrictEqual(authorization, [undefined])
    }).pipe(Effect.provide(makeAuthLayer({ url: "http://localhost:1610" }, authorization)))
  })
})

describe("AdminApi lineage", () => {
  const makeLineageLayer = (requests: Array<URL>, response: StubResponse) =>
    AdminApi.layer({ url: "http://localhost:1610" }).pipe(
      Layer.provide(
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request, url) => {
            requests.push(url)
            const body = new Response(JSON.stringify(response.body), {
              status: response.status,
              headers: { "content-type": "application/json" }
            })
            return Effect.succeed(HttpClientResponse.fromWeb(request, body))
          })
        )
      )
    )

  const lineage = {
    anchor: "edgeandnode/transfers@1.0.0",
    nodes: [
      { dataset: "edgeandnode/transfers@1.0.0", kind: "manifest", status: "ok", resolved_hash: hash, depth: 0 },
      { dataset: "edgeandnode/eth_mainnet@latest", kind: "evm-rpc", status: "ok", resolved_hash: hash, depth: 1 },
      { dataset: "edgeandnode/removed@1.0.0", kind: null, status: "missing", resolved_hash: null, depth: 1 }
    ],
    edges: [
      {
        source: "edgeandnode/eth_mainnet@latest",
        target: "edgeandnode/transfers@1.0.0",
        alias: "eth",
        resolved_source: `edgeandnode/eth_mainnet@${hash}`
      },
      {
        source: "edgeandnode/removed@1.0.0",
        target: "edgeandnode/transfers@1.0.0",
        alias: "removed",
        resolved_source: null
      }
    ],
    depth: 1,
    truncated: false,
    cycles: ["edgeandnode/transfers@1.0.0"]
  }

  it.effect("getDatasetLineage decodes the lineage graph", () => {
    const requests: Array<URL> = []
    return Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const response = yield* admin.getDatasetLineage(
        "edgeandnode" as Models.DatasetNamespace,
        "transfers" as Models.DatasetName,
        "1.0.0" as Models.DatasetRevision
      )
      assert.strictEqual(requests[0].pathname, "/datasets/edgeandnode/transfers/versions/1.0.0/lineage")
      assert.strictEqual(requests[0].search, "")
      assert.deepStrictEqual(response.anchor, { namespace: "edgeandnode", name: "transfers", revision: "1.0.0" })
      assert.strictEqual(response.nodes.length, 3)
      assert.strictEqual(response.nodes[1].kind, "evm-rpc")
      assert.strictEqual(response.nodes[2].status, "missing")
      assert.strictEqual(response.nodes[2].kind, null)
      assert.strictEqual(response.nodes[2].resolvedHash, null)
      assert.strictEqual(response.edges[0].resolvedSource?.revision, hash)
      assert.strictEqual(response.edges[1].resolvedSource, null)
      assert.strictEqual(response.cycles[0].name, "transfers")
    }).pipe(Effect.provide(makeLineageLayer(requests, { status: 200, body: lineage })))
  })

  it.effect("getDatasetLineage encodes the traversal options as query parameters", () => {
    const requests: Array<URL> = []
    return Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      yield* admin.getDatasetLineage(
        "edgeandnode" as Models.DatasetNamespace,
        "transfers" as Models.DatasetName,
        "latest" as Models.DatasetRevision,
        { direction: "both", maxDepth: 2, maxNodes: 50 }
      )
      const params = requests[0].searchParams
      assert.strictEqual(params.get("direction"), "both")
      assert.strictEqual(params.get("max_depth"), "2")
      assert.strictEqual(params.get("max_nodes"), "50")
    }).pipe(Effect.provide(makeLineageLayer(requests, { status: 200, body: lineage })))
  })

  it.effect("getDatasetLineage decodes a lineage build failure", () => {
    const requests: Array<URL> = []
    return Effect.gen(function* () {
      const admin = yield* AdminApi.AdminApi
      const error = yield* admin
        .getDatasetLineage(
          "edgeandnode" as Models.DatasetNamespace,
          "transfers" as Models.DatasetName,
          "1.0.0" as Models.DatasetRevision
        )
        .pipe(Effect.flip)
      assert.strictEqual(error._tag, "BuildLineageError")
    }).pipe(
      Effect.provide(
        makeLineageLayer(requests, {
          status: 500,
          body: { error_code: "BUILD_LINEAGE_ERROR", error_message: "failed to build lineage graph" }
        })
      )
    )
  })
})
