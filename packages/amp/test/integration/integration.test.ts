import { AdminApi } from "@edgeandnode/amp/admin/service"
import { ArrowFlight } from "@edgeandnode/amp/arrow-flight"
import * as Models from "@edgeandnode/amp/core"
import { describe, expect, it } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import { anvilManifest } from "./fixtures/anvil-manifest.ts"
import { waitForJob } from "./helpers.ts"
import { IntegrationLayer } from "./layers.ts"

// =============================================================================
// Dataset Fixture — registers and deploys an Anvil dataset once
// =============================================================================

const NAMESPACE = Models.DatasetNamespace.make("_")
const DATASET_NAME = Models.DatasetName.make("anvil")
const REVISION = Models.DatasetTag.make("latest")
const VERSION = Models.DatasetVersion.make("1.0.0")

/**
 * Shared fixture exposing the dataset reference and job ID from a single
 * register -> deploy -> wait cycle. Built once per test suite via a Layer.
 */
class DatasetFixture extends Context.Tag("Test/DatasetFixture")<DatasetFixture, {
  readonly namespace: Models.DatasetNamespace
  readonly name: Models.DatasetName
  readonly revision: Models.DatasetRevision
  readonly jobId: number
}>() {}

const DatasetFixtureLayer = Layer.effect(
  DatasetFixture,
  Effect.gen(function*() {
    const admin = yield* AdminApi

    // Register with explicit version so "latest" tag is created
    yield* admin.registerDataset(NAMESPACE, DATASET_NAME, anvilManifest, VERSION)

    // Deploy with finite endBlock so the job completes
    const { jobId } = yield* admin.deployDataset(NAMESPACE, DATASET_NAME, REVISION, {
      endBlock: "5"
    })

    // Wait for the job to reach a terminal state
    const job = yield* waitForJob(jobId)
    if (job.status !== "COMPLETED") {
      return yield* Effect.die(
        new Error(`Expected job ${jobId} to complete, got status: ${job.status}`)
      )
    }

    // Brief pause to allow data to become queryable after job completion
    yield* Effect.sleep("2 seconds")

    return DatasetFixture.of({
      namespace: NAMESPACE,
      name: DATASET_NAME,
      revision: REVISION,
      jobId
    })
  })
)

/**
 * Full integration layer that includes the dataset fixture.
 * The fixture depends on AdminApi (from IntegrationLayer) and
 * registers/deploys once before all tests in its scope.
 */
const FullIntegrationLayer = DatasetFixtureLayer.pipe(
  Layer.provideMerge(IntegrationLayer)
)

// =============================================================================
// Helpers
// =============================================================================

/**
 * Collect all rows from a query result. `flight.query` returns an array of
 * `QueryResult` batches, each with a `data` array of rows. This flattens
 * them into a single array.
 */
const collectRows = <A>(batches: ReadonlyArray<{ readonly data: ReadonlyArray<A> }>): Array<A> =>
  batches.flatMap((b) => b.data)

// =============================================================================
// Tests
// =============================================================================

it.layer(FullIntegrationLayer, {
  timeout: "3 minutes",
  excludeTestServices: true
})("Integration", (it) => {
  // ===========================================================================
  // Tier 1-2: Smoke Tests
  // ===========================================================================

  describe("AdminApi smoke tests", () => {
    it.effect("getProviders returns configured providers", () =>
      Effect.gen(function*() {
        const admin = yield* AdminApi
        const response = yield* admin.getProviders
        expect(response.providers.length).toBeGreaterThan(0)
        // The Anvil provider should be present
        const anvil = response.providers.find((p) => p.network === "anvil")
        expect(anvil).toBeDefined()
      }))

    it.effect("getDatasets returns a list", () =>
      Effect.gen(function*() {
        const admin = yield* AdminApi
        const response = yield* admin.getDatasets
        expect(response.datasets).toBeInstanceOf(Array)
      }))

    it.effect("getWorkers returns a list", () =>
      Effect.gen(function*() {
        const admin = yield* AdminApi
        const response = yield* admin.getWorkers
        expect(response).toBeDefined()
      }))

    it.effect("getJobs returns a list", () =>
      Effect.gen(function*() {
        const admin = yield* AdminApi
        const response = yield* admin.getJobs()
        expect(response.jobs).toBeInstanceOf(Array)
      }))
  })

  describe("ArrowFlight smoke tests", () => {
    it.effect("executes a simple SQL query", () =>
      Effect.gen(function*() {
        const flight = yield* ArrowFlight
        const batches = yield* flight.query("SELECT 1 AS value")
        const rows = collectRows(batches)
        expect(rows.length).toBeGreaterThan(0)
      }))
  })

  // ===========================================================================
  // Tier 3: Dataset Lifecycle — Query Validation
  // ===========================================================================

  describe("Dataset lifecycle: query validation", () => {
    it.effect("queries anvil.blocks and validates structure", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const flight = yield* ArrowFlight
        const batches = yield* flight.query(
          `SELECT block_num, hash, parent_hash, timestamp FROM ${fixture.name}.blocks ORDER BY block_num ASC`
        )
        const rows = collectRows(batches)

        // Should have at least one block (blocks 0-5)
        expect(rows.length).toBeGreaterThan(0)

        // Validate first row structure
        const first = rows[0]
        expect(first).toHaveProperty("block_num")
        expect(first).toHaveProperty("hash")
        expect(first).toHaveProperty("parent_hash")
        expect(first).toHaveProperty("timestamp")
      }))

    it.effect("queries anvil.blocks and validates sequential block numbers", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const flight = yield* ArrowFlight
        const batches = yield* flight.query(
          `SELECT block_num FROM ${fixture.name}.blocks ORDER BY block_num ASC`
        )
        const rows = collectRows(batches)

        expect(rows.length).toBeGreaterThan(0)

        // Block numbers should be sequential starting from 0
        for (let i = 0; i < rows.length; i++) {
          expect(Number(rows[i]!.block_num)).toBe(i)
        }
      }))

    it.effect("queries anvil.transactions table", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const flight = yield* ArrowFlight
        const batches = yield* flight.query(
          `SELECT block_num, tx_hash, tx_index FROM ${fixture.name}.transactions ORDER BY block_num ASC, tx_index ASC`
        )
        const rows = collectRows(batches)

        // Anvil with --block-time mines empty blocks, so transactions may be empty.
        // Validate structure only if rows exist.
        expect(rows).toBeInstanceOf(Array)

        if (rows.length > 0) {
          const first = rows[0]
          expect(first).toHaveProperty("block_num")
          expect(first).toHaveProperty("tx_hash")
          expect(first).toHaveProperty("tx_index")
        }
      }))

    it.effect("queries anvil.logs and validates structure", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const flight = yield* ArrowFlight
        const batches = yield* flight.query(
          `SELECT block_num, log_index, address, topic0, data FROM ${fixture.name}.logs ORDER BY block_num ASC, log_index ASC`
        )
        const rows = collectRows(batches)

        // May or may not have logs depending on Anvil setup, but query should succeed
        expect(rows).toBeInstanceOf(Array)

        if (rows.length > 0) {
          const first = rows[0]
          expect(first).toHaveProperty("block_num")
          expect(first).toHaveProperty("log_index")
          expect(first).toHaveProperty("address")
        }
      }))

    it.effect("queries block count and validates range", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const flight = yield* ArrowFlight
        const batches = yield* flight.query(
          `SELECT COUNT(*) AS cnt FROM ${fixture.name}.blocks`
        )
        const rows = collectRows(batches)

        expect(rows.length).toBe(1)
        const count = Number(rows[0]!.cnt)
        // endBlock=5 means blocks 0-5 = 6 blocks
        expect(count).toBeGreaterThanOrEqual(1)
        expect(count).toBeLessThanOrEqual(6)
      }))
  })

  // ===========================================================================
  // Tier 3: Dataset Lifecycle — Job & Metadata
  // ===========================================================================

  describe("Dataset lifecycle: job inspection", () => {
    it.effect("getJobById returns the deployment job", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const admin = yield* AdminApi
        const job = yield* admin.getJobById(fixture.jobId)

        expect(job.id).toBe(fixture.jobId)
        expect(job.status).toBe("COMPLETED")
      }))

    // Server bug: GET /jobs without a status filter returns an empty array
    // even when jobs exist. GET /jobs?status=COMPLETED returns them fine,
    // and getJobById also succeeds. When the server is fixed, this test
    // should assert jobs.length > 0 and find a COMPLETED job.
    it.effect("getJobs returns empty without status filter", () =>
      Effect.gen(function*() {
        const admin = yield* AdminApi
        const response = yield* admin.getJobs()

        // Succeeds but returns no jobs unless a status filter is provided
        expect(response.jobs).toBeInstanceOf(Array)
        expect(response.jobs.length).toBe(0)
      }))
  })

  describe("Dataset lifecycle: metadata readback", () => {
    it.effect("getDatasets includes the registered dataset", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const admin = yield* AdminApi
        const response = yield* admin.getDatasets

        const found = response.datasets.find(
          (d) => d.namespace === fixture.namespace && d.name === fixture.name
        )
        expect(found).toBeDefined()
      }))

    it.effect("getDatasetVersion returns version info", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const admin = yield* AdminApi
        const version = yield* admin.getDatasetVersion(
          fixture.namespace,
          fixture.name,
          fixture.revision
        )

        expect(version.kind).toBe("evm-rpc")
        expect(version.namespace).toBe(fixture.namespace)
        expect(version.name).toBe(fixture.name)
      }))

    it.effect("getDatasetManifest returns the registered manifest", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const admin = yield* AdminApi
        const manifest = yield* admin.getDatasetManifest(
          fixture.namespace,
          fixture.name,
          fixture.revision
        )

        expect(manifest.kind).toBe("evm-rpc")
        if (manifest.kind === "evm-rpc") {
          expect(manifest.network).toBe("anvil")
          expect(Object.keys(manifest.tables)).toContain("blocks")
          expect(Object.keys(manifest.tables)).toContain("transactions")
          expect(Object.keys(manifest.tables)).toContain("logs")
        }
      }))

    it.effect("getDatasetVersions returns version info", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const admin = yield* AdminApi
        const response = yield* admin.getDatasetVersions(
          fixture.namespace,
          fixture.name
        )

        expect(response.namespace).toBe(fixture.namespace)
        expect(response.name).toBe(fixture.name)
        expect(response.versions.length).toBeGreaterThan(0)

        const first = response.versions[0]!
        expect(first.version).toBe(VERSION)
        expect(first.manifestHash).toBeDefined()
        expect(first.createdAt).toBeDefined()
        expect(first.updatedAt).toBeDefined()

        // Special tags should map "latest" to our version
        expect(response.specialTags.latest).toBe(VERSION)
      }))

    // Server bug: GET /datasets/:ns/:name/versions/:rev/sync-progress
    // returns HTTP 404 with an empty body. The SDK can't decode the empty
    // response into either the success or error schema, causing a ParseError
    // defect. When the server is fixed, this test should validate sync
    // progress tables.
    it.effect("getDatasetSyncProgress fails due to server 404 with empty body", () =>
      Effect.gen(function*() {
        const fixture = yield* DatasetFixture
        const admin = yield* AdminApi
        const exit = yield* admin.getDatasetSyncProgress(
          fixture.namespace,
          fixture.name,
          fixture.revision
        ).pipe(Effect.exit)

        expect(Exit.isFailure(exit)).toBe(true)
      }))
  })
})
