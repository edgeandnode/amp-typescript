import * as AdminService from "@edgeandnode/amp/admin/service"
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as ArrowFlightNode from "@edgeandnode/amp/arrow-flight/node"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { DockerComposeEnvironment, Wait } from "testcontainers"

const AMP_ADMIN_URL = "http://localhost:1610"
const AMP_FLIGHT_URL = "http://localhost:1602"

/**
 * Resolve the monorepo root from `import.meta.dirname` (which is `packages/amp/test/integration/`).
 * Four levels up: integration → test → amp → packages → root.
 */
const resolveProjectRoot = Effect.fn("resolveProjectRoot")(function*() {
  const path = yield* Path.Path
  return path.resolve(import.meta.dirname, "../../../..")
})

/**
 * Ensure required Amp infrastructure directories exist and are clean.
 * Amp mounts these as volumes — stale data from previous runs can
 * cause the server to enter a conflicting state.
 */
const ensureCleanDirectories = Effect.fn("ensureCleanDirectories")(function*(projectRoot: string) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const dataDirs = [
    path.join(projectRoot, "infra/amp/data"),
    path.join(projectRoot, "infra/amp/datasets")
  ]
  for (const dir of dataDirs) {
    yield* fs.remove(dir, { recursive: true }).pipe(Effect.ignore)
    yield* fs.makeDirectory(dir, { recursive: true })
  }
})

const DockerComposeLayer = Layer.scopedDiscard(
  Effect.acquireRelease(
    Effect.gen(function*() {
      const projectRoot = yield* resolveProjectRoot()

      yield* ensureCleanDirectories(projectRoot)

      const env = yield* Effect.promise(() =>
        new DockerComposeEnvironment(projectRoot, "docker-compose.yml")
          .withProjectName("amp-integration-tests")
          .withWaitStrategy("postgres-1", Wait.forHealthCheck())
          .withWaitStrategy("amp-1", Wait.forLogMessage(/Admin API running at/))
          .withStartupTimeout(120_000)
          .up(["postgres", "anvil", "amp"])
      )

      return env
    }),
    (environment) => Effect.promise(() => environment.down({ removeVolumes: true }))
  )
).pipe(Layer.provide(NodeContext.layer))

/**
 * AdminApi layer — HTTP client talking to real Amp admin API.
 * No auth layer — Amp runs in dev mode.
 */
const AdminApiLayer = AdminService.layer({ url: AMP_ADMIN_URL }).pipe(
  Layer.provide(NodeHttpClient.layerUndici)
)

/**
 * ArrowFlight layer — gRPC client talking to real Amp Arrow Flight server.
 * No auth layer — Amp runs in dev mode.
 */
const ArrowFlightLayer = ArrowFlight.layer.pipe(
  Layer.provide(ArrowFlightNode.layerTransportGrpc({ baseUrl: AMP_FLIGHT_URL }))
)

export const IntegrationLayer = Layer.mergeAll(AdminApiLayer, ArrowFlightLayer).pipe(
  Layer.provideMerge(DockerComposeLayer)
)
