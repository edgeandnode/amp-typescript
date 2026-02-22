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

// =============================================================================
// Configuration
// =============================================================================

const PROJECT_NAME = "amp-integration-tests"

const AMP_ADMIN_URL = "http://localhost:1610"
const AMP_FLIGHT_URL = "http://localhost:1602"

// =============================================================================
// Container Layer
// =============================================================================

/**
 * Starts the Docker Compose environment before any services connect and
 * tears it down when the test scope closes.
 *
 * Produces no service output (`Layer<never>`). Other layers are provided
 * on top of it to guarantee evaluation ordering.
 */
const DockerComposeLayer = Layer.scopedDiscard(
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path

    const projectRoot = path.resolve(import.meta.dirname, "../../../..")

    yield* fs.makeDirectory(path.join(projectRoot, "infra/amp/data"), { recursive: true })
    yield* fs.makeDirectory(path.join(projectRoot, "infra/amp/datasets"), { recursive: true })

    yield* Effect.acquireRelease(
      Effect.promise(() =>
        new DockerComposeEnvironment(projectRoot, "docker-compose.yml")
          .withProjectName(PROJECT_NAME)
          .withWaitStrategy("postgres-1", Wait.forHealthCheck())
          .withWaitStrategy("amp-1", Wait.forListeningPorts())
          .withStartupTimeout(120_000)
          .up(["postgres", "anvil", "amp"])
      ),
      (environment) => Effect.promise(() => environment.down({ removeVolumes: true }))
    )
  })
)

// =============================================================================
// Service Layers
// =============================================================================

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

// =============================================================================
// Composed Test Layer
// =============================================================================

/**
 * The complete integration test layer.
 *
 * `Layer.provideMerge(DockerComposeLayer)` ensures containers start before
 * service layers connect. Since `DockerComposeLayer` outputs `never`, the
 * final output is `AdminApi | ArrowFlight`.
 */
export const IntegrationLayer = Layer.mergeAll(AdminApiLayer, ArrowFlightLayer).pipe(
  Layer.provideMerge(DockerComposeLayer),
  Layer.provide(NodeContext.layer)
)
