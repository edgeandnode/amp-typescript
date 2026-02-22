import * as AdminService from "@edgeandnode/amp/admin/service"
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as ArrowFlightNode from "@edgeandnode/amp/arrow-flight/node"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as fs from "node:fs"
import * as path from "node:path"
import { DockerComposeEnvironment, Wait } from "testcontainers"

// =============================================================================
// Configuration
// =============================================================================

const PROJECT_ROOT = path.resolve(import.meta.dirname, "../../../..")
const PROJECT_NAME = "amp-integration-tests"

const AMP_ADMIN_URL = "http://localhost:1610"
const AMP_FLIGHT_URL = "http://localhost:1602"

// =============================================================================
// Container Layer
// =============================================================================

const DockerComposeLive = Layer.scopedDiscard(
  Effect.acquireRelease(
    Effect.promise(async () => {
      fs.mkdirSync(path.join(PROJECT_ROOT, "infra/amp/data"), { recursive: true })
      fs.mkdirSync(path.join(PROJECT_ROOT, "infra/amp/datasets"), { recursive: true })

      return new DockerComposeEnvironment(PROJECT_ROOT, "docker-compose.yml")
        .withProjectName(PROJECT_NAME)
        .withWaitStrategy("postgres-1", Wait.forHealthCheck())
        .withWaitStrategy("amp-1", Wait.forListeningPorts())
        .withStartupTimeout(120_000)
        .up(["postgres", "anvil", "amp"])
    }),
    (environment) => Effect.promise(() => environment.down({ removeVolumes: true }))
  )
)

// =============================================================================
// Service Layers
// =============================================================================

/**
 * AdminApi layer — HTTP client talking to real Amp admin API.
 * No auth layer — Amp runs in dev mode.
 */
const AdminApiLive = AdminService.layer({ url: AMP_ADMIN_URL }).pipe(
  Layer.provide(NodeHttpClient.layerUndici)
)

/**
 * ArrowFlight layer — gRPC client talking to real Amp Arrow Flight server.
 * No auth layer — Amp runs in dev mode.
 */
const ArrowFlightLive = ArrowFlight.layer.pipe(
  Layer.provide(ArrowFlightNode.layerTransportGrpc({ baseUrl: AMP_FLIGHT_URL }))
)

// =============================================================================
// Composed Test Layer
// =============================================================================

/**
 * The complete integration test layer.
 *
 * `Layer.provideMerge(DockerComposeLive)` ensures containers start before
 * service layers connect. Since `DockerComposeLive` outputs `never`, the
 * final output is `AdminApi | ArrowFlight`.
 */
export const IntegrationLive = Layer.mergeAll(AdminApiLive, ArrowFlightLive).pipe(
  Layer.provideMerge(DockerComposeLive)
)
