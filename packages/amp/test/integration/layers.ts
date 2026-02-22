import * as AdminService from "@edgeandnode/amp/admin/service"
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as ArrowFlightNode from "@edgeandnode/amp/arrow-flight/node"
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as fs from "node:fs"
import * as path from "node:path"
import { DockerComposeEnvironment, Wait } from "testcontainers"

const PROJECT_ROOT = path.resolve(import.meta.dirname, "../../../..")
const PROJECT_NAME = "amp-integration-tests"

const AMP_ADMIN_URL = "http://localhost:1610"
const AMP_FLIGHT_URL = "http://localhost:1602"

const DockerComposeLayer = Layer.scopedDiscard(
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
