import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Auth from "@edgeandnode/amp/auth/service"
import * as CliConfig from "@effect/cli/CliConfig"
import * as Command from "@effect/cli/Command"
import * as NodeContext from "@effect/platform-node/NodeContext"
import * as FetchHttpClient from "@effect/platform/FetchHttpClient"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as Path from "@effect/platform/Path"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as NodeOS from "node:os"
import PackageJson from "../package.json" with { type: "json" }
import { AuthCommand } from "./commands/auth.ts"
import { QueryCommand } from "./commands/query.ts"

const RootCommand = Command.make("amp").pipe(
  Command.withSubcommands([AuthCommand, QueryCommand])
)

const run = Command.run(RootCommand, {
  name: "Amp",
  version: PackageJson["version"]
})

const CliConfigLayer = CliConfig.layer({
  showBuiltIns: false
})

const CliCacheLayer = Layer.unwrapEffect(
  Effect.gen(function*() {
    const path = yield* Path.Path

    const homeDirectory = NodeOS.homedir()
    const ampCachePath = path.join(homeDirectory, ".amp", "cache")

    return KeyValueStore.layerFileSystem(ampCachePath)
  })
)

const HttpClientLayer = FetchHttpClient.layer

const AuthLayer = Auth.layer.pipe(
  Layer.provide(CliCacheLayer),
  Layer.provide(HttpClientLayer)
)

const MainLayer = Layer.mergeAll(
  AuthLayer,
  CliConfigLayer,
  HttpClientLayer
).pipe(
  Layer.provideMerge(NodeContext.layer),
  Layer.orDie
)

export const Cli = run(process.argv).pipe(
  Effect.provide(MainLayer),
  Effect.catchTag("Amp/NonZeroExitCode", () => Effect.sync(() => process.exit(1)))
)
