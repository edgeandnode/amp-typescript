import * as Auth from "@edgeandnode/amp/auth/service"
import * as NodeServices from "@effect/platform-node/NodeServices"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Path from "effect/Path"
import * as Command from "effect/unstable/cli/Command"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore"
import * as NodeOS from "node:os"
import PackageJson from "../package.json" with { type: "json" }
import { AuthCommand } from "./commands/auth.ts"
import { QueryCommand } from "./commands/query.ts"

const RootCommand = Command.make("amp").pipe(
  Command.withSubcommands([AuthCommand, QueryCommand])
)

const run = Command.run(RootCommand, {
  version: PackageJson["version"]
})

const CliCacheLayer = Layer.unwrap(
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
  HttpClientLayer
).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.orDie
)

export const Cli = Effect.provide(run, MainLayer)
