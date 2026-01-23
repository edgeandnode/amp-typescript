import * as FileSystem from "@effect/platform/FileSystem"
import * as Path from "@effect/platform/Path"
import type * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Either from "effect/Either"
import { identity } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Match from "effect/Match"
import type * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import * as fs from "node:fs"
import * as path from "node:path"
import * as ManifestBuilder from "./manifest-builder/service.ts"
import * as Models from "./Models.ts"

export class ModuleContext {
  public definitionPath: string

  constructor(definitionPath: string) {
    this.definitionPath = definitionPath
  }

  /**
   * Reads a file relative to the directory of the dataset definition.
   */
  functionSource(relativePath: string): Models.FunctionSource {
    const baseDir = path.dirname(path.resolve(this.definitionPath))
    const fullPath = path.resolve(baseDir, relativePath)
    if (!fullPath.startsWith(baseDir + path.sep)) {
      throw new Error(`Invalid path: directory traversal not allowed`)
    }

    let source: string
    try {
      source = fs.readFileSync(fullPath, "utf8")
    } catch (err: any) {
      throw new Error(
        `Failed to read function source at ${fullPath}: ${err.message}`,
        { cause: err }
      )
    }

    const func = Models.FunctionSource.make({
      source,
      filename: path.basename(fullPath)
    })
    return func
  }
}

export class ConfigLoaderError extends Data.TaggedError("ConfigLoaderError")<{
  readonly cause?: unknown
  readonly message?: string
}> {}

export class ConfigLoader extends Context.Tag("Amp/ConfigLoader")<ConfigLoader, {
  /**
   * Loads a dataset configuration from a file.
   */
  readonly load: (file: string) => Effect.Effect<Models.DatasetConfig, ConfigLoaderError>

  /**
   * Finds a config file in the given directory by checking for known config file names.
   */
  readonly find: (cwd?: string) => Effect.Effect<Option.Option<string>, ConfigLoaderError>

  /**
   * Loads and builds a dataset configuration from a file.
   */
  readonly build: (file: string) => Effect.Effect<ManifestBuilder.ManifestBuildResult, ConfigLoaderError>

  /**
   * Watches a config file for changes and emits built manifests.
   */
  readonly watch: <E, R>(file: string, options?: {
    readonly onError?: (cause: Cause.Cause<ConfigLoaderError>) => Effect.Effect<void, E, R>
  }) => Stream.Stream<ManifestBuilder.ManifestBuildResult, ConfigLoaderError | E, R>
}>() {}

const make = Effect.gen(function*() {
  const path = yield* Path.Path
  const fs = yield* FileSystem.FileSystem
  const builder = yield* ManifestBuilder.ManifestBuilder

  const decodeDatasetConfig = Schema.decodeUnknown(Models.DatasetConfig)

  const jiti = yield* Effect.tryPromise({
    try: () =>
      import("jiti").then(({ createJiti }) =>
        createJiti(import.meta.url, {
          moduleCache: false,
          tryNative: false
        })
      ),
    catch: (cause) => new ConfigLoaderError({ cause })
  }).pipe(Effect.cached)

  const loadTypeScript = Effect.fnUntraced(function*(file: string) {
    return yield* Effect.tryMapPromise(jiti, {
      try: (jiti) =>
        jiti.import<(context: ModuleContext) => Models.DatasetConfig>(file, {
          default: true
        }),
      catch: identity
    }).pipe(
      Effect.map((callback) => callback(new ModuleContext(file))),
      Effect.flatMap(decodeDatasetConfig),
      Effect.mapError((cause) =>
        new ConfigLoaderError({
          cause,
          message: `Failed to load config file ${file}`
        })
      )
    )
  })

  const loadJavaScript = Effect.fnUntraced(function*(file: string) {
    return yield* Effect.tryPromise({
      try: () =>
        import(file).then(
          (module) => module.default as (context: ModuleContext) => Models.DatasetConfig
        ),
      catch: identity
    }).pipe(
      Effect.map((callback) => callback(new ModuleContext(file))),
      Effect.flatMap(decodeDatasetConfig),
      Effect.mapError((cause) =>
        new ConfigLoaderError({
          cause,
          message: `Failed to load config file ${file}`
        })
      )
    )
  })

  const loadJson = Effect.fnUntraced(function*(file: string) {
    return yield* Effect.tryMap(fs.readFileString(file), {
      try: (content) => JSON.parse(content),
      catch: identity
    }).pipe(
      Effect.flatMap(decodeDatasetConfig),
      Effect.mapError((cause) =>
        new ConfigLoaderError({
          cause,
          message: `Failed to load config file ${file}`
        })
      )
    )
  })

  const fileMatcher = Match.type<string>().pipe(
    Match.when(
      (_) => /\.(ts|mts|cts)$/.test(path.extname(_)),
      (_) => loadTypeScript(_)
    ),
    Match.when(
      (_) => /\.(js|mjs|cjs)$/.test(path.extname(_)),
      (_) => loadJavaScript(_)
    ),
    Match.when(
      (_) => /\.(json)$/.test(path.extname(_)),
      (_) => loadJson(_)
    ),
    Match.orElse((_) =>
      new ConfigLoaderError({
        message: `Unsupported file extension ${path.extname(_)}`
      })
    )
  )

  const load = Effect.fnUntraced(function*(file: string) {
    const resolved = path.resolve(file)
    return yield* fileMatcher(resolved)
  })

  const build = Effect.fnUntraced(function*(file: string) {
    const config = yield* load(file)
    return yield* builder.build(config).pipe(
      Effect.mapError(
        (cause) =>
          new ConfigLoaderError({
            cause,
            message: `Failed to build config file ${file}`
          })
      )
    )
  })

  const CANDIDATE_CONFIG_FILES = [
    "amp.config.ts",
    "amp.config.mts",
    "amp.config.cts",
    "amp.config.js",
    "amp.config.mjs",
    "amp.config.cjs",
    "amp.config.json"
  ]

  const find = Effect.fnUntraced(function*(cwd: string = ".") {
    const baseCwd = path.resolve(".")
    const resolvedCwd = path.resolve(cwd)
    if (resolvedCwd !== baseCwd && !resolvedCwd.startsWith(baseCwd + path.sep)) {
      return yield* new ConfigLoaderError({
        message: "Invalid directory path: directory traversal not allowed"
      })
    }
    const candidates = CANDIDATE_CONFIG_FILES.map((fileName) => {
      const filePath = path.resolve(cwd, fileName)
      return fs.exists(filePath).pipe(
        Effect.flatMap((exists) => exists ? Effect.succeed(filePath) : Effect.fail("not found"))
      )
    })
    return yield* Effect.firstSuccessOf(candidates).pipe(Effect.option)
  })

  const watch = <E, R>(file: string, options?: {
    readonly onError?: (
      cause: Cause.Cause<ConfigLoaderError>
    ) => Effect.Effect<void, E, R>
  }): Stream.Stream<
    ManifestBuilder.ManifestBuildResult,
    ConfigLoaderError | E,
    R
  > => {
    const baseCwd = path.resolve(".")
    const resolved = path.resolve(file)
    if (resolved !== baseCwd && !resolved.startsWith(baseCwd + path.sep)) {
      return Stream.fail(
        new ConfigLoaderError({
          message: "Invalid file path: directory traversal not allowed"
        })
      )
    }
    const open = load(resolved).pipe(
      Effect.tapErrorCause(options?.onError ?? (() => Effect.void)),
      Effect.either
    )

    const updates = fs.watch(resolved).pipe(
      Stream.buffer({ capacity: 1, strategy: "sliding" }),
      Stream.mapError(
        (cause) =>
          new ConfigLoaderError({
            cause,
            message: "Failed to watch config file"
          })
      ),
      Stream.filter(Predicate.isTagged("Update")),
      Stream.mapEffect(() => open)
    )

    const build = (config: Models.DatasetConfig) =>
      builder.build(config).pipe(
        Effect.mapError(
          (cause) =>
            new ConfigLoaderError({
              cause,
              message: `Failed to build config file ${file}`
            })
        ),
        Effect.tapErrorCause(options?.onError ?? (() => Effect.void)),
        Effect.either
      )

    return Stream.fromEffect(open).pipe(
      Stream.concat(updates),
      Stream.filterMap(Either.getRight),
      Stream.changesWith(DatasetConfigEquivalence),
      Stream.mapEffect(build),
      Stream.filterMap(Either.getRight),
      Stream.changesWith((a, b) =>
        DatasetDerivedEquivalence(a.manifest, b.manifest) &&
        DatasetMetadataEquivalence(a.metadata, b.metadata)
      )
    ) as Stream.Stream<
      ManifestBuilder.ManifestBuildResult,
      ConfigLoaderError | E,
      R
    >
  }

  return { load, find, watch, build }
})

export const layer = Layer.effect(ConfigLoader, make).pipe(
  Layer.provide(ManifestBuilder.layer)
)

const DatasetConfigEquivalence = Schema.equivalence(Models.DatasetConfig)
const DatasetDerivedEquivalence = Schema.equivalence(Models.DatasetDerived)
const DatasetMetadataEquivalence = Schema.equivalence(Models.DatasetMetadata)
