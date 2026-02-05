import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as AdminApi from "../admin/service.ts"
import * as Models from "../models.ts"

export const ManifestBuildResult = Schema.Struct({
  metadata: Models.DatasetMetadata,
  manifest: Models.DatasetDerived
})
export type ManifestBuildResult = typeof ManifestBuildResult.Type

export class ManifestBuilderError extends Data.TaggedError("ManifestBuilderError")<{
  readonly cause: unknown
  readonly message: string
  readonly table: string
}> {}

export class ManifestBuilder extends Context.Tag("Amp/ManifestBuilder")<ManifestBuilder, {
  readonly build: (config: Models.DatasetConfig) => Effect.Effect<ManifestBuildResult, ManifestBuilderError>
}>() {}

const make = Effect.gen(function*() {
  const admin = yield* AdminApi.AdminApi

  const build = Effect.fn("ManifestBuilder.build")(
    function*(config: Models.DatasetConfig) {
      // Extract metadata
      const metadata = Models.DatasetMetadata.make({
        namespace: config.namespace ?? Models.DatasetNamespace.make("_"),
        name: config.name,
        readme: config.readme,
        repository: config.repository,
        description: config.description,
        keywords: config.keywords,
        license: config.license,
        visibility: config.private ? "private" : "public",
        sources: config.sources
      })

      // Build manifest tables - send all tables in one request
      const tables = yield* Effect.gen(function*() {
        const configTables = config.tables ?? {}
        const configFunctions = config.functions ?? {}

        // Build function definitions map from config
        const functionsMap: Record<string, Models.FunctionDefinition> = {}
        for (const [name, func] of Object.entries(configFunctions)) {
          functionsMap[name] = Models.FunctionDefinition.make({
            source: func.source,
            inputTypes: func.inputTypes,
            outputType: func.outputType
          })
        }

        // If no tables and no functions, skip schema request entirely
        if (Object.keys(configTables).length === 0 && Object.keys(functionsMap).length === 0) {
          return []
        }

        // If no tables but we have functions, still skip schema request
        // (when functions-only validation happens server-side, returns empty schema)
        if (Object.keys(configTables).length === 0) {
          return []
        }

        // Prepare all table SQL queries
        const tableSqlMap: Record<string, string> = {}
        for (const [name, table] of Object.entries(configTables)) {
          tableSqlMap[name] = table.sql
        }

        // Call schema endpoint with all tables and functions at once
        const response = yield* admin.getOutputSchema({
          tables: tableSqlMap,
          dependencies: config.dependencies,
          functions: Object.keys(functionsMap).length > 0 ? functionsMap : undefined
        }).pipe(Effect.catchAll((cause) =>
          new ManifestBuilderError({
            cause,
            message: "Failed to get schemas",
            table: "(all tables)"
          })
        ))

        // Process each table's schema
        const tables: Array<[name: string, table: Models.Table]> = []
        for (const [name, table] of Object.entries(configTables)) {
          const tableSchema = response.schemas[name]

          if (Predicate.isUndefined(tableSchema)) {
            return yield* new ManifestBuilderError({
              message: `No schema returned for table ${name}`,
              table: name,
              cause: undefined
            })
          }

          if (tableSchema.networks.length !== 1) {
            return yield* new ManifestBuilderError({
              cause: undefined,
              message: `Expected 1 network for SQL query, got ${tableSchema.networks}`,
              table: name
            })
          }

          const network = Models.Network.make(tableSchema.networks[0])
          const input = Models.TableInput.make({ sql: table.sql })
          const output = Models.Table.make({ input, schema: tableSchema.schema, network })

          tables.push([name, output])
        }

        return tables
      })

      // Build manifest functions
      const functions: Array<[name: string, manifest: Models.FunctionManifest]> = []
      for (const [name, func] of Object.entries(config.functions ?? {})) {
        const { inputTypes, outputType, source } = func
        const manifest = Models.FunctionManifest.make({ name, source, inputTypes, outputType })

        functions.push([name, manifest])
      }

      const manifest = Models.DatasetDerived.make({
        kind: "manifest",
        startBlock: config.startBlock,
        dependencies: config.dependencies,
        tables: Object.fromEntries(tables),
        functions: Object.fromEntries(functions)
      })

      return ManifestBuildResult.make({
        metadata,
        manifest
      })
    }
  )

  return {
    build
  } as const
})

/**
 * Layer for creating a `ManifestBuilder`.
 */
export const layer: Layer.Layer<
  ManifestBuilder,
  never,
  AdminApi.AdminApi
> = Layer.effect(ManifestBuilder, make)
