import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Redacted from "effect/Redacted"
import * as Runtime from "effect/Runtime"
import * as Argument from "effect/cli/Argument"
import * as Command from "effect/cli/Command"
import * as Flag from "effect/cli/Flag"

type ResultFormat = "json" | "jsonl" | "pretty" | "table"
const ResultFormats: ReadonlyArray<ResultFormat> = ["json", "jsonl", "pretty", "table"]

export class QueryCommandError extends Data.TaggedError("QueryCommandError")<{
  readonly cause: ArrowFlight.ArrowFlightError
}> {
  override readonly [Runtime.errorExitCode] = 1
  override readonly [Runtime.errorReported] = false
}

// TODO(Chris): we should re-evaluate this format option
const formatFlag = Flag.Literals("format", ResultFormats).pipe(
  Flag.withAlias("f"),
  Flag.withDescription("The format to output the results in."),
  Flag.withDefault("table")
)

const limitFlag = Flag.Int("limit").pipe(
  Flag.withDescription("The number of rows to return from the query."),
  Flag.optional
)

const queryArg = Argument.String("query").pipe(Argument.withDescription("The SQL query to execute."))

const tokenFlag = Flag.Redacted("token").pipe(
  Flag.withAlias("t"),
  Flag.withDescription("The bearer token to use for authentication. Defaults to $AMP_AUTH_TOKEN."),
  Flag.withFallbackConfig(Config.Redacted("AMP_AUTH_TOKEN")),
  Flag.optional
)

const urlFlag = Flag.String("url").pipe(
  Flag.withAlias("u"),
  Flag.withDescription(
    "The Arrow Flight URL of the Amp server, e.g. grpc+tls://gateway.amp.staging.edgeandnode.com:443. Defaults to $AMP_QUERY_URL."
  ),
  Flag.withFallbackConfig(Config.String("AMP_QUERY_URL")),
  Flag.withDefault("http://localhost:1602")
)

// The gRPC transport expects an HTTP(S) base URL, but Amp and its other
// clients spell Flight endpoints as grpc:// and grpc+tls://
const toTransportUrl = (url: string): string =>
  url.replace(/^grpc\+tls:\/\//, "https://").replace(/^grpc:\/\//, "http://")

const queryCommandHandler = Effect.fnUntraced(
  function* (params: {
    readonly format: ResultFormat
    readonly limit: Option.Option<number>
    readonly query: string
    readonly token: Option.Option<Redacted.Redacted<string>>
    readonly url: string
  }) {
    const flight = yield* ArrowFlight.ArrowFlight

    const sql = Option.match(params.limit, {
      onNone: () => params.query,
      onSome: (limit) => `${params.query} LIMIT ${limit}`
    })

    const results = yield* flight.query(sql)

    const rows = results.filter(({ data }) => data.length > 0).flatMap(({ data }) => data)

    switch (params.format) {
      case "json": {
        return yield* Console.log(JSON.stringify(rows, null, 2))
      }
      case "jsonl": {
        return yield* Console.log(JSON.stringify(rows))
      }
      case "pretty": {
        return yield* Console.log(rows)
      }
      case "table": {
        return yield* Console.table(rows)
      }
    }
  },
  // Errors are not reported by the runtime, so print the reason here
  Effect.tapError((cause) =>
    Console.error(
      `Query failed: ${"cause" in cause && cause.cause instanceof Error ? cause.cause.message : cause.message || cause._tag}`
    )
  ),
  Effect.mapError((cause) => new QueryCommandError({ cause }))
)

export const QueryCommand = Command.make("query", {
  format: formatFlag,
  limit: limitFlag,
  query: queryArg,
  token: tokenFlag,
  url: urlFlag
}).pipe(
  Command.withDescription("Execute a SQL query with Amp"),
  Command.withHandler(queryCommandHandler),
  Command.provide(({ token, url }) => {
    const layerInterceptorAuth = Option.match(token, {
      onSome: (bearer) => ArrowFlight.layerInterceptorToken(bearer),
      onNone: () => ArrowFlight.layerInterceptorBearerAuth
    })
    return ArrowFlight.layer.pipe(
      Layer.provide(
        NodeArrowFlight.layerTransportGrpc({
          baseUrl: toTransportUrl(url)
        })
      ),
      Layer.provide(layerInterceptorAuth)
    )
  })
)
