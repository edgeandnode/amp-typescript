import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Console from "effect/Console"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Redacted from "effect/Redacted"
import * as Runtime from "effect/Runtime"
import * as Argument from "effect/unstable/cli/Argument"
import * as Command from "effect/unstable/cli/Command"
import * as Flag from "effect/unstable/cli/Flag"

type ResultFormat = "json" | "jsonl" | "pretty" | "table"
const ResultFormats: ReadonlyArray<ResultFormat> = ["json", "jsonl", "pretty", "table"]

export class QueryCommandError extends Data.TaggedError("QueryCommandError")<{
  readonly cause: ArrowFlight.ArrowFlightError
}> {
  override readonly [Runtime.errorExitCode] = 1
  override readonly [Runtime.errorReported] = false
}

// TODO(Chris): we should re-evaluate this format option
const format = Flag.choice("format", ResultFormats).pipe(
  Flag.withAlias("f"),
  Flag.withDescription("The format to output the results in."),
  Flag.withDefault("table")
)

const limit = Flag.integer("limit").pipe(
  Flag.withDescription("The number of rows to return from the query."),
  Flag.optional
)

const query = Argument.string("query").pipe(
  Argument.withDescription("The SQL query to execute.")
)

const token = Flag.redacted("token").pipe(
  Flag.withAlias("t"),
  Flag.withDescription("The bearer token to use for authentication."),
  Flag.optional
)

const queryCommandHandler = Effect.fnUntraced(function*(params: {
  readonly format: ResultFormat
  readonly limit: Option.Option<number>
  readonly query: string
  readonly token: Option.Option<Redacted.Redacted<string>>
}) {
  const flight = yield* ArrowFlight.ArrowFlight

  const query = Option.match(params.limit, {
    onNone: () => params.query,
    onSome: (limit) => `${params.query} LIMIT ${limit}`
  })

  const results = yield* flight.query(query)

  const data = results
    .filter(({ data }) => data.length > 0)
    .flatMap(({ data }) => data)

  switch (params.format) {
    case "json": {
      return yield* Console.log(JSON.stringify(data, null, 2))
    }
    case "jsonl": {
      return yield* Console.log(JSON.stringify(data))
    }
    case "pretty": {
      return yield* Console.log(data)
    }
    case "table": {
      return yield* Console.table(data)
    }
  }
}, Effect.mapError((cause) => new QueryCommandError({ cause })))

export const QueryCommand = Command.make("query", { format, limit, query, token }).pipe(
  Command.withDescription("Execute a SQL query with Amp"),
  Command.withHandler(queryCommandHandler),
  Command.provide(({ token }) => {
    const layerInterceptorAuth = Option
      .match(token, {
        onSome: (token) => ArrowFlight.layerInterceptorToken(token),
        onNone: () => ArrowFlight.layerInterceptorBearerAuth
      })
    return ArrowFlight.layer.pipe(
      Layer.provide(NodeArrowFlight.layerTransportGrpc({
        baseUrl: "http://localhost:1602"
      })),
      Layer.provide(layerInterceptorAuth)
    )
  })
)
