import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Redacted from "effect/Redacted"

type ResultFormat = "json" | "jsonl" | "pretty" | "table"
const ResultFormats: ReadonlyArray<ResultFormat> = ["json", "jsonl", "pretty", "table"]

// TODO(Chris): we should re-evaluate this format option
const format = Options.choice("format", ResultFormats).pipe(
  Options.withAlias("f"),
  Options.withDescription("The format to output the results in."),
  Options.withDefault("table")
)

const limit = Options.integer("limit").pipe(
  Options.withDescription("The number of rows to return from the query."),
  Options.optional
)

const query = Args.text({ name: "query" }).pipe(
  Args.withDescription("The SQL query to execute.")
)

const token = Options.redacted("token").pipe(
  Options.withAlias("t"),
  Options.withDescription("The bearer token to use for authentication."),
  Options.optional
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
})

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
