import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, createContextValues } from "@connectrpc/connect"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Filter from "effect/Filter"
import * as Fn from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Auth } from "../auth/service.ts"
import type { AuthInfo, BlockRange } from "../core/domain.ts"
import { RecordBatchMetadataFromUint8Array } from "../core/domain.ts"
import { decodeRecordBatch, DictionaryRegistry } from "../internal/arrow-flight-ipc/Decoder.ts"
import { recordBatchToJson, type RecordBatchToJsonOptions } from "../internal/arrow-flight-ipc/Json.ts"
import { parseRecordBatch } from "../internal/arrow-flight-ipc/RecordBatch.ts"
import {
  type ArrowSchema,
  getMessageType,
  MessageHeaderType,
  parseSchema
} from "../internal/arrow-flight-ipc/Schema.ts"
import { ExplainResultRow, parsePlan, planToTable, prefixExplain } from "../internal/explain/parser.ts"
import { FlightDescriptor_DescriptorType, FlightDescriptorSchema, FlightService } from "../protobuf/Flight_pb.ts"
import { CommandStatementQuerySchema } from "../protobuf/FlightSql_pb.ts"
import {
  type ArrowFlightError,
  MultipleEndpointsError,
  NoEndpointsError,
  ParseRecordBatchError,
  ParseSchemaError,
  RpcError,
  TicketNotFoundError
} from "./errors.ts"
import { AuthInfoContextKey, Transport } from "./transport.ts"
import type { ExplainResult, ExtractQueryResult, QueryOptions } from "./types.ts"

// =============================================================================
// Arrow Flight Service
// =============================================================================

// TODO: cleanup service interface (just implemented as is for testing right now)
/**
 * A service which can be used to execute queries against an Arrow Flight API.
 */
export class ArrowFlight extends Context.Service<
  ArrowFlight,
  {
    /**
     * The Connect `Client` that will be used to execute Arrow Flight queries.
     */
    readonly client: Client<typeof FlightService>

    /**
     * Executes an Arrow Flight SQL query and returns a all results as an array.
     */
    readonly query: <Options extends QueryOptions>(
      sql: string,
      options?: Options
    ) => Effect.Effect<
      ReadonlyArray<ExtractQueryResult<Options>>,
      ArrowFlightError
    >

    /**
     * Executes an Arrow Flight SQL query and returns a stream of results.
     */
    readonly streamQuery: <Options extends QueryOptions>(
      sql: string,
      options?: Options
    ) => Stream.Stream<ExtractQueryResult<Options>, ArrowFlightError>

    /**
     * Executes the given SQL with `EXPLAIN` (or `EXPLAIN ANALYZE` when
     * `analyze` is true) prepended, parses the returned plan text, and
     * returns a normalized table of plan nodes — one row per node, with
     * numeric properties and metrics expanded into columns. Durations are
     * converted to seconds and their column names suffixed with `_secs`.
     */
    readonly explain: (
      sql: string,
      options?: { readonly analyze?: boolean | undefined }
    ) => Effect.Effect<ExplainResult, ArrowFlightError>
  }
>()("Amp/ArrowFlight") {}

const make = Effect.gen(function*() {
  const auth = yield* Effect.serviceOption(Auth)
  const transport = yield* Transport
  const client = createClient(FlightService, transport)

  const decodeRecordBatchMetadata = Schema.decodeEffect(
    RecordBatchMetadataFromUint8Array
  )

  /**
   * Execute a SQL query and return a stream of rows.
   */
  const streamQuery = <Options extends QueryOptions>(
    query: string,
    options?: Options
  ): Stream.Stream<ExtractQueryResult<Options>, ArrowFlightError> =>
    Effect.gen(function*() {
      const contextValues = createContextValues()
      // A cache failure ("can't read the saved token") is not an Arrow Flight
      // error and shouldn't surface as one — fall back to "no cached auth"
      // and let the server's auth response (if any) be the source of truth.
      const authInfo = Option.isSome(auth)
        ? yield* auth.value.getCachedAuthInfo.pipe(
          Effect.orElseSucceed(() => Option.none<AuthInfo>())
        )
        : Option.none<AuthInfo>()

      // Setup the query context with authentication information, if available
      if (Option.isSome(authInfo)) {
        contextValues.set(AuthInfoContextKey, authInfo.value)
      }

      const cmd = create(CommandStatementQuerySchema, { query })
      const any = anyPack(CommandStatementQuerySchema, cmd)
      const desc = create(FlightDescriptorSchema, {
        type: FlightDescriptor_DescriptorType.CMD,
        cmd: toBinary(AnySchema, any)
      })

      // Setup the query headers
      const headers = new Headers()
      if (Predicate.isNotUndefined(options?.stream)) {
        headers.set("amp-stream", "true")
      }
      if (Predicate.isNotUndefined(options?.resumeWatermark)) {
        headers.set(
          "amp-resume",
          blockRangesToResumeWatermark(options.resumeWatermark)
        )
      }

      const flightInfo = yield* Effect.tryPromise({
        try: (signal) => client.getFlightInfo(desc, { contextValues, headers, signal }),
        catch: (cause) => new RpcError({ cause, method: "getFlightInfo" })
      })

      if (flightInfo.endpoint.length !== 1) {
        return yield* flightInfo.endpoint.length <= 0
          ? new NoEndpointsError({ query })
          : new MultipleEndpointsError({ query })
      }

      const { ticket } = flightInfo.endpoint[0]!

      if (ticket === undefined) {
        return yield* new TicketNotFoundError({ query })
      }

      const flightDataStream = Stream.unwrap(
        Effect.gen(function*() {
          const controller = yield* Effect.acquireRelease(
            Effect.sync(() => new AbortController()),
            (abort) => Effect.sync(() => abort.abort())
          )
          return Stream.fromAsyncIterable(
            client.doGet(ticket, { signal: controller.signal, contextValues }),
            (cause) => new RpcError({ cause, method: "doGet" })
          )
        })
      )

      let schema: ArrowSchema | undefined
      const dictionaryRegistry = new DictionaryRegistry()
      const dataSchema = Schema.Array(
        options?.schema ?? Schema.Record(Schema.String, Schema.Unknown)
      )

      const decodeRecordBatchData = Schema.decodeEffect(dataSchema)

      type Result = ExtractQueryResult<Options>
      // Convert FlightData stream to a stream of rows
      return flightDataStream.pipe(
        Stream.mapEffect(
          Effect.fnUntraced(function*(flightData): Effect.fn.Return<
            Option.Option<Result>,
            ArrowFlightError
          > {
            const messageType = yield* Effect.orDie(getMessageType(flightData))

            switch (messageType) {
              case MessageHeaderType.SCHEMA: {
                schema = yield* parseSchema(flightData).pipe(
                  Effect.mapError((cause) => new ParseSchemaError({ cause }))
                )
                return Option.none<Result>()
              }
              case MessageHeaderType.DICTIONARY_BATCH: {
                // TODO: figure out what to do (if anything) with dictionary batches
                // const dictionaryBatch = yield* parseDictionaryBatch(flightData).pipe(
                //   Effect.mapError((cause) => new ParseDictionaryBatchError({ cause }))
                // )
                // decodeDictionaryBatch(dictionaryBatch, flightData.dataBody, schema!, dictionaryRegistry, readColumnValues)
                return Option.none<Result>()
              }
              case MessageHeaderType.RECORD_BATCH: {
                const metadata = yield* decodeRecordBatchMetadata(
                  flightData.appMetadata
                ).pipe(
                  Effect.mapError(
                    (cause) => new ParseRecordBatchError({ cause })
                  )
                )
                const recordBatch = yield* parseRecordBatch(flightData).pipe(
                  Effect.mapError(
                    (cause) => new ParseRecordBatchError({ cause })
                  )
                )
                const decodedRecordBatch = decodeRecordBatch(
                  recordBatch,
                  flightData.dataBody,
                  schema!
                )
                const jsonOptions: RecordBatchToJsonOptions = {
                  dictionaryRegistry
                }
                if (options?.bigIntHandling !== undefined) {
                  jsonOptions.bigIntHandling = options.bigIntHandling
                }
                if (options?.binaryHandling !== undefined) {
                  jsonOptions.binaryHandling = options.binaryHandling
                }
                if (options?.dateHandling !== undefined) {
                  jsonOptions.dateHandling = options.dateHandling
                }
                if (options?.includeNulls !== undefined) {
                  jsonOptions.includeNulls = options.includeNulls
                }

                const json = recordBatchToJson(decodedRecordBatch, jsonOptions)
                const data = yield* decodeRecordBatchData(json).pipe(
                  Effect.mapError(
                    (cause) => new ParseRecordBatchError({ cause })
                  )
                )
                return Option.some({ data, metadata } as Result)
              }
            }

            return yield* Effect.die(
              new Cause.IllegalArgumentError(
                `Invalid message type received: ${messageType}`
              )
            )
          })
        ),
        Stream.filterMap(Filter.fromPredicateOption(Fn.identity))
      )
    }).pipe(Stream.unwrap, Stream.withSpan("ArrowFlight.stream"))

  const query = <Options extends QueryOptions>(
    sql: string,
    options?: Options
  ): Effect.Effect<
    ReadonlyArray<ExtractQueryResult<Options>>,
    ArrowFlightError
  > =>
    Stream.runCollect(streamQuery(sql, options)).pipe(
      Effect.map((chunk) => Array.from(chunk)),
      Effect.withSpan("ArrowFlight.query")
    )

  const explain = Effect.fn("ArrowFlight.explain")(function*(
    sql: string,
    options?: { readonly analyze?: boolean | undefined }
  ) {
    const prepared = prefixExplain(sql, options?.analyze ?? false)
    const head = yield* streamQuery(prepared, {
      schema: ExplainResultRow
    }).pipe(Stream.runHead)
    const planText = head.pipe(
      Option.flatMap((result) => Option.fromNullishOr(result.data[0])),
      Option.map((row) => row.plan),
      Option.getOrElse(() => "")
    )
    return planToTable(parsePlan(planText))
  })

  return {
    client,
    query,
    streamQuery,
    explain
  } as const
})

/**
 * A layer which constructs a concrete implementation of an `ArrowFlight`
 * service and depends upon some implementation of a `Transport`.
 */
export const layer: Layer.Layer<ArrowFlight, ArrowFlightError, Transport> = Layer.effect(ArrowFlight, make)

// =============================================================================
// Internal Utilities
// =============================================================================

/**
 * Converts a list of block ranges into a resume watermark string.
 *
 * @param ranges - The block ranges to convert.
 * @returns A resume watermark string.
 */
const blockRangesToResumeWatermark = (
  ranges: ReadonlyArray<BlockRange>
): string => {
  const watermarks: Record<string, { number: number; hash: string }> = {}
  for (const range of ranges) {
    watermarks[range.network] = {
      number: range.numbers.end,
      hash: range.hash
    }
  }
  return JSON.stringify(watermarks)
}
