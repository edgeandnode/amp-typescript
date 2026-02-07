import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, createContextValues } from "@connectrpc/connect"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import { identity } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { Auth } from "../auth/service.ts"
import type { AuthInfo, BlockRange } from "../core/domain.ts"
import { RecordBatchMetadataFromUint8Array } from "../core/domain.ts"
import { decodeRecordBatch, DictionaryRegistry } from "../internal/arrow-flight-ipc/Decoder.ts"
import { recordBatchToJson } from "../internal/arrow-flight-ipc/Json.ts"
import { parseRecordBatch } from "../internal/arrow-flight-ipc/RecordBatch.ts"
import {
  type ArrowSchema,
  getMessageType,
  MessageHeaderType,
  parseSchema
} from "../internal/arrow-flight-ipc/Schema.ts"
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
import type { ExtractQueryResult, QueryOptions, QueryResult } from "./types.ts"

// =============================================================================
// Arrow Flight Service
// =============================================================================

// TODO: cleanup service interface (just implemented as is for testing right now)
/**
 * A service which can be used to execute queries against an Arrow Flight API.
 */
export class ArrowFlight extends Context.Tag("Amp/ArrowFlight")<ArrowFlight, {
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
  ) => Effect.Effect<ReadonlyArray<ExtractQueryResult<Options>>, ArrowFlightError>

  /**
   * Executes an Arrow Flight SQL query and returns a stream of results.
   */
  readonly streamQuery: <Options extends QueryOptions>(
    sql: string,
    options?: Options
  ) => Stream.Stream<ExtractQueryResult<Options>, ArrowFlightError>
}>() {}

const make = Effect.gen(function*() {
  const auth = yield* Effect.serviceOption(Auth)
  const transport = yield* Transport
  const client = createClient(FlightService, transport)

  const decodeRecordBatchMetadata = Schema.decode(RecordBatchMetadataFromUint8Array)

  /**
   * Execute a SQL query and return a stream of rows.
   */
  const streamQuery = (query: string, options?: QueryOptions) =>
    Effect.gen(function*() {
      const contextValues = createContextValues()
      const authInfo = Option.isSome(auth)
        ? yield* auth.value.getCachedAuthInfo
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
        headers.set("amp-resume", blockRangesToResumeWatermark(options.resumeWatermark))
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

      const flightDataStream = Stream.unwrapScoped(Effect.gen(function*() {
        const controller = yield* Effect.acquireRelease(
          Effect.sync(() => new AbortController()),
          (controller) => Effect.sync(() => controller.abort())
        )
        return Stream.fromAsyncIterable(
          client.doGet(ticket, { signal: controller.signal, contextValues }),
          (cause) => new RpcError({ cause, method: "doGet" })
        )
      }))

      let schema: ArrowSchema | undefined
      const dictionaryRegistry = new DictionaryRegistry()
      const dataSchema: Schema.Array$<
        Schema.Record$<
          typeof Schema.String,
          typeof Schema.Unknown
        >
      > = Schema.Array(
        options?.schema ?? Schema.Record({
          key: Schema.String,
          value: Schema.Unknown
        }) as any
      )
      const decodeRecordBatchData = Schema.decode(dataSchema)

      // Convert FlightData stream to a stream of rows
      return flightDataStream.pipe(
        Stream.mapEffect(Effect.fnUntraced(function*(flightData): Effect.fn.Return<
          Option.Option<QueryResult<any>>,
          ArrowFlightError
        > {
          const messageType = yield* Effect.orDie(getMessageType(flightData))

          switch (messageType) {
            case MessageHeaderType.SCHEMA: {
              schema = yield* parseSchema(flightData).pipe(
                Effect.mapError((cause) => new ParseSchemaError({ cause }))
              )
              return Option.none<QueryResult<any>>()
            }
            case MessageHeaderType.DICTIONARY_BATCH: {
              // TODO: figure out what to do (if anything) with dictionary batches
              // const dictionaryBatch = yield* parseDictionaryBatch(flightData).pipe(
              //   Effect.mapError((cause) => new ParseDictionaryBatchError({ cause }))
              // )
              // decodeDictionaryBatch(dictionaryBatch, flightData.dataBody, schema!, dictionaryRegistry, readColumnValues)
              return Option.none<QueryResult<any>>()
            }
            case MessageHeaderType.RECORD_BATCH: {
              const metadata = yield* decodeRecordBatchMetadata(flightData.appMetadata).pipe(
                Effect.mapError((cause) => new ParseRecordBatchError({ cause }))
              )
              const recordBatch = yield* parseRecordBatch(flightData).pipe(
                Effect.mapError((cause) => new ParseRecordBatchError({ cause }))
              )
              const decodedRecordBatch = decodeRecordBatch(recordBatch, flightData.dataBody, schema!)
              const json = recordBatchToJson(decodedRecordBatch, { dictionaryRegistry })
              const data = yield* decodeRecordBatchData(json).pipe(
                Effect.mapError((cause) => new ParseRecordBatchError({ cause }))
              )
              return Option.some({ data, metadata })
            }
          }

          return yield* Effect.die(new Cause.RuntimeException(`Invalid message type received: ${messageType}`))
        })),
        Stream.filterMap(identity)
      )
    }).pipe(
      Stream.unwrap,
      Stream.withSpan("ArrowFlight.stream")
    ) as any

  const query = Effect.fn("ArrowFlight.query")(
    function*(query: string, options?: QueryOptions) {
      const chunk = yield* Stream.runCollect(streamQuery(query, options))
      return Array.from(chunk)
    }
  ) as any

  return {
    client,
    query,
    streamQuery
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
const blockRangesToResumeWatermark = (ranges: ReadonlyArray<BlockRange>): string => {
  const watermarks: Record<string, { number: number; hash: string }> = {}
  for (const range of ranges) {
    watermarks[range.network] = {
      number: range.numbers.end,
      hash: range.hash
    }
  }
  return JSON.stringify(watermarks)
}
