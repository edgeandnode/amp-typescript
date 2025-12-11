import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, type Transport as ConnectTransport } from "@connectrpc/connect"
import * as Console from "effect/Console"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { decodeRecordBatch } from "./internal/arrow-flight-ipc/Decoder.ts"
import { recordBatchToJson } from "./internal/arrow-flight-ipc/Json.ts"
import { parseRecordBatch } from "./internal/arrow-flight-ipc/RecordBatch.ts"
import { type ArrowSchema, getMessageType, MessageHeaderType, parseSchema } from "./internal/arrow-flight-ipc/Schema.ts"
import { FlightDescriptor_DescriptorType, FlightDescriptorSchema, FlightService } from "./Protobuf/Flight_pb.ts"
import { CommandStatementQuerySchema } from "./Protobuf/FlightSql_pb.ts"

// =============================================================================
// Errors
// =============================================================================

/**
 * Represents the possible errors that can occur when executing an Arrow Flight
 * query.
 */
export type ArrowFlightQueryError =
  | RpcError
  | NoEndpointsError
  | MultipleEndpointsError
  | TicketNotFoundError
  | ParseRecordBatchError
  | ParseSchemaError

/**
 * Represents an Arrow Flight RPC request that failed.
 */
export class RpcError extends Schema.TaggedError<RpcError>(
  "@edgeandnode/amp/RpcError"
)("RpcError", {
  method: Schema.String,
  /**
   * The underlying reason for the failed RPC request.
   */
  cause: Schema.Defect
}) {}

/**
 * Represents an error that occurred as a result of a `FlightInfo` request
 * returning an empty list of endpoints from which data can be acquired.
 */
export class NoEndpointsError extends Schema.TaggedError<NoEndpointsError>(
  "@edgeandnode/amp/NoEndpointsError"
)("NoEndpointsError", {
  /**
   * The SQL query that was requested.
   */
  query: Schema.String
}) {}

// TODO: determine if this is _really_ a logical error case
/**
 * Represents an error that occured as a result of a `FlightInfo` request
 * returning multiple endpoints from which data can be acquired.
 *
 * For Amp queries, there should only ever be **one** authoritative source
 * of data.
 */
export class MultipleEndpointsError extends Schema.TaggedError<MultipleEndpointsError>(
  "@edgeandnode/amp/MultipleEndpointsError"
)("MultipleEndpointsError", {
  /**
   * The SQL query that was requested.
   */
  query: Schema.String
}) {}

/**
 * Represents an error that occurred as a result of a `FlightInfo` request
 * whose endpoint did not have a ticket.
 */
export class TicketNotFoundError extends Schema.TaggedError<TicketNotFoundError>(
  "@edgeandnode/amp/TicketNotFoundError"
)("TicketNotFoundError", {
  /**
   * The SQL query that was requested.
   */
  query: Schema.String
}) {}

/**
 * Represents an error that occurred as a result of failing to parse an Apache
 * Arrow RecordBatch.
 */
export class ParseRecordBatchError extends Schema.TaggedError<ParseRecordBatchError>(
  "Amp/ParseRecordBatchError"
)("ParseRecordBatchError", {
  /**
   * The underlying reason for the failure to parse a record batch.
   */
  cause: Schema.Defect
}) {}

/**
 * Represents an error that occurred as a result of failing to parse an Apache
 * Arrow Schema.
 */
export class ParseSchemaError extends Schema.TaggedError<ParseSchemaError>(
  "Amp/ParseSchemaError"
)("ParseSchemaError", {
  /**
   * The underlying reason for the failure to parse a schema.
   */
  cause: Schema.Defect
}) {}

// =============================================================================
// Arrow Flight Service
// =============================================================================

// TODO: cleanup service interface (just implemented as is for testing right now)
/**
 * A service which can be used to execute queries against an Arrow Flight API.
 */
export class ArrowFlight extends Context.Tag("@edgeandnode/amp/ArrowFlight")<ArrowFlight, {
  /**
   * The Connect `Client` that will be used to execute Arrow Flight queries.
   */
  readonly client: Client<typeof FlightService>

  readonly query: (query: string) => Effect.Effect<unknown, ArrowFlightQueryError>
}>() {}

const make = Effect.gen(function*() {
  const transport = yield* Transport
  const client = createClient(FlightService, transport)

  /**
   * Execute a SQL query and return a stream of rows.
   */
  const query = Effect.fn("ArrowFlight.request")(function*(query: string) {
    const cmd = create(CommandStatementQuerySchema, { query })
    const any = anyPack(CommandStatementQuerySchema, cmd)
    const desc = create(FlightDescriptorSchema, {
      type: FlightDescriptor_DescriptorType.CMD,
      cmd: toBinary(AnySchema, any)
    })

    const flightInfo = yield* Effect.tryPromise({
      try: (signal) => client.getFlightInfo(desc, { signal }),
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
        client.doGet(ticket, { signal: controller.signal }),
        (cause) => new RpcError({ cause, method: "doGet" })
      )
    }))

    let schema: ArrowSchema | undefined

    // Convert FlightData stream to a stream of rows
    return yield* flightDataStream.pipe(
      Stream.runForEach(Effect.fnUntraced(function*(flightData) {
        const messageType = yield* Effect.orDie(getMessageType(flightData))

        switch (messageType) {
          case MessageHeaderType.SCHEMA: {
            schema = yield* parseSchema(flightData).pipe(
              Effect.mapError((cause) => new ParseSchemaError({ cause }))
            )
            break
          }
          case MessageHeaderType.RECORD_BATCH: {
            const recordBatch = yield* parseRecordBatch(flightData).pipe(
              Effect.mapError((cause) => new ParseRecordBatchError({ cause }))
            )
            const decodedRecordBatch = decodeRecordBatch(recordBatch, flightData.dataBody, schema!)
            const json = recordBatchToJson(decodedRecordBatch)
            yield* Console.dir(json, { depth: null, colors: true })
            break
          }
        }

        return yield* Effect.void
      }))
    )
  })

  return {
    client,
    query
  } as const
})

/**
 * A layer which constructs a concrete implementation of an `ArrowFlight`
 * service and depends upon some implementation of a `Transport`.
 */
export const layer: Layer.Layer<ArrowFlight, ArrowFlightQueryError, Transport> = Layer.effect(ArrowFlight, make)

// =============================================================================
// Transport Service
// =============================================================================

/**
 * A service which abstracts the underlying transport for a given client.
 *
 * A transport implements a protocol, such as Connect or gRPC-web, and allows
 * for the concrete clients to be independent of the protocol.
 */
export class Transport extends Context.Tag("@edgeandnode/amp/Transport")<
  Transport,
  ConnectTransport
>() {}
