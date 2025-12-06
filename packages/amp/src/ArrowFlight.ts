import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, type Transport as ConnectTransport } from "@connectrpc/connect"
import { getMessageType, MessageHeaderType } from "@edgeandnode/arrow-flight-ipc/core/types"
import { decodeRecordBatch } from "@edgeandnode/arrow-flight-ipc/record-batch/decoder"
import { recordBatchToJson } from "@edgeandnode/arrow-flight-ipc/record-batch/json"
import { parseRecordBatch } from "@edgeandnode/arrow-flight-ipc/record-batch/parser"
import { parseSchema } from "@edgeandnode/arrow-flight-ipc/schema/parser"
import type { ArrowSchema } from "@edgeandnode/arrow-flight-ipc/schema/types"
import * as Console from "effect/Console"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { FlightDescriptor_DescriptorType, FlightDescriptorSchema, FlightService } from "./proto/Flight_pb.ts"
import { CommandStatementQuerySchema } from "./proto/FlightSql_pb.ts"

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

/**
 * Represents the possible errors that can occur when executing an Arrow Flight
 * query.
 */
export type ArrowFlightQueryError =
  | RpcError
  | NoEndpointsError
  | MultipleEndpointsError
  | TicketNotFoundError

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
 * A service which can be used to execute queries against an Arrow Flight API.
 */
export class ArrowFlight extends Context.Tag("@edgeandnode/amp/ArrowFlight")<ArrowFlight, {
  /**
   * The Connect `Client` that will be used to execute Arrow Flight queries.
   */
  readonly client: Client<typeof FlightService>

  readonly query: (query: string) => Effect.Effect<any>
}>() {}

const make = Effect.gen(function*() {
  const transport = yield* Transport
  const client = createClient(FlightService, transport)

  /**
   * Execute a SQL query and return a stream of rows.
   */
  const request = Effect.fn("ArrowFlight.request")(function*(query: string) {
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
        const messageType = yield* getMessageType(flightData)

        switch (messageType) {
          case MessageHeaderType.SCHEMA: {
            schema = yield* parseSchema(flightData)
            break
          }
          case MessageHeaderType.RECORD_BATCH: {
            const recordBatch = yield* parseRecordBatch(flightData)
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
    /**
     * Execute a SQL query and return a stream of rows.
     */
    query: request
  } as const
})

/**
 * A layer which constructs a concrete implementation of an `ArrowFlight`
 * service and depends upon some implementation of a `Transport`.
 */
export const layer: Layer.Layer<ArrowFlight, ArrowFlightQueryError, Transport> = Layer.effect(ArrowFlight, make)
