import { RecordBatchReader, tableFromIPC } from 'apache-arrow'
import { create, toBinary } from "@bufbuild/protobuf"
import { anyPack, AnySchema } from "@bufbuild/protobuf/wkt"
import { type Client, createClient, type Transport as ConnectTransport } from "@connectrpc/connect"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import {
  type FlightData,
  FlightDescriptor_DescriptorType,
  FlightDescriptorSchema,
  FlightService
} from "./proto/Flight_pb.ts"
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
}>() {}

const make = Effect.gen(function*() {
  const transport = yield* Transport
  const client = createClient(FlightService, transport)

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
    })).pipe(Stream.map(flightDataToIPC))

    yield* flightDataStream.pipe(
      Stream.runForEach((table) =>
        Effect.sync(() => {
          console.dir(table, { depth: null, colors: true })
        })
      )
    )
  })

  // Test code
  yield* Effect.orDie(request("SELECT * FROM intTable"))

  return {
    client
  } as const
})

/**
 * A layer which constructs a concrete implementation of an `ArrowFlight`
 * service and depends upon some implementation of a `Transport`.
 */
export const layer: Layer.Layer<ArrowFlight, never, Transport> = Layer.effect(ArrowFlight, make)

/**
 * Converts a `FlightData` payload into Apache Arrow IPC format.
 */
const flightDataToIPC = (flightData: FlightData): Uint8Array => {
  // Arrow IPC Stream format requires:
  // 1. Continuation indicator (4 bytes): 0xFFFFFFFF
  // 2. Metadata length (4 bytes, little-endian)
  // 3. Metadata (padded to 8-byte boundary)
  // 4. Body data (already 8-byte aligned)

  const continuationToken = new Uint8Array([0xFF, 0xFF, 0xFF, 0xFF])

  // Get metadata length
  const metadataLength = flightData.dataHeader.length
  const metadataLengthBytes = new Uint8Array(4)
  new DataView(metadataLengthBytes.buffer).setUint32(0, metadataLength, true) // little-endian

  // Calculate padding needed to align to 8 bytes
  const paddingSize = (8 - ((8 + metadataLength) % 8)) % 8
  const padding = new Uint8Array(paddingSize)

  // Combine all parts
  const totalLength = continuationToken.length + // 4 bytes
    metadataLengthBytes.length + // 4 bytes
    metadataLength + // variable
    paddingSize + // 0-7 bytes
    flightData.dataBody.length // variable

  const ipcMessage = new Uint8Array(totalLength)
  let offset = 0

  // Write continuation token
  ipcMessage.set(continuationToken, offset)
  offset += continuationToken.length

  // Write metadata length
  ipcMessage.set(metadataLengthBytes, offset)
  offset += metadataLengthBytes.length

  // Write metadata
  ipcMessage.set(flightData.dataHeader, offset)
  offset += metadataLength

  // Write padding
  ipcMessage.set(padding, offset)
  offset += paddingSize

  // Write body
  ipcMessage.set(flightData.dataBody, offset)

  return ipcMessage
}
