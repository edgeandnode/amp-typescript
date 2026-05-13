import { create } from "@bufbuild/protobuf"
import { createRouterTransport } from "@connectrpc/connect"
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import {
  type FlightData,
  FlightDataSchema,
  FlightEndpointSchema,
  FlightInfoSchema,
  FlightService,
  TicketSchema
} from "@edgeandnode/amp/protobuf/Flight_pb"
import { describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as FlightDataGenerator from "../arrow-test-harness/FlightDataGenerator.ts"
import * as SchemaBuilder from "../arrow-test-harness/SchemaBuilder.ts"

const encoder = new TextEncoder()

const recordBatchMetadata = encoder.encode(JSON.stringify({ ranges: [], ranges_complete: false }))

const toHex = (bytes: Uint8Array): string =>
  Array.from(bytes).map((byte) => byte.toString(16).padStart(2, "0")).join("")

const makeTransport = (flightData: ReadonlyArray<FlightData>) =>
  createRouterTransport((router) => {
    const ticket = create(TicketSchema, { ticket: encoder.encode("ticket") })
    const endpoint = create(FlightEndpointSchema, {
      appMetadata: new Uint8Array(0),
      location: [],
      ticket
    })
    const flightInfo = create(FlightInfoSchema, {
      appMetadata: new Uint8Array(0),
      endpoint: [endpoint],
      ordered: true,
      schema: new Uint8Array(0),
      totalBytes: 0n,
      totalRecords: 1n
    })

    router.service(FlightService, {
      doGet() {
        async function* messages() {
          yield* flightData
        }
        return messages()
      },
      getFlightInfo() {
        return flightInfo
      }
    })
  })

const toProtoFlightData = (
  flightData: { readonly dataHeader: Uint8Array; readonly dataBody: Uint8Array },
  appMetadata: Uint8Array
): FlightData =>
  create(FlightDataSchema, {
    appMetadata,
    dataBody: flightData.dataBody,
    dataHeader: flightData.dataHeader
  })

describe("ArrowFlight", () => {
  it.effect("passes binaryHandling to query output conversion", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .binary("bin")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        defaultNullRate: 0,
        fields: { bin: { minLength: 3, maxLength: 3 } },
        numRows: 1,
        seed: 333
      })

      const firstExpected = generated.expectedValues.bin?.[0]
      if (!(firstExpected instanceof Uint8Array)) {
        return yield* Effect.die(new Error("Expected generated binary value"))
      }

      const transport = makeTransport([
        toProtoFlightData(generated.schemaFlightData, new Uint8Array(0)),
        toProtoFlightData(generated.recordBatchFlightData, recordBatchMetadata)
      ])
      const layer = ArrowFlight.layer.pipe(
        Layer.provide(Layer.succeed(ArrowFlight.Transport, transport))
      )

      const results = yield* Effect.gen(function*() {
        const flight = yield* ArrowFlight.ArrowFlight
        return yield* flight.query("SELECT bin FROM test", { binaryHandling: "hex" })
      }).pipe(Effect.provide(layer))

      expect(results).toHaveLength(1)
      expect(results[0]?.data).toEqual([{ bin: toHex(firstExpected) }])
    }))
})
