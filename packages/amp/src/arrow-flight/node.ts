import { createGrpcTransport, type GrpcTransportOptions } from "@connectrpc/connect-node"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { Interceptors, Transport } from "./transport.ts"

/**
 * Create a `Transport` for the gRPC protocol using the Node.js `http2` module.
 */
export const layerTransportGrpc = (options: GrpcTransportOptions): Layer.Layer<Transport> =>
  Layer.effect(
    Transport,
    Effect.gen(function*() {
      const interceptors = yield* Interceptors
      return createGrpcTransport({
        ...options,
        interceptors: [...(options.interceptors ?? []), ...interceptors]
      })
    })
  )
