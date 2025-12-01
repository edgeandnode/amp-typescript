import { createGrpcTransport, type GrpcTransportOptions } from "@connectrpc/connect-node"
import * as Layer from "effect/Layer"
import { Transport } from "../ArrowFlight.ts"

/**
 * Create a `Transport` for the gRPC protocol using the Node.js `http2` module.
 */
export const layerTransportGrpc = (options: GrpcTransportOptions): Layer.Layer<Transport> =>
  Layer.sync(Transport, () => createGrpcTransport(options))
