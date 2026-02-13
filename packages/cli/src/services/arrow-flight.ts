import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Layer from "effect/Layer"
import type * as Redacted from "effect/Redacted"

/**
 * A layer which constructs an `ArrowFlight` service which will used cached
 * credentials for authentication.
 */
export const layer = ArrowFlight.layer.pipe(
  Layer.provide(NodeArrowFlight.layerTransportGrpc({
    baseUrl: "http://localhost:1602"
  })),
  Layer.provide(ArrowFlight.layerInterceptorBearerAuth)
)

/**
 * A layer which constructs an `ArrowFlight` service which will used cached
 * credentials for authentication.
 *
 * Also accepts a generated access token which can be used for authentication.
 */
export const layerToken = (token: Redacted.Redacted<string>) =>
  ArrowFlight.layer.pipe(
    Layer.provide(NodeArrowFlight.layerTransportGrpc({
      baseUrl: "http://localhost:1602"
    })),
    Layer.provide(ArrowFlight.layerInterceptorToken(token))
  )
