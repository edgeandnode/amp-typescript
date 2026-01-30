import type { Interceptor } from "@connectrpc/connect"
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Arr from "effect/Array"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Redacted from "effect/Redacted"

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
export const layerToken = (token: Option.Option<Redacted.Redacted<string>>) =>
  ArrowFlight.layer.pipe(
    Layer.provide(NodeArrowFlight.layerTransportGrpc({
      baseUrl: "http://localhost:1602"
    })),
    Layer.provide(Layer.unwrapEffect(Effect.gen(function*() {
      const interceptors = yield* ArrowFlight.Interceptors
      if (Option.isSome(token)) {
        const interceptor: Interceptor = (next) => (request) => {
          const accessToken = Redacted.value(token.value)
          request.header.append("Authorization", `Bearer ${accessToken}`)
          return next(request)
        }
        const context = Context.make(ArrowFlight.Interceptors, Arr.append(interceptors, interceptor))
        return Layer.succeedContext(context)
      }
      return ArrowFlight.layerInterceptorBearerAuth
    })))
  )
