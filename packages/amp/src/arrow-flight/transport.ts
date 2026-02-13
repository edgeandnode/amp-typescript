import { createContextKey, type Interceptor, type Transport as ConnectTransport } from "@connectrpc/connect"
import * as Arr from "effect/Array"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"
import type { AuthInfo } from "../core/domain.ts"

// =============================================================================
// Connect RPC Transport
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

/**
 * A service which abstracts the set of interceptors that are passed to a given
 * transport.
 *
 * An interceptor can add logic to clients or servers, similar to the decorators
 * or middleware you may have seen in other libraries. Interceptors may
 * mutate the request and response, catch errors and retry/recover, emit
 * logs, or do nearly everything else.
 */
export class Interceptors extends Context.Reference<Interceptors>()(
  "Amp/ArrowFlight/ConnectRPC/Interceptors",
  { defaultValue: () => Arr.empty<Interceptor>() }
) {}

export const AuthInfoContextKey = createContextKey<AuthInfo | undefined>(
  undefined,
  { description: "Authentication information obtained from the Amp auth server" }
)

/**
 * A layer which will add an interceptor to the configured set of `Interceptors`
 * which uses the specified `token` for bearer token authentication.
 */
export const layerInterceptorToken = (token: Redacted.Redacted<string>) =>
  Layer.effectContext(Effect.gen(function*() {
    const interceptors = yield* Interceptors

    const interceptor: Interceptor = (next) => (request) => {
      const accessToken = Redacted.value(token)

      request.header.append("Authorization", `Bearer ${accessToken}`)

      return next(request)
    }

    return Context.make(Interceptors, Arr.append(interceptors, interceptor))
  }))

/**
 * A layer which will add an interceptor to the configured set of `Interceptors`
 * which attempts to read authentication information from the Connect context
 * values.
 *
 * If authentication information is found, the interceptor will add an
 * `"Authorization"` header to the request containing a bearer token with the
 * value of the authentication information access token.
 */
export const layerInterceptorBearerAuth = Layer.effectContext(
  Effect.gen(function*() {
    const interceptors = yield* Interceptors

    const interceptor: Interceptor = (next) => (request) => {
      const authInfo = request.contextValues.get(AuthInfoContextKey)

      if (authInfo !== undefined) {
        const accessToken = Redacted.value(authInfo.accessToken)
        request.header.append("Authorization", `Bearer ${accessToken}`)
      }
      return next(request)
    }

    return Context.make(Interceptors, Arr.append(interceptors, interceptor))
  })
)
