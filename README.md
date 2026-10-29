[![pkg.pr.new](https://pkg.pr.new/badge/edgeandnode/amp-typescript)](https://pkg.pr.new/~/edgeandnode/amp-typescript)

# Amp Typescript SDK

## Getting started

Requires Node.js 22.7 or newer (CI uses 24) and pnpm.

```bash
pnpm install
pnpm amp --help
```

`pnpm amp` runs the CLI from source (`packages/cli`). Query an Amp server by
passing its Arrow Flight URL and a token, or by setting `AMP_QUERY_URL` and
`AMP_AUTH_TOKEN`:

```bash
export AMP_QUERY_URL=grpc+tls://gateway.amp.staging.edgeandnode.com:443
export AMP_AUTH_TOKEN=<your token>

pnpm amp query -f json \
  'SELECT block_num, hash FROM "edgeandnode/ethereum_mainnet".blocks ORDER BY block_num DESC LIMIT 5'
```

Without a URL, the CLI queries a local `ampd` at `http://localhost:1602`.

To use the SDK from code, provide the Arrow Flight service with a gRPC transport:

```ts
import * as ArrowFlight from "@edgeandnode/amp/arrow-flight"
import * as NodeArrowFlight from "@edgeandnode/amp/arrow-flight/node"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Redacted from "effect/Redacted"

const token = process.env.AMP_AUTH_TOKEN
if (!token) throw new Error("Set AMP_AUTH_TOKEN to your Amp access token")

const FlightLayer = ArrowFlight.layer.pipe(
  Layer.provide(NodeArrowFlight.layerTransportGrpc({ baseUrl: "https://gateway.amp.staging.edgeandnode.com" })),
  Layer.provide(ArrowFlight.layerInterceptorToken(Redacted.make(token)))
)

const program = Effect.gen(function* () {
  const flight = yield* ArrowFlight.ArrowFlight
  const batches = yield* flight.query(`SELECT block_num FROM "edgeandnode/ethereum_mainnet".blocks LIMIT 5`)
  console.log(batches.flatMap((batch) => batch.data))
})

Effect.runPromise(program.pipe(Effect.provide(FlightLayer)))
```

The Admin API client lives in `@edgeandnode/amp/admin/service`:
`AdminApi.layer({ url: "https://gateway.amp.staging.edgeandnode.com/controller", jwt })`,
provided with an `HttpClient` such as `FetchHttpClient.layer`.

Note: `@edgeandnode/amp` on npm (0.0.55) was published from the older SDK in the
`edgeandnode/amp` repository and does not match this code.
