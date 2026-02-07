---
root: false
targets: ["*"]
description: "When you write any code using Effect, you must follow these standards."
globs: ["**/*.ts"]
---

# Effect Standards

This codebase uses the [Effect](https://effect.website) library throughout. All new code must follow these patterns.

## Imports

Always import Effect modules as namespaces from subpaths:

```typescript
// Good
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Schema from "effect/Schema"

// Bad — never import from the barrel
import { Effect, Stream, Schema } from "effect"
```

## Services

Define services with `Context.Tag`:

```typescript
class MyService extends Context.Tag("Amp/MyService")<MyService, {
  readonly doSomething: (input: string) => Effect.Effect<Result, MyError>
}>() {}
```

Access services in generators with `yield*`:

```typescript
Effect.gen(function*() {
  const svc = yield* MyService
  return yield* svc.doSomething("input")
})
```

## Layers

### Naming: exported layers must start with `layer`

```typescript
// Good
export const layer: Layer.Layer<MyService> = Layer.effect(MyService, make)
export const layerWithState = (initial: StateSnapshot): Layer.Layer<StateStore> => ...
export const layerTest = (options: TestOptions): Layer.Layer<StateStore> => ...

// Bad
export const live: Layer.Layer<MyService> = ...
export const testLayer: Layer.Layer<StateStore> = ...
```

### Compose layers ahead of time — never chain `Effect.provide`

Multiple `Effect.provide` calls each wrap the effect in another layer of indirection. Compose into a single layer and provide once:

```typescript
// Bad — repeated provide calls
Effect.runPromise(program.pipe(
  Effect.provide(TransactionalStream.layer),
  Effect.provide(InMemoryStateStore.layer),
  Effect.provide(ProtocolStream.layer),
  Effect.provide(ArrowFlight.layer),
  Effect.provide(Transport.layer)
))

// Good — compose first, provide once
const AppLayer = TransactionalStream.layer.pipe(
  Layer.provide(InMemoryStateStore.layer),
  Layer.provide(ProtocolStream.layer),
  Layer.provide(ArrowFlight.layer),
  Layer.provide(Transport.layer)
)

Effect.runPromise(program.pipe(Effect.provide(AppLayer)))
```

## Use `Effect.fn` instead of arrow functions returning `Effect.gen`

Wrapping `Effect.gen` in an arrow function allocates a new generator on every call. `Effect.fn` avoids this overhead and automatically adds a tracing span.

```typescript
// Bad — allocates a generator per invocation
const query = (sql: string) =>
  Effect.gen(function*() {
    const svc = yield* ArrowFlight
    return yield* svc.query(sql)
  })

// Good
const query = Effect.fn("query")(function*(sql: string) {
  const svc = yield* ArrowFlight
  return yield* svc.query(sql)
})

// With explicit return type annotation
const query = Effect.fn("query")(function*(sql: string): Effect.fn.Return<
  QueryResult,
  ArrowFlightError
> {
  const svc = yield* ArrowFlight
  return yield* svc.query(sql)
})
```

## Schemas and Branded Types

Use `Schema.brand` for domain primitives:

```typescript
export const Network = Schema.Lowercase.pipe(
  Schema.brand("Amp/Models/Network")
).annotations({ identifier: "Network" })
export type Network = typeof Network.Type
```

## Tagged Errors

Use `Schema.TaggedError` for typed, serializable errors:

```typescript
export class MyError extends Schema.TaggedError<MyError>(
  "Amp/MyError"
)("MyError", {
  cause: Schema.Defect
}) {}
```
