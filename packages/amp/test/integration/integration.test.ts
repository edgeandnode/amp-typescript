import { AdminApi } from "@edgeandnode/amp/admin/service"
import { ArrowFlight } from "@edgeandnode/amp/arrow-flight"
import { it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { expect } from "vitest"
import { IntegrationLayer } from "./layers.ts"

it.layer(IntegrationLayer, { timeout: "2 minutes" })("Integration", (it) => {
  // ===========================================================================
  // AdminApi
  // ===========================================================================

  it.effect("AdminApi.getProviders returns configured providers", () =>
    Effect.gen(function*() {
      const admin = yield* AdminApi
      const response = yield* admin.getProviders
      expect(response.providers.length).toBeGreaterThan(0)
    }))

  it.effect("AdminApi.getDatasets returns a list", () =>
    Effect.gen(function*() {
      const admin = yield* AdminApi
      const response = yield* admin.getDatasets
      expect(response.datasets).toBeInstanceOf(Array)
    }))

  it.effect("AdminApi.getWorkers returns a list", () =>
    Effect.gen(function*() {
      const admin = yield* AdminApi
      const response = yield* admin.getWorkers
      expect(response).toBeDefined()
    }))

  it.effect("AdminApi.getJobs returns a list", () =>
    Effect.gen(function*() {
      const admin = yield* AdminApi
      const response = yield* admin.getJobs()
      expect(response.jobs).toBeInstanceOf(Array)
    }))

  // ===========================================================================
  // ArrowFlight
  // ===========================================================================

  it.effect("ArrowFlight executes a simple SQL query", () =>
    Effect.gen(function*() {
      const flight = yield* ArrowFlight
      const results = yield* flight.query("SELECT 1 AS value")
      expect(results.length).toBeGreaterThan(0)
    }))
})
