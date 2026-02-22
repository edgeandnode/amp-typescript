import * as AdminService from "@edgeandnode/amp/admin/service"
import type * as Models from "@edgeandnode/amp/core"
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"

/**
 * Generate a unique namespace for test isolation.
 * Each test group gets its own namespace so tests don't collide.
 */
export const uniqueNamespace = (prefix: string): Models.DatasetNamespace =>
  `_test_${prefix}_${Date.now()}` as Models.DatasetNamespace

/**
 * Poll sync progress until at least one table has blocks.
 * Retries up to 30 times with 2-second spacing (60s total).
 */
export const waitForSync = Effect.fn("waitForSync")(
  function*(
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    revision: Models.DatasetRevision
  ) {
    const admin = yield* AdminService.AdminApi

    yield* Effect.retry(
      admin.getDatasetSyncProgress(namespace, name, revision).pipe(
        Effect.flatMap((progress) =>
          progress.tables.some((t) => t.currentBlock !== undefined && t.currentBlock > 0)
            ? Effect.void
            : Effect.fail("not synced yet" as const)
        )
      ),
      Schedule.intersect(
        Schedule.recurs(30),
        Schedule.spaced("2 seconds")
      )
    )
  }
)
