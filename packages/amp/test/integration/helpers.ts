import * as AdminService from "@edgeandnode/amp/admin/service"
import type * as Models from "@edgeandnode/amp/core"
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"

/**
 * Generate a unique dataset name for test isolation.
 * Each test group gets its own name so tests don't collide.
 */
export const uniqueDatasetName = (prefix: string): Models.DatasetName => `${prefix}_${Date.now()}` as Models.DatasetName

/** Terminal job statuses — polling stops when the job reaches one of these. */
const TERMINAL_STATUSES = new Set<string>(["COMPLETED", "STOPPED", "FAILED", "UNKNOWN"])

/**
 * Poll job status until it reaches a terminal state.
 * Retries up to 60 times with 2-second spacing (120s total).
 * Returns the final `JobInfo`.
 */
export const waitForJob = Effect.fn("waitForJob")(
  function*(jobId: number) {
    const admin = yield* AdminService.AdminApi

    return yield* Effect.retry(
      admin.getJobById(jobId).pipe(
        Effect.flatMap((job) =>
          TERMINAL_STATUSES.has(job.status)
            ? Effect.succeed(job)
            : Effect.fail("job not terminal yet" as const)
        )
      ),
      Schedule.intersect(
        Schedule.recurs(60),
        Schedule.spaced("2 seconds")
      )
    )
  }
)

/**
 * Poll sync progress until at least one table has blocks.
 * Retries up to 60 times with 2-second spacing (120s total).
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
        Schedule.recurs(60),
        Schedule.spaced("2 seconds")
      )
    )
  }
)
