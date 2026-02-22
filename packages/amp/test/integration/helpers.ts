import * as AdminService from "@edgeandnode/amp/admin/service"
import * as Models from "@edgeandnode/amp/core"
import * as Effect from "effect/Effect"

/**
 * Generate a unique dataset name for test isolation.
 * Each test group gets its own name so tests don't collide.
 */
export const uniqueDatasetName = (prefix: string): Models.DatasetName =>
  Models.DatasetName.make(`${prefix}_${Date.now()}`)

/** Terminal job statuses — polling stops when the job reaches one of these. */
const TERMINAL_STATUSES = new Set<string>(["COMPLETED", "STOPPED", "FAILED", "UNKNOWN"])

/**
 * Poll job status until it reaches a terminal state.
 * Polls up to 15 times with 1-second spacing (15s total).
 * Returns the final `JobInfo`.
 */
export const waitForJob = Effect.fn(function*(jobId: number) {
  const admin = yield* AdminService.AdminApi

  for (let attempt = 1; attempt <= 15; attempt++) {
    if (attempt > 1) {
      yield* Effect.sleep("1 second")
    }
    const job = yield* admin.getJobById(jobId)
    if (TERMINAL_STATUSES.has(job.status)) {
      return job
    }
  }

  return yield* Effect.die(
    new Error(
      `Job ${jobId} did not reach terminal state after 15 attempts`
    )
  )
})

/**
 * Poll sync progress until at least one table has blocks.
 * Polls up to 60 times with 2-second spacing (120s total).
 *
 * Resilient to transient errors — the sync progress endpoint may not
 * be available immediately after job completion (the SDK converts some
 * API errors to defects via `Effect.die`).
 */
export const waitForSync = Effect.fn(
  function*(
    namespace: Models.DatasetNamespace,
    name: Models.DatasetName,
    revision: Models.DatasetRevision
  ) {
    const admin = yield* AdminService.AdminApi

    for (let attempt = 1; attempt <= 60; attempt++) {
      if (attempt > 1) {
        yield* Effect.sleep("2 seconds")
      }
      const synced = yield* admin.getDatasetSyncProgress(namespace, name, revision).pipe(
        Effect.map((progress) => progress.tables.some((t) => t.currentBlock !== undefined && t.currentBlock > 0)),
        Effect.catchAllCause(() => Effect.succeed(false))
      )
      if (synced) {
        return
      }
    }

    return yield* Effect.die(
      new Error(
        "Sync progress did not show blocks after 60 attempts"
      )
    )
  }
)
