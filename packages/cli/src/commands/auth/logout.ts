import type * as AuthError from "@edgeandnode/amp/auth/error"
import * as Auth from "@edgeandnode/amp/auth/service"
import * as Console from "effect/Console"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Runtime from "effect/Runtime"
import * as Command from "effect/unstable/cli/Command"
import * as Prompt from "effect/unstable/cli/Prompt"

export class LogoutCommandError extends Data.TaggedError("LogoutCommandError")<{
  readonly cause: AuthError.AuthCacheError
}> {
  override readonly [Runtime.errorExitCode] = 1
  override readonly [Runtime.errorReported] = false
}

const handleLogoutCommand = Effect.fnUntraced(
  function*() {
    const auth = yield* Auth.Auth

    const shouldLogout = yield* Prompt.confirm({
      message: "Are you sure you want to logout of Amp?",
      initial: false
    })

    if (!shouldLogout) {
      return yield* Console.error("Logout cancelled, exiting...")
    }

    yield* auth.clearCachedAuthInfo

    yield* Console.error("You have successfully logged out!")
  },
  Effect.catchTag("QuitError", () => Effect.void),
  Effect.mapError((cause) => new LogoutCommandError({ cause }))
)

export const LogoutCommand = Command.make("logout").pipe(
  Command.withDescription("Logout of the Amp CLI"),
  Command.withHandler(handleLogoutCommand)
)
