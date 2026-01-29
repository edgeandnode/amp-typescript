import * as Auth from "@edgeandnode/amp/auth/service"
import * as Command from "@effect/cli/Command"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"

const handleLogoutCommand = Effect.fnUntraced(function*() {
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
})

export const LogoutCommand = Command.make("logout").pipe(
  Command.withDescription("Logout of the Amp CLI"),
  Command.withHandler(handleLogoutCommand)
)
