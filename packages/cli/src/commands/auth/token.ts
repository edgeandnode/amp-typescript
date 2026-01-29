import * as Auth from "@edgeandnode/amp/auth/service"
import * as Models from "@edgeandnode/amp/models"
import * as Args from "@effect/cli/Args"
import * as Command from "@effect/cli/Command"
import * as Options from "@effect/cli/Options"
import * as Console from "effect/Console"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"
import * as String from "effect/String"
import * as Errors from "../../errors.ts"

const audience = Options.text("audience").pipe(
  Options.withAlias("a"),
  Options.withDescription(
    "URLs that are valid to use the generated access token. " +
      "Becomes the JWT aud value"
  ),
  Options.repeated
)

const duration = Args.text({ name: "duration" }).pipe(
  Args.withDescription(
    "Duration of the generated access token before it expires. " +
      "Ex: \"7 days\", \"30 days\", \"1 hour\""
  ),
  Args.withSchema(Models.TokenDuration)
)

const handleTokenCommand = Effect.fnUntraced(function*({ audience, duration }: {
  readonly audience: Array<string>
  readonly duration: Models.TokenDuration
}) {
  const auth = yield* Auth.Auth

  const authInfo = yield* auth.getCachedAuthInfo.pipe(
    Effect.flatten,
    Effect.catchTag(
      "NoSuchElementException",
      Effect.fnUntraced(function*() {
        const errorMessage = [
          "You must be authenticated with Amp to generate an access token.",
          "Run \"amp auth login\" to authenticate."
        ].join(" ")
        yield* Console.error(errorMessage)
        return yield* new Errors.NonZeroExitCode()
      })
    )
  )

  const response = yield* auth.generateAccessToken({ authInfo, audience, duration }).pipe(
    Effect.catchAll(Effect.fnUntraced(function*(error) {
      const errorMessage = [
        "Failed to generate access token.\n",
        error.userMessage,
        error.userSuggestion
      ].join("\n")
      yield* Console.error(errorMessage)
      return yield* new Errors.NonZeroExitCode()
    }))
  )

  yield* auth.verifyAccessToken(Redacted.make(response.token), response.iss).pipe(
    Effect.catchAll(Effect.fnUntraced(function*(error) {
      const errorMessage = [
        "Failed to verify the signed token.",
        error.userMessage,
        error.userSuggestion
      ].join("\n")
      yield* Console.error(errorMessage)
      return yield* new Errors.NonZeroExitCode()
    }))
  )

  const expiresAt = DateTime.unsafeMake(response.exp * 1000)
  const formatDateTime = DateTime.formatLocal({
    timeStyle: "full",
    dateStyle: "medium"
  })
  const message = [
    "Access token generated successfully!",
    "We do not store this value - you will need to store it safely.",
    "You can use this token as an bearer authorization header in requests to Amp",
    String.stripMargin(
      `|      token: ${response.token}
       |    expires: ${formatDateTime(expiresAt)}`
    )
  ].join("\n\n")

  yield* Console.error(message)
})

export const TokenCommand = Command.make("token", { audience, duration }).pipe(
  Command.withDescription(
    "Generates an access token (Bearer JWT) to be used by your applictaion to interact with Amp"
  ),
  Command.withHandler(handleTokenCommand)
)
