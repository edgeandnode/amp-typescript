import * as AuthError from "@edgeandnode/amp/auth/error"
import * as Auth from "@edgeandnode/amp/auth/service"
import * as Models from "@edgeandnode/amp/core"
import * as Console from "effect/Console"
import * as Data from "effect/Data"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Redacted from "effect/Redacted"
import * as Runtime from "effect/Runtime"
import * as String from "effect/String"
import * as Argument from "effect/unstable/cli/Argument"
import * as Command from "effect/unstable/cli/Command"
import * as Flag from "effect/unstable/cli/Flag"

export class TokenCommandError extends Data.TaggedError("TokenCommandError")<{
  readonly cause?:
    | AuthError.AuthCacheError
    | AuthError.AuthDeviceFlowError
    | AuthError.AuthNetworkError
    | AuthError.AuthProtocolError
    | AuthError.AuthRefreshError
    | AuthError.AuthRequestError
    | undefined
}> {
  override readonly [Runtime.errorExitCode] = 1
  override readonly [Runtime.errorReported] = false
}

const audienceFlag = Flag.string("audience").pipe(
  Flag.withAlias("a"),
  Flag.withDescription(
    "URLs that are valid to use the generated access token. " +
      "Becomes the JWT aud value"
  ),
  Flag.atLeast(0)
)

const durationArg = Argument.string("duration").pipe(
  Argument.withDescription(
    "Duration of the generated access token before it expires. " +
      "Ex: \"7 days\", \"30 days\", \"1 hour\""
  ),
  Argument.withSchema(Models.TokenDuration)
)

const handleTokenCommand = Effect.fnUntraced(function*({ audience, duration }: {
  readonly audience: Array<string>
  readonly duration: Models.TokenDuration
}) {
  const auth = yield* Auth.Auth

  const authInfo = yield* auth.getCachedAuthInfo.pipe(
    Effect.flatMap(Effect.fromOption),
    Effect.catchTag(
      "NoSuchElementError",
      Effect.fnUntraced(function*() {
        const errorMessage = [
          "You must be authenticated with Amp to generate an access token.",
          "Run \"amp auth login\" to authenticate."
        ].join(" ")
        yield* Console.error(errorMessage)
        return yield* new TokenCommandError({})
      })
    )
  )

  const response = yield* auth.generateAccessToken({ authInfo, audience, duration }).pipe(
    Effect.catch(Effect.fnUntraced(function*(error) {
      const userMessage = AuthError.getUserMessage(error)
      const userSuggestion = AuthError.getUserSuggestion(error)
      const errorMessage = `${userMessage}. ${userSuggestion}`
      yield* Console.error(errorMessage)
      return yield* new TokenCommandError({})
    }))
  )

  yield* auth.verifyAccessToken(Redacted.make(response.token), response.iss).pipe(
    Effect.catch(Effect.fnUntraced(function*(error) {
      const userMessage = AuthError.getUserMessage(error)
      const userSuggestion = AuthError.getUserSuggestion(error)
      const errorMessage = [
        "Failed to verify the signed token.",
        userMessage,
        userSuggestion
      ].join("\n")
      yield* Console.error(errorMessage)
      return yield* new TokenCommandError({})
    }))
  )

  const expiresAt = DateTime.makeUnsafe(response.exp * 1000)
  const formatDateTime = DateTime.formatLocal({
    timeStyle: "full",
    dateStyle: "medium"
  })
  const message = [
    "Access token generated successfully!",
    "We do not store this value - make sure you store it securely.",
    "You can use this token as an bearer authorization header in requests to Amp",
    String.stripMargin(
      `|      token: ${response.token}
       |    expires: ${formatDateTime(expiresAt)}`
    )
  ].join("\n\n")

  yield* Console.error(message)
}, Effect.catchTag("AuthCacheError", (cause) => Effect.fail(new TokenCommandError({ cause }))))

export const TokenCommand = Command.make("token", { audience: audienceFlag, duration: durationArg }).pipe(
  Command.withDescription(
    "Generates an access token (Bearer JWT) to be used by your applictaion to interact with Amp"
  ),
  Command.withHandler(handleTokenCommand)
)
