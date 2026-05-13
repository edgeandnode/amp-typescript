import type * as AuthError from "@edgeandnode/amp/auth/error"
import * as Auth from "@edgeandnode/amp/auth/service"
import * as Console from "effect/Console"
import * as Data from "effect/Data"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Runtime from "effect/Runtime"
import * as Schedule from "effect/Schedule"
import * as String from "effect/String"
import * as Command from "effect/unstable/cli/Command"
import * as Prompt from "effect/unstable/cli/Prompt"
import Open from "open"

export class LoginCommandError extends Data.TaggedError("LoginCommandError")<{
  readonly cause:
    | AuthError.AuthCacheError
    | AuthError.AuthDeviceFlowError
    | AuthError.AuthNetworkError
    | AuthError.AuthProtocolError
    | AuthError.AuthRefreshError
    | AuthError.AuthRequestError
}> {
  override readonly [Runtime.errorExitCode] = 1
  override readonly [Runtime.errorReported] = false
}

const handleLoginCommand = Effect.fnUntraced(
  function*() {
    const auth = yield* Auth.Auth

    const authInfo = yield* auth.getCachedAuthInfo

    // User already authenticated
    if (Option.isSome(authInfo)) {
      yield* Console.error("You are already authenticated with Amp.")
      return yield* Effect.void
    }

    // Perform OAuth2 PKCE flow
    const { codeChallenge, codeVerifier } = yield* auth.createChallenge

    const {
      expiresIn,
      deviceCode,
      interval,
      userCode,
      verificationUri
    } = yield* auth.requestDeviceAuthorization(codeChallenge)

    // Show the user the OAuth2 PKCE code
    yield* Console.error(String.stripMargin(
      `|Copy the following verification code and enter it in your browser:
     |
     |    ${userCode}
     |`
    ))

    // Ask if we should auto-open the user's browser
    const autoOpenBrowser = yield* Prompt.confirm({
      message: "Would you like to open your browser automatically?",
      initial: true
    })

    yield* Console.error()

    if (autoOpenBrowser) {
      // If so, attempt to open the browser, falling back to a useful message
      yield* Effect.tryPromise(() => Open(verificationUri, { wait: false })).pipe(
        Effect.catchCause(() =>
          Console.error(String.stripMargin(
            `|If the browser window does not open automatically, enter the verification code into the following URL:
           |
           |    ${verificationUri}
           |`
          ))
        )
      )
    } else {
      // If not, indicate that the user show navigate to the verification URL
      yield* Console.error(String.stripMargin(
        `|Enter the verification code into the following URL:
       |
       |    ${verificationUri}
       |`
      ))
    }

    // Initially starts polling with a faster exponential backoff (1s, 1.5s, 2.25s, ...),
    // but then caps at the server's requested interval, setting the maximum
    // number of polling attempts based on the device code's lifetime
    const pollingSchedule = Schedule.exponential("1 second", 1.5).pipe(
      Schedule.both(Schedule.spaced(Duration.seconds(interval))),
      Schedule.either(Schedule.recurs(Math.floor(expiresIn / interval)))
    )

    // Show a spinner while we wait
    const spinnerFiber = yield* Effect.forkChild(showSpinner("Waiting for the user to authenticate..."))

    // Poll for the auth info response
    const response = yield* auth.pollDeviceToken(deviceCode, codeVerifier).pipe(
      Effect.retry({
        schedule: pollingSchedule,
        while: (error) => error._tag === "AuthDeviceFlowError" && error.reason === "pending"
      }),
      Effect.tapCause(() => Console.error("Authentication timed out or failed. Please try again.")),
      Effect.ensuring(Fiber.interrupt(spinnerFiber))
    )

    // Cache the auth information so it can be used by other commands
    yield* auth.setCachedAuthInfo(response)

    yield* Console.error("Authenticated successfully!")
  },
  Effect.catchTag("QuitError", () => Effect.void),
  Effect.mapError((cause) => new LoginCommandError({ cause }))
)

export const LoginCommand = Command.make("login").pipe(
  Command.withDescription("Login to the Amp CLI"),
  Command.withHandler(handleLoginCommand)
)

// =============================================================================
// Internal Utilities
// =============================================================================

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

const showSpinner = Effect.fnUntraced(function*(message: string) {
  let index = 0
  return yield* Effect.sync(() => {
    const frame = SPINNER_FRAMES[index]
    const spinner = `\r\x1b[36m${frame}\x1b[0m ${message}`
    process.stdout.write(spinner)
    index = (index + 1) % SPINNER_FRAMES.length
  }).pipe(
    Effect.schedule(Schedule.fixed("80 millis")),
    // Make sure to cleanup the spinner output
    Effect.ensuring(Effect.sync(() => process.stdout.write("\r\x1b[K")))
  )
})
