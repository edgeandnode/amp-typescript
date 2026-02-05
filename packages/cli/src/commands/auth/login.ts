import * as Auth from "@edgeandnode/amp/auth/service"
import * as Command from "@effect/cli/Command"
import * as Prompt from "@effect/cli/Prompt"
import * as Console from "effect/Console"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Schedule from "effect/Schedule"
import * as String from "effect/String"
import Open from "open"

const handleLoginCommand = Effect.fnUntraced(function*() {
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
  }).pipe(Effect.zipLeft(Console.error()))

  if (autoOpenBrowser) {
    // If so, attempt to open the browser, falling back to a useful message
    yield* Effect.try(() => Open(verificationUri, { wait: false })).pipe(
      Effect.catchAllCause(() =>
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
    Schedule.union(Schedule.spaced(Duration.seconds(interval))),
    Schedule.intersect(Schedule.recurs(Math.floor(expiresIn / interval)))
  )

  // Show a spinner while we wait
  const spinnerFiber = yield* Effect.fork(showSpinner("Waiting for the user to authenticate..."))

  // Poll for the auth info response
  const response = yield* auth.pollDeviceToken(deviceCode, codeVerifier).pipe(
    Effect.retry({
      schedule: pollingSchedule,
      while: (error) => error._tag === "AuthDeviceFlowError" && error.reason === "pending"
    }),
    Effect.tapErrorCause(() => Console.error("Authentication timed out or failed. Please try again.")),
    Effect.ensuring(Fiber.interrupt(spinnerFiber))
  )

  // Cache the auth information so it can be used by other commands
  yield* auth.setCachedAuthInfo(response)

  yield* Console.error("Authenticated successfully!")
})

export const LoginCommand = Command.make("login").pipe(
  Command.withDescription("Login to the Amp CLI"),
  Command.withHandler(handleLoginCommand)
)

// =============================================================================
// Internal Utilities
// =============================================================================

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

const showSpinner = Effect.fnUntraced(function*(message) {
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
