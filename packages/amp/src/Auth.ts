import type * as PlatformError from "@effect/platform/Error"
import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as HttpClientResponse from "@effect/platform/HttpClientResponse"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as UrlParams from "@effect/platform/UrlParams"
import type { TimeoutException } from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as ParseResult from "effect/ParseResult"
import * as Predicate from "effect/Predicate"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import { pkceChallenge } from "./internal/pkce.ts"
import { AccessToken, Address, AuthInfo, RefreshToken, UserId } from "./Models.ts"

const AUTH_INFO_CACHE_KEY = "amp_cli_auth"
export const AUTH_PLATFORM_BASE_URL = new URL("https://auth.amp.thegraph.com/")

// =============================================================================
// Models
// =============================================================================

export const CodeChallenge = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/CodeChallenge")
).annotations({ identifier: "CodeChallenge" })
export type CodeChallenge = Schema.Schema.Type<typeof CodeChallenge>

export const CodeVerifier = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/CodeVerifier")
).annotations({ identifier: "CodeVerifier" })
export type CodeVerifier = Schema.Schema.Type<typeof CodeVerifier>

export class PKCEChallenge extends Schema.Class<PKCEChallenge>(
  "Amp/Auth/PKCEChallenge"
)({
  codeChallenge: CodeChallenge,
  codeVerifier: CodeVerifier
}, { identifier: "PKCEChallenge" }) {}

export const DeviceCode = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/DeviceCode")
).annotations({ identifier: "DeviceCode" })
export type DeviceCode = Schema.Schema.Type<typeof DeviceCode>

export const UserCode = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/UserCode")
).annotations({ identifier: "UserCode" })
export type UserCode = Schema.Schema.Type<typeof UserCode>

export class DeviceAuthorizationResponse extends Schema.Class<DeviceAuthorizationResponse>(
  "Amp/Auth/DeviceAuthorizationResponse"
)({
  deviceCode: DeviceCode.pipe(
    Schema.propertySignature,
    Schema.fromKey("device_code")
  ).annotations({
    description: "Device verification code used for polling"
  }),
  userCode: UserCode.pipe(
    Schema.propertySignature,
    Schema.fromKey("user_code")
  ).annotations({
    description: "User code to display for manual entry"
  }),
  verificationUri: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("verification_uri")
  ).annotations({
    description: "URL where user enters the code"
  }),
  expiresIn: Schema.Int.pipe(
    Schema.positive(),
    Schema.propertySignature,
    Schema.fromKey("expires_in")
  ).annotations({
    description: "Time in seconds until device code expires"
  }),
  interval: Schema.Int.pipe(Schema.positive()).annotations({
    description: "Minimum polling interval in seconds"
  })
}, { identifier: "DeviceAuthorizationResponse" }) {}

export const DeviceTokenResponse = Schema.Struct({
  accessToken: AccessToken.pipe(
    Schema.propertySignature,
    Schema.fromKey("access_token")
  ).annotations({ description: "The access token for authenticated requests" }),
  refreshToken: RefreshToken.pipe(
    Schema.propertySignature,
    Schema.fromKey("refresh_token")
  ).annotations({ description: "The refresh token for renewing access" }),
  userId: UserId.pipe(
    Schema.propertySignature,
    Schema.fromKey("user_id")
  ).annotations({
    description: "The authenticated user's ID"
  }),
  userAccounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Address)).pipe(
    Schema.propertySignature,
    Schema.fromKey("user_accounts")
  ),
  expiresIn: Schema.Int.pipe(
    Schema.positive(),
    Schema.propertySignature,
    Schema.fromKey("expires_in")
  ).annotations({ description: "Seconds until the token expires from receipt" })
}).pipe(
  Schema.attachPropertySignature("_tag", "DeviceTokenResponse")
).annotations({ identifier: "DeviceTokenResponse" })
export type DeviceTokenResponse = typeof DeviceTokenResponse.Type

export const DeviceTokenPendingResponse = Schema.Struct({
  error: Schema.Literal("authorization_pending")
}).pipe(
  Schema.attachPropertySignature("_tag", "DeviceTokenPendingResponse")
).annotations({ identifier: "DeviceTokenPendingResponse" })
export type DeviceTokenPendingResponse = typeof DeviceTokenPendingResponse.Type

export const DeviceTokenExpiredResponse = Schema.Struct({
  error: Schema.Literal("expired_token")
}).pipe(
  Schema.attachPropertySignature("_tag", "DeviceTokenExpiredResponse")
).annotations({ identifier: "DeviceTokenExpiredResponse" })
export type DeviceTokenExpiredResponse = typeof DeviceTokenExpiredResponse.Type

export const DeviceTokenPollingResponse = Schema.Union(
  DeviceTokenResponse,
  DeviceTokenPendingResponse,
  DeviceTokenExpiredResponse
)
export type DeviceTokenPollingResponse = typeof DeviceTokenPollingResponse.Type

export class RefreshTokenRequest extends Schema.Class<RefreshTokenRequest>(
  "Amp/Auth/RefreshTokenRequest"
)({
  refreshToken: Schema.Redacted(RefreshToken).pipe(
    Schema.propertySignature,
    Schema.fromKey("refresh_token")
  ),
  userId: UserId.pipe(
    Schema.propertySignature,
    Schema.fromKey("user_id")
  )
}) {
  static fromAuthInfo(authInfo: AuthInfo) {
    return RefreshTokenRequest.make({
      userId: authInfo.userId,
      refreshToken: authInfo.refreshToken
    })
  }
}

export class RefreshTokenResponse extends Schema.Class<RefreshTokenResponse>(
  "Amp/models/auth/RefreshTokenResponse"
)({
  token: Schema.NonEmptyTrimmedString,
  refreshToken: Schema.NullOr(Schema.String).pipe(
    Schema.propertySignature,
    Schema.fromKey("refresh_token")
  ),
  sessionUpdateAction: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("session_update_action")
  ),
  expiresIn: Schema.Int.pipe(
    Schema.positive(),
    Schema.propertySignature,
    Schema.fromKey("expires_in")
  ).annotations({ description: "Seconds from receipt of when the token expires (def is 1hr)" }),
  user: Schema.Struct({
    id: UserId,
    accounts: Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Address)).annotations({
      description: "List of accounts (connected wallets, etc) belonging to the user",
      examples: [["cmfd6bf6u006vjx0b7xb2eybx", "0x5c8fA0bDf68C915a88cD68291fC7CF011C126C29"]]
    })
  }).annotations({ description: "The user the access token belongs to" })
}) {}

// =============================================================================
// Errors
// =============================================================================

export class AuthTokenExpiredError extends Schema.TaggedError<AuthTokenExpiredError>(
  "Amp/Auth/AuthTokenExpiredError"
)("AuthTokenExpiredError", {}) {}

export class AuthRateLimitError extends Schema.TaggedError<AuthRateLimitError>(
  "Amp/Auth/AuthRateLimitError"
)("AuthRateLimitError", {
  retryAfter: Schema.Int,
  message: Schema.String
}) {}

export class AuthRefreshError extends Schema.TaggedError<AuthRefreshError>(
  "Amp/Auth/AuthRefreshError"
)("AuthRefreshError", {
  status: Schema.Int,
  message: Schema.String
}) {}

export class AuthUserMismatchError extends Schema.TaggedError<AuthUserMismatchError>(
  "Amp/Auth/AuthUserMismatchError"
)("AuthUserMismatchError", {
  expected: Schema.String,
  received: Schema.String
}) {}

export class VerifySignedAccessTokenError extends Schema.TaggedError<VerifySignedAccessTokenError>(
  "Amp/Auth/VerifySignedAccessTokenError"
)("VerifySignedAccessTokenError", { cause: Schema.Defect }) {}

export class DeviceTokenPendingError extends Schema.TaggedError<DeviceTokenPendingError>(
  "Amp/Auth/DeviceTokenPendingError"
)("DeviceTokenPendingError", {}) {}

export class DeviceTokenExpiredError extends Schema.TaggedError<DeviceTokenExpiredError>(
  "Amp/Auth/DeviceTokenExpiredError"
)("DeviceTokenExpiredError", {}) {}

// =============================================================================
// Service
// =============================================================================

export class Auth extends Context.Tag("Amp/Auth")<Auth, {
  readonly createChallenge: Effect.Effect<PKCEChallenge>

  readonly requestDeviceAuthorization: (codeChallenge: CodeChallenge) => Effect.Effect<
    DeviceAuthorizationResponse,
    HttpClientError.HttpClientError | TimeoutException | ParseResult.ParseError
  >

  readonly pollDeviceToken: (deviceCode: DeviceCode, codeVerifier: CodeVerifier) => Effect.Effect<
    AuthInfo,
    | HttpClientError.HttpClientError
    | ParseResult.ParseError
    | PlatformError.PlatformError
    | TimeoutException
    | DeviceTokenPendingError
    | DeviceTokenExpiredError
  >

  readonly refreshAccessToken: (authInfo: AuthInfo) => Effect.Effect<
    AuthInfo,
    | HttpClientError.HttpClientError
    | ParseResult.ParseError
    | PlatformError.PlatformError
    | TimeoutException
    | AuthTokenExpiredError
    | AuthRateLimitError
    | AuthRefreshError
    | AuthUserMismatchError
  >

  readonly getCachedAuthInfo: Effect.Effect<Option.Option<AuthInfo>>

  readonly setCachedAuthInfo: (authInfo: AuthInfo) => Effect.Effect<
    void,
    ParseResult.ParseError | PlatformError.PlatformError
  >

  readonly clearCachedAuthInfo: Effect.Effect<void, PlatformError.PlatformError>
}>() {}

const make = Effect.gen(function*() {
  const store = yield* KeyValueStore.KeyValueStore
  const kvs = store.forSchema(AuthInfo)

  const httpClient = (yield* HttpClient.HttpClient).pipe(
    HttpClient.mapRequest(HttpClientRequest.prependUrl(AUTH_PLATFORM_BASE_URL.toString()))
  )

  // ------------------------------------------------------------------------
  // OAuth2 Authorization Code Flow with PKCE
  // ------------------------------------------------------------------------

  const createChallenge = Effect.gen(function*() {
    const { codeChallenge, codeVerifier } = yield* pkceChallenge()
    return new PKCEChallenge({
      codeChallenge: CodeChallenge.make(codeChallenge),
      codeVerifier: CodeVerifier.make(codeVerifier)
    })
  }).pipe(Effect.withSpan("Auth.createChallenge"))

  const requestDeviceAuthorization = Effect.fn("Auth.requestDeviceAuthorization")(
    function*(codeChallenge: string) {
      return yield* httpClient.post("/api/v1/device/authorize", {
        acceptJson: true,
        body: HttpBody.unsafeJson({
          code_challenge: codeChallenge,
          code_challenge_method: "S256"
        })
      }).pipe(
        Effect.timeout("30 seconds"),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(DeviceAuthorizationResponse))
      )
    }
  )

  const pollDeviceToken = Effect.fn("Auth.pollDeviceToken")(
    function*(deviceCode: DeviceCode, codeVerifier: CodeVerifier) {
      const response = yield* httpClient.get("/api/v1/device/token", {
        acceptJson: true,
        urlParams: UrlParams.fromInput({
          "device_code": deviceCode,
          "code_verifier": codeVerifier
        })
      }).pipe(
        Effect.timeout("10 seconds"),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(DeviceTokenPollingResponse))
      )

      if (response._tag === "DeviceTokenPendingResponse") {
        return yield* new DeviceTokenPendingError()
      }

      if (response._tag === "DeviceTokenExpiredResponse") {
        return yield* new DeviceTokenExpiredError()
      }

      const authInfo = yield* makeAuthInfo({
        accessToken: response.accessToken,
        refreshToken: response.refreshToken,
        expiresIn: response.expiresIn,
        userId: response.userId,
        accounts: response.userAccounts
      })

      yield* setCachedAuthInfo(authInfo)

      return authInfo
    }
  )

  // ------------------------------------------------------------------------
  // OAuth2 Refresh Token
  // ------------------------------------------------------------------------

  const refreshAccessToken = Effect.fn("Auth.refreshAccessToken")(
    function*(authInfo: AuthInfo) {
      const request = HttpClientRequest.post("/refresh", {
        body: HttpBody.unsafeJson(RefreshTokenRequest.fromAuthInfo(authInfo)),
        acceptJson: true
      }).pipe(HttpClientRequest.bearerToken(authInfo.accessToken))

      const response = yield* httpClient.execute(request).pipe(
        Effect.timeout("15 seconds"),
        Effect.flatMap(HttpClientResponse.matchStatus({
          "2xx": (response) => HttpClientResponse.schemaBodyJson(RefreshTokenResponse)(response),
          // Unauthorized
          401: () => Effect.fail(new AuthTokenExpiredError()),
          // Insufficient Permissions
          403: () => Effect.fail(new AuthTokenExpiredError()),
          // Too Many Requests
          429: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            const retryAfter = Option.fromNullable(response.headers["retry-after"]).pipe(
              Option.flatMap((retryAfter) => {
                const parsed = Number.parseInt(retryAfter, 10)
                return Number.isNaN(parsed) ? Option.none() : Option.some(parsed)
              }),
              Option.getOrElse(() => 60)
            )
            return yield* new AuthRateLimitError({ message, retryAfter })
          }),
          orElse: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            return yield* new AuthRefreshError({ message, status: response.status })
          })
        }))
      )

      // Validate that the received user ID matches the cached user ID
      if (response.user.id !== authInfo.userId) {
        return yield* new AuthUserMismatchError({
          expected: authInfo.userId,
          received: response.user.id
        })
      }

      const refreshedAuthInfo = yield* makeAuthInfo({
        accessToken: response.token,
        refreshToken: response.refreshToken ?? Redacted.value(authInfo.refreshToken),
        expiresIn: response.expiresIn,
        userId: response.user.id,
        accounts: response.user.accounts
      })

      // Reset the cached tokens
      yield* setCachedAuthInfo(refreshedAuthInfo)

      return refreshedAuthInfo
    }
  )

  // ------------------------------------------------------------------------
  // Cache Operations
  // ------------------------------------------------------------------------

  const getCachedAuthInfo = Effect.gen(function*() {
    const cache = yield* Effect.flatten(kvs.get(AUTH_INFO_CACHE_KEY))

    const now = yield* Clock.currentTimeMillis

    // Check if we need to refresh the token
    const needsRefresh =
      // Missing expiry field - refresh to populate it
      Predicate.isNullable(cache.expiry) ||
      // Missing accounts field - refresh to populate it
      Predicate.isNullable(cache.accounts) ||
      // Token is expired
      cache.expiry < now ||
      // Token is expiring within 5 minutes
      cache.expiry - now <= 5 * 60 * 1000

    // If a refresh is required, perform the refresh request
    if (needsRefresh) {
      return yield* refreshAccessToken(cache)
    }

    // Token is still valid, return as is
    return cache
  }).pipe(
    Effect.option,
    Effect.withSpan("AuthService.getCachedAuthInfo")
  )

  const setCachedAuthInfo = Effect.fn("Auth.setCachedAuthInfo")(
    function*(authInfo: AuthInfo) {
      yield* kvs.set(AUTH_INFO_CACHE_KEY, authInfo)
    }
  )

  const clearCachedAuthInfo = kvs.remove(AUTH_INFO_CACHE_KEY).pipe(
    Effect.catchIf(
      (error) => error._tag === "SystemError" && error.reason === "NotFound",
      () => Effect.void
    ),
    Effect.withSpan("Auth.clearCachedAuthInfo")
  )

  return {
    createChallenge,
    requestDeviceAuthorization,
    pollDeviceToken,
    refreshAccessToken,
    getCachedAuthInfo,
    setCachedAuthInfo,
    clearCachedAuthInfo
  } as const
})

export const layer: Layer.Layer<
  Auth,
  never,
  HttpClient.HttpClient | KeyValueStore.KeyValueStore
> = Layer.effect(Auth, make)

// =============================================================================
// Internal Utilities
// =============================================================================

// Helper to extract error description from response body
const extractErrorDescription = (response: HttpClientResponse.HttpClientResponse) =>
  response.json.pipe(
    Effect.option,
    Effect.map(
      Option.flatMap((body) =>
        typeof body === "object" && body !== null &&
          "error_description" in body && typeof body.error_description === "string"
          ? Option.some(body.error_description)
          : Option.none()
      )
    ),
    Effect.map(Option.getOrElse(() => "Failed to refresh token"))
  )

const makeAuthInfo = Effect.fnUntraced(function*(params: {
  readonly accessToken: string
  readonly refreshToken: string
  readonly expiresIn: number
  readonly userId: UserId
  readonly accounts: ReadonlyArray<string | Address>
}): Effect.fn.Return<AuthInfo> {
  const now = yield* DateTime.now
  const expiry = DateTime.toEpochMillis(DateTime.add(now, {
    seconds: params.expiresIn
  }))
  const accessToken = AccessToken.make(params.accessToken)
  const refreshToken = RefreshToken.make(params.refreshToken)
  return AuthInfo.make({
    accessToken: Redacted.make(accessToken),
    refreshToken: Redacted.make(refreshToken),
    userId: params.userId,
    accounts: params.accounts,
    expiry
  })
})
