import * as HttpBody from "@effect/platform/HttpBody"
import * as HttpClient from "@effect/platform/HttpClient"
import type * as HttpClientError from "@effect/platform/HttpClientError"
import * as HttpClientRequest from "@effect/platform/HttpClientRequest"
import * as HttpClientResponse from "@effect/platform/HttpClientResponse"
import * as KeyValueStore from "@effect/platform/KeyValueStore"
import * as UrlParams from "@effect/platform/UrlParams"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as DateTime from "effect/DateTime"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as ParseResult from "effect/ParseResult"
import * as Predicate from "effect/Predicate"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as Jose from "jose"
import { AccessToken, Address, AuthInfo, RefreshToken, TokenDuration, UserId } from "../core/domain.ts"
import { pkceChallenge } from "../internal/pkce.ts"
import {
  AuthCacheError,
  AuthDeviceFlowError,
  AuthNetworkError,
  AuthRateLimitError,
  AuthRefreshError,
  AuthTokenExpiredError,
  AuthUserMismatchError,
  AuthVerifyTokenError
} from "./error.ts"

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

export class GenerateTokenRequest extends Schema.Class<GenerateTokenRequest>(
  "Amp/Auth/GenerateTokenRequest"
)({
  audience: Schema.optional(Schema.Array(Schema.String)),
  duration: Schema.optional(TokenDuration)
}) {}

export class GenerateTokenResponse extends Schema.Class<GenerateTokenResponse>(
  "Amp/Auth/GenerateTokenResponse"
)({
  token: AccessToken,
  token_type: Schema.Literal("Bearer"),
  exp: Schema.Int.pipe(Schema.positive()),
  sub: Schema.NonEmptyTrimmedString,
  iss: Schema.String
}) {}

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
// Legacy Errors (kept for backwards compatibility)
// =============================================================================

export class VerifySignedAccessTokenError extends Schema.TaggedError<VerifySignedAccessTokenError>(
  "Amp/Auth/VerifySignedAccessTokenError"
)("VerifySignedAccessTokenError", { cause: Schema.Defect }) {}

// =============================================================================
// Service
// =============================================================================

export class Auth extends Context.Tag("Amp/Auth")<Auth, {
  readonly createChallenge: Effect.Effect<PKCEChallenge>

  readonly requestDeviceAuthorization: (codeChallenge: CodeChallenge) => Effect.Effect<
    DeviceAuthorizationResponse,
    AuthNetworkError | AuthRefreshError
  >

  readonly pollDeviceToken: (deviceCode: DeviceCode, codeVerifier: CodeVerifier) => Effect.Effect<
    AuthInfo,
    AuthNetworkError | AuthCacheError | AuthDeviceFlowError
  >

  readonly refreshAccessToken: (authInfo: AuthInfo) => Effect.Effect<
    AuthInfo,
    | AuthNetworkError
    | AuthCacheError
    | AuthTokenExpiredError
    | AuthRateLimitError
    | AuthRefreshError
    | AuthUserMismatchError
  >

  readonly generateAccessToken: (options: {
    readonly authInfo: AuthInfo
    readonly audience?: ReadonlyArray<string> | undefined
    readonly duration?: TokenDuration | undefined
  }) => Effect.Effect<
    GenerateTokenResponse,
    | AuthNetworkError
    | AuthTokenExpiredError
    | AuthRateLimitError
    | AuthRefreshError
  >

  readonly verifyAccessToken: (
    token: Redacted.Redacted<string>,
    issuer: string
  ) => Effect.Effect<Jose.JWTPayload, AuthVerifyTokenError>

  readonly getCachedAuthInfo: Effect.Effect<Option.Option<AuthInfo>, AuthCacheError>

  readonly setCachedAuthInfo: (authInfo: AuthInfo) => Effect.Effect<void, AuthCacheError>

  readonly clearCachedAuthInfo: Effect.Effect<void, AuthCacheError>
}>() {}

// =============================================================================
// Service Implementation
// =============================================================================

const make = Effect.gen(function*() {
  const store = yield* KeyValueStore.KeyValueStore
  const kvs = store.forSchema(AuthInfo)

  const httpClient = (yield* HttpClient.HttpClient).pipe(
    HttpClient.mapRequest(HttpClientRequest.prependUrl(AUTH_PLATFORM_BASE_URL.toString()))
  )

  // ------------------------------------------------------------------------
  // Error Handling Helpers
  // ------------------------------------------------------------------------

  /**
   * Executes an authenticated HTTP request with standard error handling.
   * Handles status codes (401, 403, 429) and wraps HTTP errors into SDK errors.
   */
  const executeAuthenticatedRequest = <A>(
    request: HttpClientRequest.HttpClientRequest,
    endpoint: string,
    decodeBody: (response: HttpClientResponse.HttpClientResponse) => Effect.Effect<
      A,
      ParseResult.ParseError | HttpClientError.ResponseError
    >
  ) =>
    httpClient.execute(request).pipe(
      Effect.timeout("15 seconds"),
      Effect.flatMap(
        HttpClientResponse.matchStatus({
          "2xx": decodeBody,
          401: () =>
            Effect.fail(
              new AuthTokenExpiredError({
                message: "Access token is no longer valid (401 Unauthorized)"
              })
            ),
          403: () =>
            Effect.fail(
              new AuthTokenExpiredError({
                message: "Access token lacks required permissions (403 Forbidden)"
              })
            ),
          429: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            const retryAfter = Option.fromNullable(response.headers["retry-after"]).pipe(
              Option.flatMap((retryAfter) => {
                const parsed = Number.parseInt(retryAfter, 10)
                return Number.isNaN(parsed)
                  ? Option.none()
                  : Option.some(Duration.seconds(parsed))
              }),
              Option.getOrElse(() => Duration.minutes(1))
            )
            return yield* new AuthRateLimitError({ message, retryAfter })
          }),
          orElse: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            return yield* new AuthRefreshError({
              message,
              status: Option.some(response.status),
              cause: Option.none()
            })
          })
        })
      ),
      Effect.catchTag("TimeoutException", (cause) =>
        Effect.fail(
          new AuthNetworkError({
            message: `Request to ${endpoint} timed out`,
            endpoint: Option.some(endpoint),
            isTimeout: true,
            cause: Option.some(cause)
          })
        )),
      Effect.catchTag("RequestError", (cause) =>
        Effect.fail(
          new AuthNetworkError({
            message: `Connection failed to ${endpoint}: ${cause.message}`,
            endpoint: Option.some(endpoint),
            isTimeout: false,
            cause: Option.some(cause)
          })
        )),
      Effect.catchTag("ParseError", (cause) =>
        Effect.fail(
          new AuthRefreshError({
            message: `Failed to parse ${endpoint} response: ${cause.message}`,
            status: Option.none(),
            cause: Option.some(cause)
          })
        )),
      Effect.catchTag("ResponseError", (cause) =>
        Effect.fail(
          new AuthRefreshError({
            message: `Failed to read ${endpoint} response: ${cause.message}`,
            status: Option.some(cause.response.status),
            cause: Option.some(cause)
          })
        ))
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
      const endpoint = "/api/v1/device/authorize"
      return yield* httpClient.post(endpoint, {
        acceptJson: true,
        body: HttpBody.unsafeJson({
          code_challenge: codeChallenge,
          code_challenge_method: "S256"
        })
      }).pipe(
        Effect.timeout("30 seconds"),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(DeviceAuthorizationResponse)),
        Effect.catchTag("TimeoutException", (cause) =>
          Effect.fail(
            new AuthNetworkError({
              message: `Request to ${endpoint} timed out`,
              endpoint: Option.some(endpoint),
              isTimeout: true,
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("RequestError", (cause) =>
          Effect.fail(
            new AuthNetworkError({
              message: `Connection failed to ${endpoint}: ${cause.message}`,
              endpoint: Option.some(endpoint),
              isTimeout: false,
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("ParseError", (cause) =>
          Effect.fail(
            new AuthRefreshError({
              message: `Failed to parse ${endpoint} response: ${cause.message}`,
              status: Option.none(),
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("ResponseError", (cause) =>
          Effect.fail(
            new AuthRefreshError({
              message: `Failed to read ${endpoint} response: ${cause.message}`,
              status: Option.some(cause.response.status),
              cause: Option.some(cause)
            })
          ))
      )
    }
  )

  const pollDeviceToken = Effect.fn("Auth.pollDeviceToken")(
    function*(deviceCode: DeviceCode, codeVerifier: CodeVerifier) {
      const endpoint = "/api/v1/device/token"
      const response = yield* httpClient.get(endpoint, {
        acceptJson: true,
        urlParams: UrlParams.fromInput({
          "device_code": deviceCode,
          "code_verifier": codeVerifier
        })
      }).pipe(
        Effect.timeout("10 seconds"),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(DeviceTokenPollingResponse)),
        Effect.catchTag("TimeoutException", (cause) =>
          Effect.fail(
            new AuthNetworkError({
              message: `Request to ${endpoint} timed out`,
              endpoint: Option.some(endpoint),
              isTimeout: true,
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("RequestError", (cause) =>
          Effect.fail(
            new AuthNetworkError({
              message: `Connection failed to ${endpoint}: ${cause.message}`,
              endpoint: Option.some(endpoint),
              isTimeout: false,
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("ResponseError", (cause) =>
          Effect.fail(
            new AuthNetworkError({
              message: `Device token request failed: ${cause.message}`,
              endpoint: Option.some(endpoint),
              isTimeout: false,
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("ParseError", () =>
          Effect.fail(
            new AuthDeviceFlowError({
              message: "Failed to parse device token response",
              reason: "expired",
              verificationUri: Option.none()
            })
          ))
      )

      if (response._tag === "DeviceTokenPendingResponse") {
        return yield* new AuthDeviceFlowError({
          message: "Device authorization is still pending",
          reason: "pending",
          verificationUri: Option.none()
        })
      }

      if (response._tag === "DeviceTokenExpiredResponse") {
        return yield* new AuthDeviceFlowError({
          message: "Device authorization code has expired",
          reason: "expired",
          verificationUri: Option.none()
        })
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
  // OAuth2 Generate / Refresh Token
  // ------------------------------------------------------------------------

  const generateAccessToken = Effect.fn("Auth.generateAccessToken")(
    function*({ authInfo, audience, duration }: {
      readonly authInfo: AuthInfo
      readonly audience?: ReadonlyArray<string> | undefined
      readonly duration?: TokenDuration | undefined
    }) {
      const endpoint = "/api/v1/auth/generate"
      const request = HttpClientRequest.post(endpoint, {
        body: HttpBody.unsafeJson(new GenerateTokenRequest({ audience, duration })),
        acceptJson: true
      }).pipe(HttpClientRequest.bearerToken(authInfo.accessToken))

      return yield* executeAuthenticatedRequest(
        request,
        endpoint,
        HttpClientResponse.schemaBodyJson(GenerateTokenResponse)
      )
    }
  )

  const refreshAccessToken = Effect.fn("Auth.refreshAccessToken")(
    function*(authInfo: AuthInfo) {
      const endpoint = "/api/v1/auth/refresh"
      const request = HttpClientRequest.post(endpoint, {
        body: HttpBody.unsafeJson(RefreshTokenRequest.fromAuthInfo(authInfo)),
        acceptJson: true
      }).pipe(HttpClientRequest.bearerToken(authInfo.accessToken))

      const response = yield* executeAuthenticatedRequest(
        request,
        endpoint,
        HttpClientResponse.schemaBodyJson(RefreshTokenResponse)
      )

      // Validate that the received user ID matches the cached user ID
      if (response.user.id !== authInfo.userId) {
        return yield* new AuthUserMismatchError({
          message: `Expected user ID ${authInfo.userId} but received ${response.user.id}`,
          expectedUserId: authInfo.userId,
          receivedUserId: response.user.id
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
  // OAuth2 Token Verification
  // ------------------------------------------------------------------------

  const JWKS = Jose.createRemoteJWKSet(new URL("./.well-known/jwks.json", AUTH_PLATFORM_BASE_URL))

  const verifyAccessToken = Effect.fn("Auth.verifyAccessToken")(
    function*(token: Redacted.Redacted<string>, issuer: string) {
      const result = yield* Effect.tryPromise({
        try: () => Jose.jwtVerify(Redacted.value(token), JWKS, { issuer }),
        catch: (cause) => {
          if (!(cause instanceof Jose.errors.JOSEError)) {
            return new AuthVerifyTokenError({
              message: `Unknown verification error: ${String(cause)}`,
              reason: "unknown",
              claim: Option.none(),
              cause: Option.some(cause)
            })
          }
          switch (cause.code) {
            case "ERR_JWT_EXPIRED":
              return new AuthVerifyTokenError({
                message: `Token expired: ${cause.message}`,
                reason: "expired",
                claim: Option.fromNullable((cause as Jose.errors.JWTExpired).claim),
                cause: Option.some(cause)
              })
            case "ERR_JWS_SIGNATURE_VERIFICATION_FAILED":
              return new AuthVerifyTokenError({
                message: `Signature verification failed: ${cause.message}`,
                reason: "invalid_signature",
                claim: Option.none(),
                cause: Option.some(cause)
              })
            case "ERR_JWT_CLAIM_VALIDATION_FAILED":
              return new AuthVerifyTokenError({
                message: `Claim validation failed: ${(cause as Jose.errors.JWTClaimValidationFailed).claim} - ${
                  (cause as Jose.errors.JWTClaimValidationFailed).reason
                }`,
                reason: "invalid_claims",
                claim: Option.fromNullable((cause as Jose.errors.JWTClaimValidationFailed).claim),
                cause: Option.some(cause)
              })
            case "ERR_JWKS_NO_MATCHING_KEY":
            case "ERR_JWKS_TIMEOUT":
              return new AuthVerifyTokenError({
                message: `JWKS error: ${cause.message}`,
                reason: "jwks_error",
                claim: Option.none(),
                cause: Option.some(cause)
              })
            default:
              return new AuthVerifyTokenError({
                message: `Verification error: ${cause.message}`,
                reason: "unknown",
                claim: Option.none(),
                cause: Option.some(cause)
              })
          }
        }
      })
      return result.payload
    }
  )

  // ------------------------------------------------------------------------
  // Cache Operations
  // ------------------------------------------------------------------------

  const getCachedAuthInfo = Effect.gen(function*() {
    const cacheResult = yield* kvs.get(AUTH_INFO_CACHE_KEY).pipe(
      // Treat "not found" as Option.none() before wrapping other errors
      Effect.catchIf(
        (error) => error._tag === "SystemError" && error.reason === "NotFound",
        () => Effect.succeed(Option.none<AuthInfo>())
      ),
      Effect.catchTag("SystemError", (cause) =>
        Effect.fail(
          new AuthCacheError({
            message: `Cache read failed: ${cause.message}`,
            operation: "read",
            cause: Option.some(cause)
          })
        )),
      Effect.catchTag("ParseError", (cause) =>
        Effect.fail(
          new AuthCacheError({
            message: `Cache read failed: ${cause.message}`,
            operation: "read",
            cause: Option.some(cause)
          })
        )),
      Effect.catchTag("BadArgument", (cause) =>
        Effect.fail(
          new AuthCacheError({
            message: `Cache read failed: ${cause.message}`,
            operation: "read",
            cause: Option.some(cause)
          })
        ))
    )

    if (Option.isNone(cacheResult)) {
      return Option.none<AuthInfo>()
    }

    const cache = cacheResult.value
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
      const refreshed = yield* refreshAccessToken(cache).pipe(
        Effect.option // Catch refresh errors and return None
      )
      return refreshed
    }

    // Token is still valid, return as is
    return Option.some(cache)
  }).pipe(
    Effect.withSpan("AuthService.getCachedAuthInfo")
  )

  const setCachedAuthInfo = Effect.fn("Auth.setCachedAuthInfo")(
    function*(authInfo: AuthInfo) {
      yield* kvs.set(AUTH_INFO_CACHE_KEY, authInfo).pipe(
        Effect.catchTag("SystemError", (cause) =>
          Effect.fail(
            new AuthCacheError({
              message: `Cache write failed: ${cause.message}`,
              operation: "write",
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("ParseError", (cause) =>
          Effect.fail(
            new AuthCacheError({
              message: `Cache write failed: ${cause.message}`,
              operation: "write",
              cause: Option.some(cause)
            })
          )),
        Effect.catchTag("BadArgument", (cause) =>
          Effect.fail(
            new AuthCacheError({
              message: `Cache write failed: ${cause.message}`,
              operation: "write",
              cause: Option.some(cause)
            })
          ))
      )
    }
  )

  const clearCachedAuthInfo = kvs.remove(AUTH_INFO_CACHE_KEY).pipe(
    Effect.catchIf(
      (error) => error._tag === "SystemError" && error.reason === "NotFound",
      () => Effect.void
    ),
    Effect.catchTag("SystemError", (cause) =>
      Effect.fail(
        new AuthCacheError({
          message: `Cache clear failed: ${cause.message}`,
          operation: "clear",
          cause: Option.some(cause)
        })
      )),
    Effect.catchTag("BadArgument", (cause) =>
      Effect.fail(
        new AuthCacheError({
          message: `Cache clear failed: ${cause.message}`,
          operation: "clear",
          cause: Option.some(cause)
        })
      )),
    Effect.withSpan("Auth.clearCachedAuthInfo")
  )

  return {
    createChallenge,
    requestDeviceAuthorization,
    pollDeviceToken,
    generateAccessToken,
    refreshAccessToken,
    verifyAccessToken,
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
