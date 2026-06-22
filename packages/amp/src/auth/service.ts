import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as DateTime from "effect/DateTime"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Redacted from "effect/Redacted"
import * as Schema from "effect/Schema"
import * as HttpBody from "effect/unstable/http/HttpBody"
import * as HttpClient from "effect/unstable/http/HttpClient"
import type * as HttpClientError from "effect/unstable/http/HttpClientError"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse"
import * as UrlParams from "effect/unstable/http/UrlParams"
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore"
import * as Jose from "jose"
import {
  AccessToken,
  Address,
  AuthInfo,
  NonEmptyTrimmedString,
  RefreshToken,
  TokenDuration,
  UserId
} from "../core/domain.ts"
import { pkceChallenge } from "../internal/pkce.ts"
import {
  AuthCacheError,
  AuthDeviceFlowError,
  AuthNetworkError,
  AuthProtocolError,
  AuthRateLimitError,
  AuthRefreshError,
  AuthRequestError,
  AuthTokenExpiredError,
  AuthUserMismatchError,
  AuthVerifyTokenError
} from "./error.ts"

const AUTH_INFO_CACHE_KEY = "amp_cli_auth"
export const AUTH_PLATFORM_BASE_URL = new URL("https://auth.amp.thegraph.com/")

// =============================================================================
// Models
// =============================================================================

export const CodeChallenge = NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/CodeChallenge")
).annotate({ identifier: "CodeChallenge" })
export type CodeChallenge = Schema.Schema.Type<typeof CodeChallenge>

export const CodeVerifier = NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/CodeVerifier")
).annotate({ identifier: "CodeVerifier" })
export type CodeVerifier = Schema.Schema.Type<typeof CodeVerifier>

export const PKCEChallenge = Schema.Struct({
  codeChallenge: CodeChallenge,
  codeVerifier: CodeVerifier
}).annotate({ identifier: "PKCEChallenge" })
export type PKCEChallenge = typeof PKCEChallenge.Type

export const DeviceCode = NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/DeviceCode")
).annotate({ identifier: "DeviceCode" })
export type DeviceCode = Schema.Schema.Type<typeof DeviceCode>

export const UserCode = NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Auth/UserCode")
).annotate({ identifier: "UserCode" })
export type UserCode = Schema.Schema.Type<typeof UserCode>

export const DeviceAuthorizationResponse = Schema.Struct({
  deviceCode: DeviceCode.annotate({
    description: "Device verification code used for polling"
  }),
  userCode: UserCode.annotate({
    description: "User code to display for manual entry"
  }),
  verificationUri: Schema.String.annotate({
    description: "URL where user enters the code"
  }),
  expiresIn: Schema.Int.check(Schema.isGreaterThan(0)).annotate({
    description: "Time in seconds until device code expires"
  }),
  interval: Schema.Int.check(Schema.isGreaterThan(0)).annotate({
    description: "Minimum polling interval in seconds"
  })
}).pipe(Schema.encodeKeys({
  deviceCode: "device_code",
  userCode: "user_code",
  verificationUri: "verification_uri",
  expiresIn: "expires_in"
})).annotate({ identifier: "DeviceAuthorizationResponse" })
export type DeviceAuthorizationResponse = typeof DeviceAuthorizationResponse.Type

export const DeviceTokenResponse = Schema.Struct({
  _tag: Schema.tagDefaultOmit("DeviceTokenResponse"),
  accessToken: AccessToken.annotate({
    description: "The access token for authenticated requests"
  }),
  refreshToken: RefreshToken.annotate({
    description: "The refresh token for renewing access"
  }),
  userId: UserId.annotate({
    description: "The authenticated user's ID"
  }),
  userAccounts: Schema.Array(Schema.Union([NonEmptyTrimmedString, Address])),
  expiresIn: Schema.Int.check(Schema.isGreaterThan(0)).annotate({
    description: "Seconds until the token expires from receipt"
  })
}).pipe(Schema.encodeKeys({
  accessToken: "access_token",
  refreshToken: "refresh_token",
  userId: "user_id",
  userAccounts: "user_accounts",
  expiresIn: "expires_in"
})).annotate({ identifier: "DeviceTokenResponse" })
export type DeviceTokenResponse = typeof DeviceTokenResponse.Type

export const DeviceTokenPendingResponse = Schema.Struct({
  _tag: Schema.tagDefaultOmit("DeviceTokenPendingResponse"),
  error: Schema.Literal("authorization_pending")
}).annotate({ identifier: "DeviceTokenPendingResponse" })
export type DeviceTokenPendingResponse = typeof DeviceTokenPendingResponse.Type

export const DeviceTokenExpiredResponse = Schema.Struct({
  _tag: Schema.tagDefaultOmit("DeviceTokenExpiredResponse"),
  error: Schema.Literal("expired_token")
}).annotate({ identifier: "DeviceTokenExpiredResponse" })
export type DeviceTokenExpiredResponse = typeof DeviceTokenExpiredResponse.Type

export const DeviceTokenPollingResponse = Schema.Union([
  DeviceTokenResponse,
  DeviceTokenPendingResponse,
  DeviceTokenExpiredResponse
])
export type DeviceTokenPollingResponse = typeof DeviceTokenPollingResponse.Type

export const GenerateTokenRequest = Schema.Struct({
  audience: Schema.optional(Schema.Array(Schema.String)),
  duration: Schema.optional(TokenDuration)
})
export type GenerateTokenRequest = typeof GenerateTokenRequest.Type

export const GenerateTokenResponse = Schema.Struct({
  token: AccessToken,
  token_type: Schema.Literal("Bearer"),
  exp: Schema.Int.check(Schema.isGreaterThan(0)),
  sub: NonEmptyTrimmedString,
  iss: Schema.String
})
export type GenerateTokenResponse = typeof GenerateTokenResponse.Type

export const RefreshTokenRequest = Schema.Struct({
  refreshToken: Schema.Redacted(RefreshToken),
  userId: UserId
}).pipe(Schema.encodeKeys({
  refreshToken: "refresh_token",
  userId: "user_id"
}))
export type RefreshTokenRequest = typeof RefreshTokenRequest.Type

export const RefreshTokenResponse = Schema.Struct({
  token: NonEmptyTrimmedString,
  refreshToken: Schema.NullOr(Schema.String),
  sessionUpdateAction: Schema.String,
  expiresIn: Schema.Int.check(Schema.isGreaterThan(0)).annotate({
    description: "Seconds from receipt of when the token expires (def is 1hr)"
  }),
  user: Schema.Struct({
    id: UserId,
    accounts: Schema.Array(Schema.Union([NonEmptyTrimmedString, Address])).annotate({
      description: "List of accounts (connected wallets, etc) belonging to the user",
      examples: [["cmfd6bf6u006vjx0b7xb2eybx", "0x5c8fA0bDf68C915a88cD68291fC7CF011C126C29"]]
    })
  }).annotate({ description: "The user the access token belongs to" })
}).pipe(Schema.encodeKeys({
  refreshToken: "refresh_token",
  sessionUpdateAction: "session_update_action",
  expiresIn: "expires_in"
}))
export type RefreshTokenResponse = typeof RefreshTokenResponse.Type

// =============================================================================
// Legacy Errors (kept for backwards compatibility)
// =============================================================================

export class VerifySignedAccessTokenError extends Schema.TaggedErrorClass<VerifySignedAccessTokenError>(
  "Amp/Auth/VerifySignedAccessTokenError"
)("VerifySignedAccessTokenError", { cause: Schema.Defect() }) {}

// =============================================================================
// Service
// =============================================================================

export class Auth extends Context.Service<Auth, {
  readonly createChallenge: Effect.Effect<PKCEChallenge>

  readonly requestDeviceAuthorization: (codeChallenge: CodeChallenge) => Effect.Effect<
    DeviceAuthorizationResponse,
    | AuthNetworkError
    | AuthProtocolError
    | AuthRequestError
    | AuthRefreshError
  >

  readonly pollDeviceToken: (deviceCode: DeviceCode, codeVerifier: CodeVerifier) => Effect.Effect<
    AuthInfo,
    | AuthCacheError
    | AuthNetworkError
    | AuthProtocolError
    | AuthRequestError
    | AuthDeviceFlowError
  >

  readonly refreshAccessToken: (authInfo: AuthInfo) => Effect.Effect<
    AuthInfo,
    | AuthNetworkError
    | AuthProtocolError
    | AuthRequestError
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
    | AuthProtocolError
    | AuthRequestError
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
}>()("Amp/Auth") {}

// =============================================================================
// Service Implementation
// =============================================================================

const make = Effect.gen(function*() {
  const store = yield* KeyValueStore.KeyValueStore
  const kvs = KeyValueStore.toSchemaStore(store, AuthInfo)

  const httpClient = (yield* HttpClient.HttpClient).pipe(
    HttpClient.mapRequest(HttpClientRequest.prependUrl(AUTH_PLATFORM_BASE_URL.toString()))
  )

  // ------------------------------------------------------------------------
  // Error Handling Helpers
  // ------------------------------------------------------------------------

  const makeAuthNetworkTimeoutError = (
    endpoint: string,
    cause: unknown
  ): AuthNetworkError =>
    AuthNetworkError.make({
      code: "AUTH_NETWORK_ERROR",
      message: `Request to ${endpoint} timed out`,
      endpoint: Option.some(endpoint),
      isTimeout: true,
      cause: Option.some(cause)
    })

  const makeAuthNetworkRequestError = (
    endpoint: string,
    cause: { readonly message: string }
  ): AuthNetworkError =>
    AuthNetworkError.make({
      code: "AUTH_NETWORK_ERROR",
      message: `Connection failed to ${endpoint}: ${cause.message}`,
      endpoint: Option.some(endpoint),
      isTimeout: false,
      cause: Option.some(cause)
    })

  const makeAuthRefreshSchemaError = (
    endpoint: string,
    cause: { readonly message: string }
  ): AuthRefreshError =>
    AuthRefreshError.make({
      code: "AUTH_REFRESH_FAILED",
      message: `Failed to parse ${endpoint} response: ${cause.message}`,
      status: Option.none(),
      cause: Option.some(cause)
    })

  const makeAuthProtocolDecodeError = (
    endpoint: string,
    cause: unknown
  ): AuthProtocolError =>
    AuthProtocolError.make({
      code: "AUTH_PROTOCOL_ERROR",
      message: `Authentication service returned an unreadable response for ${endpoint}`,
      endpoint: Option.some(endpoint),
      status: Option.none(),
      cause: Option.some(cause)
    })

  const makeAuthProtocolEmptyBodyError = (
    endpoint: string,
    cause: unknown
  ): AuthProtocolError =>
    AuthProtocolError.make({
      code: "AUTH_PROTOCOL_ERROR",
      message: `Authentication service returned an empty response for ${endpoint}`,
      endpoint: Option.some(endpoint),
      status: Option.none(),
      cause: Option.some(cause)
    })

  const makeAuthProtocolStatusCodeError = (
    endpoint: string,
    cause: { readonly response: { readonly status: number } }
  ): AuthProtocolError =>
    AuthProtocolError.make({
      code: "AUTH_PROTOCOL_ERROR",
      message: `Authentication service returned an unexpected status for ${endpoint}`,
      endpoint: Option.some(endpoint),
      status: Option.some(cause.response.status),
      cause: Option.some(cause)
    })

  const makeAuthRequestEncodeError = (
    endpoint: string,
    cause: unknown
  ): AuthRequestError =>
    AuthRequestError.make({
      code: "AUTH_REQUEST_ERROR",
      message: `Failed to encode the authentication request for ${endpoint}`,
      endpoint: Option.some(endpoint),
      cause: Option.some(cause)
    })

  const makeAuthRequestInvalidUrlError = (
    endpoint: string,
    cause: unknown
  ): AuthRequestError =>
    AuthRequestError.make({
      code: "AUTH_REQUEST_ERROR",
      message: `Authentication endpoint is invalid for ${endpoint}`,
      endpoint: Option.some(endpoint),
      cause: Option.some(cause)
    })

  const makeAuthCacheError = (
    operation: "read" | "write" | "clear",
    cause: { readonly message: string }
  ): AuthCacheError =>
    AuthCacheError.make({
      code: "AUTH_CACHE_ERROR",
      message: `Cache ${operation} failed: ${cause.message}`,
      operation,
      cause: Option.some(cause)
    })

  const makeAuthDeviceFlowSchemaError = (): AuthDeviceFlowError =>
    AuthDeviceFlowError.make({
      code: "AUTH_DEVICE_FLOW_ERROR",
      message: "Failed to parse device token response",
      reason: "expired",
      verificationUri: Option.none()
    })

  const makeUnwrappedHttpClientErrorHandlers = <SchemaError>(
    endpoint: string,
    handlers: {
      readonly onSchemaError: (cause: { readonly message: string }) => SchemaError
    }
  ) => ({
    SchemaError: (cause: { readonly message: string }) => Effect.fail(handlers.onSchemaError(cause)),
    TimeoutError: (cause: unknown) => Effect.fail(makeAuthNetworkTimeoutError(endpoint, cause)),
    DecodeError: (cause: unknown) => Effect.fail(makeAuthProtocolDecodeError(endpoint, cause)),
    EmptyBodyError: (cause: unknown) => Effect.fail(makeAuthProtocolEmptyBodyError(endpoint, cause)),
    EncodeError: (cause: unknown) => Effect.fail(makeAuthRequestEncodeError(endpoint, cause)),
    InvalidUrlError: (cause: unknown) => Effect.fail(makeAuthRequestInvalidUrlError(endpoint, cause)),
    StatusCodeError: (cause: { readonly response: { readonly status: number } }) =>
      Effect.fail(makeAuthProtocolStatusCodeError(endpoint, cause)),
    TransportError: (cause: { readonly message: string }) => Effect.fail(makeAuthNetworkRequestError(endpoint, cause))
  })

  /**
   * Executes an authenticated HTTP request with standard error handling.
   * Handles status codes (401, 403, 429) and wraps HTTP errors into SDK errors.
   */
  const executeAuthenticatedRequest = <A>(
    request: HttpClientRequest.HttpClientRequest,
    endpoint: string,
    decodeBody: (response: HttpClientResponse.HttpClientResponse) => Effect.Effect<
      A,
      Schema.SchemaError | HttpClientError.HttpClientError
    >
  ) =>
    httpClient.execute(request).pipe(
      Effect.timeout("15 seconds"),
      Effect.flatMap(
        HttpClientResponse.matchStatus({
          "2xx": decodeBody,
          401: () =>
            Effect.fail(
              AuthTokenExpiredError.make({
                code: "AUTH_TOKEN_EXPIRED",
                message: "Access token is no longer valid (401 Unauthorized)"
              })
            ),
          403: () =>
            Effect.fail(
              AuthTokenExpiredError.make({
                code: "AUTH_TOKEN_EXPIRED",
                message: "Access token lacks required permissions (403 Forbidden)"
              })
            ),
          429: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            const retryAfter = Option.fromNullishOr(response.headers["retry-after"]).pipe(
              Option.flatMap((header) => {
                const parsed = Number.parseInt(header, 10)
                return Number.isNaN(parsed)
                  ? Option.none()
                  : Option.some(Duration.seconds(parsed))
              }),
              Option.getOrElse(() => Duration.minutes(1))
            )
            return yield* Effect.fail(AuthRateLimitError.make({
              code: "AUTH_RATE_LIMITED",
              message,
              retryAfter
            }))
          }),
          orElse: Effect.fnUntraced(function*(response) {
            const message = yield* extractErrorDescription(response)
            return yield* Effect.fail(AuthRefreshError.make({
              code: "AUTH_REFRESH_FAILED",
              message,
              status: Option.some(response.status),
              cause: Option.none()
            }))
          })
        })
      ),
      Effect.unwrapReason("HttpClientError"),
      Effect.catchTags(makeUnwrappedHttpClientErrorHandlers(endpoint, {
        onSchemaError: (cause) => makeAuthRefreshSchemaError(endpoint, cause)
      }))
    )

  // ------------------------------------------------------------------------
  // OAuth2 Authorization Code Flow with PKCE
  // ------------------------------------------------------------------------

  const createChallenge = Effect.gen(function*() {
    const { codeChallenge, codeVerifier } = yield* pkceChallenge()
    return PKCEChallenge.make({
      codeChallenge: CodeChallenge.make(codeChallenge),
      codeVerifier: CodeVerifier.make(codeVerifier)
    })
  }).pipe(Effect.withSpan("Auth.createChallenge"))

  const requestDeviceAuthorization = Effect.fn("Auth.requestDeviceAuthorization")(
    function*(codeChallenge: string) {
      const endpoint = "/api/v1/device/authorize"
      return yield* httpClient.post(endpoint, {
        acceptJson: true,
        body: HttpBody.jsonUnsafe({
          code_challenge: codeChallenge,
          code_challenge_method: "S256"
        })
      }).pipe(
        Effect.timeout("30 seconds"),
        Effect.flatMap(HttpClientResponse.schemaBodyJson(DeviceAuthorizationResponse)),
        Effect.unwrapReason("HttpClientError"),
        Effect.catchTags(makeUnwrappedHttpClientErrorHandlers(endpoint, {
          onSchemaError: (cause) => makeAuthRefreshSchemaError(endpoint, cause)
        }))
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
        Effect.unwrapReason("HttpClientError"),
        Effect.catchTags(makeUnwrappedHttpClientErrorHandlers(endpoint, {
          onSchemaError: () => makeAuthDeviceFlowSchemaError()
        }))
      )

      if (response._tag === "DeviceTokenPendingResponse") {
        return yield* Effect.fail(AuthDeviceFlowError.make({
          code: "AUTH_DEVICE_FLOW_ERROR",
          message: "Device authorization is still pending",
          reason: "pending",
          verificationUri: Option.none()
        }))
      }

      if (response._tag === "DeviceTokenExpiredResponse") {
        return yield* Effect.fail(AuthDeviceFlowError.make({
          code: "AUTH_DEVICE_FLOW_ERROR",
          message: "Device authorization code has expired",
          reason: "expired",
          verificationUri: Option.none()
        }))
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
        body: HttpBody.jsonUnsafe(GenerateTokenRequest.make({ audience, duration })),
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
        body: HttpBody.jsonUnsafe(RefreshTokenRequest.make({
          userId: authInfo.userId,
          refreshToken: authInfo.refreshToken
        })),
        acceptJson: true
      }).pipe(HttpClientRequest.bearerToken(authInfo.accessToken))

      const response = yield* executeAuthenticatedRequest(
        request,
        endpoint,
        HttpClientResponse.schemaBodyJson(RefreshTokenResponse)
      )

      // Validate that the received user ID matches the cached user ID
      if (response.user.id !== authInfo.userId) {
        return yield* Effect.fail(AuthUserMismatchError.make({
          code: "AUTH_USER_MISMATCH",
          message: `Expected user ID ${authInfo.userId} but received ${response.user.id}`,
          expectedUserId: authInfo.userId,
          receivedUserId: response.user.id
        }))
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
            return AuthVerifyTokenError.make({
              code: "AUTH_VERIFY_TOKEN_FAILED",
              message: `Unknown verification error: ${String(cause)}`,
              reason: "unknown",
              claim: Option.none(),
              cause: Option.some(cause)
            })
          }
          switch (cause.code) {
            case "ERR_JWT_EXPIRED":
              return AuthVerifyTokenError.make({
                code: "AUTH_VERIFY_TOKEN_FAILED",
                message: `Token expired: ${cause.message}`,
                reason: "expired",
                claim: Option.fromNullishOr((cause as Jose.errors.JWTExpired).claim),
                cause: Option.some(cause)
              })
            case "ERR_JWS_SIGNATURE_VERIFICATION_FAILED":
              return AuthVerifyTokenError.make({
                code: "AUTH_VERIFY_TOKEN_FAILED",
                message: `Signature verification failed: ${cause.message}`,
                reason: "invalid_signature",
                claim: Option.none(),
                cause: Option.some(cause)
              })
            case "ERR_JWT_CLAIM_VALIDATION_FAILED":
              return AuthVerifyTokenError.make({
                code: "AUTH_VERIFY_TOKEN_FAILED",
                message: `Claim validation failed: ${(cause as Jose.errors.JWTClaimValidationFailed).claim} - ${
                  (cause as Jose.errors.JWTClaimValidationFailed).reason
                }`,
                reason: "invalid_claims",
                claim: Option.fromNullishOr((cause as Jose.errors.JWTClaimValidationFailed).claim),
                cause: Option.some(cause)
              })
            case "ERR_JWKS_NO_MATCHING_KEY":
            case "ERR_JWKS_TIMEOUT":
              return AuthVerifyTokenError.make({
                code: "AUTH_VERIFY_TOKEN_FAILED",
                message: `JWKS error: ${cause.message}`,
                reason: "jwks_error",
                claim: Option.none(),
                cause: Option.some(cause)
              })
            default:
              return AuthVerifyTokenError.make({
                code: "AUTH_VERIFY_TOKEN_FAILED",
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
        (error) => error._tag === "KeyValueStoreError",
        () => Effect.succeed(Option.none<AuthInfo>())
      ),
      Effect.catchTag("SchemaError", (cause) => Effect.fail(makeAuthCacheError("read", cause)))
    )

    if (Option.isNone(cacheResult)) {
      return Option.none<AuthInfo>()
    }

    const cache = cacheResult.value
    const now = yield* Clock.currentTimeMillis

    // Check if we need to refresh the token
    const needsRefresh =
      // Missing expiry field - refresh to populate it
      Predicate.isNullish(cache.expiry) ||
      // Missing accounts field - refresh to populate it
      Predicate.isNullish(cache.accounts) ||
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
        Effect.catchTags({
          SchemaError: (cause) => Effect.fail(makeAuthCacheError("write", cause)),
          KeyValueStoreError: (cause) => Effect.fail(makeAuthCacheError("write", cause))
        })
      )
    }
  )

  const clearCachedAuthInfo = kvs.remove(AUTH_INFO_CACHE_KEY).pipe(
    Effect.ignore,
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
