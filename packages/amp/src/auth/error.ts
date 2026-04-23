import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Match from "effect/Match"
import * as Schema from "effect/Schema"

// =============================================================================
// Helpers
// =============================================================================

export const makeError = <
  const Code extends string,
  const Tag extends string,
  const Fields extends Schema.Struct.Fields = {}
>(
  code: Code,
  tag: Tag,
  fields?: Fields
): Schema.encodeKeys<
  Schema.Struct<
    Fields & {
      readonly _tag: Schema.withDecodingDefaultKey<Schema.tag<Tag>>
      /**
       * Machine-readable error code in SCREAMING_SNAKE_CASE format
       *
       * Error codes are stable across API versions and should be used
       * for programmatic error handling. Examples: `INVALID_SELECTOR`,
       * `DATASET_NOT_FOUND`, `METADATA_DB_ERROR`
       */
      readonly code: Schema.withConstructorDefault<Schema.Literal<Code>>
      /**
       * Human-readable error message
       *
       * Messages provide detailed context about the error but may change
       * over time. Use `error_code` for programmatic decisions.
       */
      readonly message: Schema.String
    }
  >,
  { readonly code: "error_code"; readonly message: "error_message" }
> =>
  Schema.Struct({
    ...fields,
    _tag: Schema.tagDefaultOmit(tag),
    /**
     * Machine-readable error code in SCREAMING_SNAKE_CASE format
     *
     * Error codes are stable across API versions and should be used
     * for programmatic error handling. Examples: `INVALID_SELECTOR`,
     * `DATASET_NOT_FOUND`, `METADATA_DB_ERROR`
     */
    code: Schema.Literal(code).pipe(
      Schema.withConstructorDefault(Effect.succeed(code))
    ),
    /**
     * Human-readable error message
     *
     * Messages provide detailed context about the error but may change
     * over time. Use `error_code` for programmatic decisions.
     */
    message: Schema.String
  }).pipe(Schema.encodeKeys({
    code: "error_code",
    message: "error_message"
  })) as any

// =============================================================================
// Error Details
// =============================================================================

export const DeviceFlowReason = Schema.Literals([
  "expired",
  "pending",
  "access_denied",
  "slow_down"
])

export type DeviceFlowReason = Schema.Schema.Type<typeof DeviceFlowReason>

export const CacheOperation = Schema.Literals(["read", "write", "clear"])

export type CacheOperation = Schema.Schema.Type<typeof CacheOperation>

export const VerifyTokenFailureReason = Schema.Literals([
  "expired",
  "invalid_signature",
  "invalid_claims",
  "jwks_error",
  "unknown"
])

export type VerifyTokenFailureReason = Schema.Schema.Type<typeof VerifyTokenFailureReason>

// =============================================================================
// Errors
// =============================================================================

/**
 * Indicates that the user's session has expired and they need to re-authenticate.
 */
export const AuthTokenExpiredError = makeError(
  "AUTH_TOKEN_EXPIRED",
  "AuthTokenExpiredError"
)

export type AuthTokenExpiredError = typeof AuthTokenExpiredError.Type

/**
 * Indicates that too many authentication requests have been made.
 */
export const AuthRateLimitError = makeError(
  "AUTH_RATE_LIMITED",
  "AuthRateLimitError",
  {
    retryAfter: Schema.DurationFromMillis
  }
)

export type AuthRateLimitError = typeof AuthRateLimitError.Type

/**
 * Indicates a general token refresh failure.
 */
export const AuthRefreshError = makeError(
  "AUTH_REFRESH_FAILED",
  "AuthRefreshError",
  {
    status: Schema.OptionFromOptional(Schema.Int),
    cause: Schema.OptionFromOptional(Schema.Defect)
  }
)

export type AuthRefreshError = typeof AuthRefreshError.Type

/**
 * Indicates that the token belongs to a different user than expected.
 */
export const AuthUserMismatchError = makeError(
  "AUTH_USER_MISMATCH",
  "AuthUserMismatchError",
  {
    expectedUserId: Schema.String,
    receivedUserId: Schema.String
  }
)

export type AuthUserMismatchError = typeof AuthUserMismatchError.Type

/**
 * Indicates an issue with the device authorization flow.
 */
export const AuthDeviceFlowError = makeError(
  "AUTH_DEVICE_FLOW_ERROR",
  "AuthDeviceFlowError",
  {
    reason: DeviceFlowReason,
    verificationUri: Schema.OptionFromOptional(Schema.String)
  }
)

export type AuthDeviceFlowError = typeof AuthDeviceFlowError.Type

/**
 * Indicates a failure with cache read/write/clear operations.
 */
export const AuthCacheError = makeError(
  "AUTH_CACHE_ERROR",
  "AuthCacheError",
  {
    operation: CacheOperation,
    cause: Schema.OptionFromOptional(Schema.Defect)
  }
)

export type AuthCacheError = typeof AuthCacheError.Type

/**
 * Indicates network or timeout issues during authentication.
 */
export const AuthNetworkError = makeError(
  "AUTH_NETWORK_ERROR",
  "AuthNetworkError",
  {
    endpoint: Schema.OptionFromOptional(Schema.String),
    isTimeout: Schema.Boolean,
    cause: Schema.OptionFromOptional(Schema.Defect)
  }
)

export type AuthNetworkError = typeof AuthNetworkError.Type

/**
 * Indicates that the client failed to construct or encode an authentication request.
 */
export const AuthRequestError = makeError(
  "AUTH_REQUEST_ERROR",
  "AuthRequestError",
  {
    endpoint: Schema.OptionFromOptional(Schema.String),
    cause: Schema.OptionFromOptional(Schema.Defect)
  }
)

export type AuthRequestError = typeof AuthRequestError.Type

/**
 * Indicates that the authentication service returned a response that violated the expected protocol.
 */
export const AuthProtocolError = makeError(
  "AUTH_PROTOCOL_ERROR",
  "AuthProtocolError",
  {
    endpoint: Schema.OptionFromOptional(Schema.String),
    status: Schema.OptionFromOptional(Schema.Int),
    cause: Schema.OptionFromOptional(Schema.Defect)
  }
)

export type AuthProtocolError = typeof AuthProtocolError.Type

/**
 * Indicates a failure when verifying a JWT access token.
 */
export const AuthVerifyTokenError = makeError(
  "AUTH_VERIFY_TOKEN_FAILED",
  "AuthVerifyTokenError",
  {
    reason: VerifyTokenFailureReason,
    claim: Schema.OptionFromOptional(Schema.String),
    cause: Schema.OptionFromOptional(Schema.Defect)
  }
)

export type AuthVerifyTokenError = typeof AuthVerifyTokenError.Type

// =============================================================================
// Union Type
// =============================================================================

/**
 * A union of all authentication errors for exhaustive handling.
 */
export type AuthError =
  | AuthTokenExpiredError
  | AuthRateLimitError
  | AuthRefreshError
  | AuthUserMismatchError
  | AuthDeviceFlowError
  | AuthCacheError
  | AuthNetworkError
  | AuthRequestError
  | AuthProtocolError
  | AuthVerifyTokenError

export const getUserMessage = Match.type<AuthError>().pipe(
  Match.when({ code: "AUTH_TOKEN_EXPIRED" }, () => "Your session has expired"),
  Match.when({ code: "AUTH_RATE_LIMITED" }, () => "Too many authentication requests"),
  Match.when({ code: "AUTH_REFRESH_FAILED" }, () => "Failed to refresh your authentication token"),
  Match.when({ code: "AUTH_USER_MISMATCH" }, () => "Authentication identity mismatch detected"),
  Match.when({ code: "AUTH_DEVICE_FLOW_ERROR" }, (error) => {
    switch (error.reason) {
      case "expired":
        return "The login code has expired"
      case "pending":
        return "Waiting for authorization to complete"
      case "access_denied":
        return "Authorization was denied"
      case "slow_down":
        return "Too many login attempts"
    }
  }),
  Match.when({ code: "AUTH_CACHE_ERROR" }, (error) => {
    switch (error.operation) {
      case "read":
        return "Could not read saved credentials"
      case "write":
        return "Could not save your credentials"
      case "clear":
        return "Could not clear saved credentials"
    }
  }),
  Match.when({ code: "AUTH_NETWORK_ERROR" }, (error) =>
    error.isTimeout
      ? "Authentication request timed out"
      : "Could not connect to the authentication service"),
  Match.when({ code: "AUTH_REQUEST_ERROR" }, () => "Failed to prepare the authentication request"),
  Match.when({ code: "AUTH_PROTOCOL_ERROR" }, () => "Authentication service returned an invalid response"),
  Match.when({ code: "AUTH_VERIFY_TOKEN_FAILED" }, (error) => {
    switch (error.reason) {
      case "expired":
        return "The access token has expired"
      case "invalid_signature":
        return "The access token signature is invalid"
      case "invalid_claims":
        return "The access token claims are invalid"
      case "jwks_error":
        return "Could not verify the access token"
      case "unknown":
        return "Token verification failed"
    }
  }),
  Match.exhaustive
)

export const getUserSuggestion = Match.type<AuthError>().pipe(
  Match.when({ code: "AUTH_TOKEN_EXPIRED" }, () => "Run 'amp auth login' to sign in again"),
  Match.when(
    { code: "AUTH_RATE_LIMITED" },
    (error) => `Please wait about ${Duration.format(error.retryAfter)} before trying again`
  ),
  Match.when(
    { code: "AUTH_REFRESH_FAILED" },
    () => "Try signing out and signing in again with 'amp auth logout' then 'amp auth login'"
  ),
  Match.when(
    { code: "AUTH_USER_MISMATCH" },
    () => "Your cached credentials may be corrupted. Run 'amp auth logout' and 'amp auth login' to re-authenticate"
  ),
  Match.when({ code: "AUTH_DEVICE_FLOW_ERROR" }, (error) => {
    switch (error.reason) {
      case "expired":
        return "Run 'amp auth login' to start a new login session"
      case "pending":
        return "Complete the login in your browser"
      case "access_denied":
        return "Run 'amp auth login' to try again"
      case "slow_down":
        return "Please wait a moment before trying again"
    }
  }),
  Match.when({ code: "AUTH_CACHE_ERROR" }, (error) =>
    error.operation === "clear"
      ? "You may need to manually remove the credentials file"
      : "Check file permissions in your configuration directory and try again"),
  Match.when({ code: "AUTH_NETWORK_ERROR" }, (error) =>
    error.isTimeout
      ? "The service may be experiencing high load. Please try again in a few moments"
      : "Check your internet connection and try again"),
  Match.when(
    { code: "AUTH_REQUEST_ERROR" },
    () => "This is a client-side request construction problem. Please report it if it persists"
  ),
  Match.when(
    { code: "AUTH_PROTOCOL_ERROR" },
    () => "Try again in a moment. If the problem persists, the authentication service may have changed"
  ),
  Match.when({ code: "AUTH_VERIFY_TOKEN_FAILED" }, (error) => {
    switch (error.reason) {
      case "expired":
      case "invalid_signature":
      case "invalid_claims":
        return "Run 'amp auth login' to obtain a new token"
      case "jwks_error":
        return "Check your internet connection and try again"
      case "unknown":
        return "Try signing out and signing in again"
    }
  }),
  Match.exhaustive
)
