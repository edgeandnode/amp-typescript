import * as Duration from "effect/Duration"
import * as Schema from "effect/Schema"

// =============================================================================
// Helpers
// =============================================================================

const AuthErrorCode = <Code extends string>(code: Code) =>
  Schema.Literal(code).pipe(
    Schema.propertySignature,
    Schema.fromKey("error_code"),
    Schema.withConstructorDefault(() => code)
  )

const BaseAuthErrorFields = {
  message: Schema.String.pipe(
    Schema.propertySignature,
    Schema.fromKey("error_message")
  )
}

// =============================================================================
// Errors
// =============================================================================

/**
 * Indicates that the user's session has expired and they need to re-authenticate.
 */
export class AuthTokenExpiredError extends Schema.TaggedError<AuthTokenExpiredError>(
  "Amp/Auth/AuthTokenExpiredError"
)("AuthTokenExpiredError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_TOKEN_EXPIRED")
}) {
  get userMessage(): string {
    return "Your session has expired"
  }
  get userSuggestion(): string {
    return "Run 'amp auth login' to sign in again"
  }
}

/**
 * Indicates that too many authentication requests have been made.
 */
export class AuthRateLimitError extends Schema.TaggedError<AuthRateLimitError>(
  "Amp/Auth/AuthRateLimitError"
)("AuthRateLimitError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_RATE_LIMITED"),
  retryAfter: Schema.DurationFromMillis
}) {
  get userMessage(): string {
    return "Too many authentication requests"
  }
  get userSuggestion(): string {
    const duration = Duration.format(this.retryAfter)
    return `Please wait about ${duration} before trying again`
  }
}

/**
 * Indicates a general token refresh failure.
 */
export class AuthRefreshError extends Schema.TaggedError<AuthRefreshError>(
  "Amp/Auth/AuthRefreshError"
)("AuthRefreshError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_REFRESH_FAILED"),
  status: Schema.optionalWith(Schema.Int, { as: "Option" }),
  cause: Schema.optionalWith(Schema.Defect, { as: "Option" })
}) {
  get userMessage(): string {
    return "Failed to refresh your authentication"
  }
  get userSuggestion(): string {
    return "Try signing out and signing in again with 'amp auth logout' then 'amp auth login'"
  }
}

/**
 * Indicates that the token belongs to a different user than expected.
 */
export class AuthUserMismatchError extends Schema.TaggedError<AuthUserMismatchError>(
  "Amp/Auth/AuthUserMismatchError"
)("AuthUserMismatchError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_USER_MISMATCH"),
  expectedUserId: Schema.String,
  receivedUserId: Schema.String
}) {
  get userMessage(): string {
    return "Authentication identity mismatch detected"
  }
  get userSuggestion(): string {
    return "Your cached credentials may be corrupted. Run 'amp auth logout' and 'amp auth login' to re-authenticate"
  }
}

const DeviceFlowReason = Schema.Literal("expired", "pending", "access_denied", "slow_down")
export type DeviceFlowReason = Schema.Schema.Type<typeof DeviceFlowReason>

/**
 * Indicates an issue with the device authorization flow.
 */
export class AuthDeviceFlowError extends Schema.TaggedError<AuthDeviceFlowError>(
  "Amp/Auth/AuthDeviceFlowError"
)("AuthDeviceFlowError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_DEVICE_FLOW_ERROR"),
  reason: DeviceFlowReason,
  verificationUri: Schema.optionalWith(Schema.String, { as: "Option" })
}) {
  get userMessage(): string {
    switch (this.reason) {
      case "expired":
        return "The login code has expired"
      case "pending":
        return "Waiting for authorization to complete"
      case "access_denied":
        return "Authorization was denied"
      case "slow_down":
        return "Too many login attempts"
    }
  }
  get userSuggestion(): string {
    switch (this.reason) {
      case "expired":
        return "Run 'amp auth login' to start a new login session"
      case "pending":
        return "Complete the login in your browser"
      case "access_denied":
        return "Run 'amp auth login' to try again"
      case "slow_down":
        return "Please wait a moment before trying again"
    }
  }
}

const CacheOperation = Schema.Literal("read", "write", "clear")
export type CacheOperation = Schema.Schema.Type<typeof CacheOperation>

/**
 * Indicates a failure with cache read/write/clear operations.
 */
export class AuthCacheError extends Schema.TaggedError<AuthCacheError>(
  "Amp/Auth/AuthCacheError"
)("AuthCacheError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_CACHE_ERROR"),
  operation: CacheOperation,
  cause: Schema.optionalWith(Schema.Defect, { as: "Option" })
}) {
  get userMessage(): string {
    switch (this.operation) {
      case "read":
        return "Could not read saved credentials"
      case "write":
        return "Could not save your credentials"
      case "clear":
        return "Could not clear saved credentials"
    }
  }
  get userSuggestion(): string {
    return this.operation === "clear"
      ? "You may need to manually remove the credentials file"
      : "Check file permissions in your configuration directory and try again"
  }
}

/**
 * Indicates network or timeout issues during authentication.
 */
export class AuthNetworkError extends Schema.TaggedError<AuthNetworkError>(
  "Amp/Auth/AuthNetworkError"
)("AuthNetworkError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_NETWORK_ERROR"),
  endpoint: Schema.optionalWith(Schema.String, { as: "Option" }),
  isTimeout: Schema.Boolean,
  cause: Schema.optionalWith(Schema.Defect, { as: "Option" })
}) {
  get userMessage(): string {
    return this.isTimeout
      ? "Authentication request timed out"
      : "Could not connect to the authentication service"
  }
  get userSuggestion(): string {
    return this.isTimeout
      ? "The service may be experiencing high load. Please try again in a few moments"
      : "Check your internet connection and try again"
  }
}

const VerifyTokenFailureReason = Schema.Literal(
  "expired",
  "invalid_signature",
  "invalid_claims",
  "jwks_error",
  "unknown"
)
export type VerifyTokenFailureReason = Schema.Schema.Type<typeof VerifyTokenFailureReason>

/**
 * Indicates a failure when verifying a JWT access token.
 */
export class AuthVerifyTokenError extends Schema.TaggedError<AuthVerifyTokenError>(
  "Amp/Auth/AuthVerifyTokenError"
)("AuthVerifyTokenError", {
  ...BaseAuthErrorFields,
  code: AuthErrorCode("AUTH_VERIFY_TOKEN_FAILED"),
  reason: VerifyTokenFailureReason,
  claim: Schema.optionalWith(Schema.String, { as: "Option" }),
  cause: Schema.optionalWith(Schema.Defect, { as: "Option" })
}) {
  get userMessage(): string {
    switch (this.reason) {
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
  }
  get userSuggestion(): string {
    switch (this.reason) {
      case "expired":
      case "invalid_signature":
      case "invalid_claims":
        return "Run 'amp auth login' to obtain a new token"
      case "jwks_error":
        return "Check your internet connection and try again"
      case "unknown":
        return "Try signing out and signing in again"
    }
  }
}

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
  | AuthVerifyTokenError
