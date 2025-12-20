import * as Schema from "effect/Schema"
import { isAddress } from "viem"

/**
 * A branded type representing a string in Ethereum address format. An Ethereum
 * address is a unique, 42-character hexadecimal identifier (starting with `0x`)
 * used to send and receive funds.
 */
export const Address = Schema.NonEmptyTrimmedString.pipe(
  Schema.filter((val) => isAddress(val)),
  Schema.brand("Amp/Models/Address")
).annotations({ identifier: "Address" })
export type Address = typeof Address.Type

/**
 * A branded type representing an OAuth2 access token.
 */
export const AccessToken = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Models/AccessToken")
).annotations({ identifier: "AccessToken" })
export type AccessToken = typeof AccessToken.Type

/**
 * A branded type representing an OAuth2 refresh token.
 */
export const RefreshToken = Schema.NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Models/RefreshToken")
).annotations({ identifier: "RefreshToken" })
export type RefreshToken = typeof RefreshToken.Type

/**
 * A branded type representing the identifier for an authenticated user.
 */
export const UserId = Schema.NonEmptyTrimmedString.pipe(
  Schema.pattern(/^(c[a-z0-9]{24}|did:privy:c[a-z0-9]{24})$/),
  Schema.brand("Amp/Models/UserId")
).annotations({ identifier: "UserId" })
export type UserId = typeof UserId.Type

/**
 * Represents authentication information obtained from the Amp.
 */
export const AuthInfo = Schema.Struct({
  accessToken: Schema.Redacted(AccessToken),
  refreshToken: Schema.Redacted(RefreshToken),
  userId: UserId,
  accounts: Schema.optional(Schema.Array(Schema.Union(Schema.NonEmptyTrimmedString, Address))),
  expiry: Schema.Int.pipe(Schema.positive(), Schema.optional)
}).annotations({ identifier: "AuthInfo" })
export type AuthInfo = typeof AuthInfo.Type
