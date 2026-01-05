import * as ParseResult from "effect/ParseResult"
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

/**
 * Represents a block number.
 */
export const BlockNumber = Schema.NonNegativeInt.pipe(
  Schema.brand("Amp/Models/BlockNumber")
).annotations({
  identifier: "BlockNumber",
  description: "A block number"
})
export type BlockNumber = typeof BlockNumber.Type

/**
 * Represents a block hash.
 */
export const BlockHash = Schema.NonEmptyTrimmedString.pipe(
  Schema.pattern(/^0x[a-z0-9]{64}/),
  Schema.brand("Amp/Models/BlockHash")
).annotations({ identifier: "BlockHash" })
export type BlockHash = typeof BlockHash.Type

/**
 * Represents a blockchain network.
 */
export const Network = Schema.Lowercase.pipe(
  Schema.brand("Amp/Models/Network")
).annotations({
  title: "Network",
  description: "a blockchain network",
  examples: ["mainnet" as Network]
})
export type Network = typeof Network.Type

/**
 * Represents a range of blocks from a given network.
 */
export const BlockRange = Schema.Struct({
  /**
   * The name of the network from which the associated blocks were extracted.
   */
  network: Network,
  /**
   * A start and end index  representing the inclusive range of block numbers.
   */
  numbers: Schema.Struct({ start: BlockNumber, end: BlockNumber }),
  /**
   * The hash associated with the end block.
   */
  hash: BlockHash,
  /**
   * The hash associated with the parent of the start block, if present
   */
  prevHash: Schema.optional(BlockHash).pipe(
    Schema.fromKey("prev_hash")
  )
}).annotations({
  identifier: "BlockRange",
  description: "A range of blocks on a given network"
})
export type BlockRange = typeof BlockRange.Type

/**
 * Represents metadata carrying information about the block ranges covered by
 * the associated Apache Arrow RecordBatch.
 */
export const RecordBatchMetadata = Schema.Struct({
  /**
   * The block ranges included in the associated Apache Arrow RecordBatch.
   */
  ranges: Schema.Array(BlockRange),
  /**
   * Indicates whether this is the final record batch associated to the ranges.
   */
  rangesComplete: Schema.Boolean.pipe(
    Schema.propertySignature,
    Schema.fromKey("ranges_complete")
  )
}).annotations({
  identifier: "RecordBatchMetadata",
  description: "Metadata carrying information about the block ranges covered by this record batch"
})
export type RecordBatchMetadata = typeof RecordBatchMetadata.Type

/**
 * Represents the conversion of the binary `appMetadata` received from a
 * `FlightData` response into metadata about the associated Arrow Flight
 * RecordBatch.
 */
export const RecordBatchMetadataFromUint8Array = Schema.transformOrFail(
  Schema.Uint8ArrayFromSelf,
  Schema.parseJson(RecordBatchMetadata),
  {
    strict: true,
    encode: (decoded, _, ast) =>
      ParseResult.try({
        try: () => new TextEncoder().encode(decoded),
        catch: () => new ParseResult.Type(ast, decoded, "Failed to encode record batch metadata")
      }),
    decode: (encoded, _, ast) =>
      ParseResult.try({
        try: () => new TextDecoder().decode(encoded),
        catch: () => new ParseResult.Type(ast, encoded, "Failed to encode record batch metadata")
      })
  }
).pipe(Schema.asSchema)
export type RecordBatchMetadataFromUint8Array = typeof RecordBatchMetadataFromUint8Array.Type
