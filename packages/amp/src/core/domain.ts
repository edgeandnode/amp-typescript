import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as SchemaGetter from "effect/SchemaGetter"
import * as SchemaIssue from "effect/SchemaIssue"
import * as SchemaParser from "effect/SchemaParser"
import * as SchemaTransformation from "effect/SchemaTransformation"
import { isAddress } from "viem"

/**
 * A schema representing a non-empty trimmed string.
 */
export const NonEmptyTrimmedString = Schema.Trimmed.check(Schema.isNonEmpty())

export type NonEmptyTrimmedString = typeof NonEmptyTrimmedString.Type

export const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

export type NonNegativeInt = typeof NonNegativeInt.Type

/**
 * A branded type representing a string in Ethereum address format. An Ethereum
 * address is a unique, 42-character hexadecimal identifier (starting with `0x`)
 * used to send and receive funds.
 */
export const Address = NonEmptyTrimmedString.check(
  Schema.makeFilter((val) => isAddress(val))
).pipe(
  Schema.brand("Amp/Models/Address")
).annotate({ identifier: "Address" })
export type Address = typeof Address.Type

/**
 * A branded type representing an OAuth2 access token.
 */
export const AccessToken = NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Models/AccessToken")
).annotate({ identifier: "AccessToken" })
export type AccessToken = typeof AccessToken.Type

/**
 * A branded type representing an OAuth2 refresh token.
 */
export const RefreshToken = NonEmptyTrimmedString.pipe(
  Schema.brand("Amp/Models/RefreshToken")
).annotate({ identifier: "RefreshToken" })
export type RefreshToken = typeof RefreshToken.Type

const TOKEN_DURATION_REGEX =
  /^-?\d+\.?\d*\s*(sec|secs|second|seconds|s|minute|minutes|min|mins|m|hour|hours|hr|hrs|h|day|days|d|week|weeks|w|year|years|yr|yrs|y)(\s+ago|\s+from\s+now)?$/i

/**
 * A branded type representing the duration an OAuth2 access token should be
 * valid for.
 */
export const TokenDuration = NonEmptyTrimmedString.check(
  Schema.isPattern(TOKEN_DURATION_REGEX)
).pipe(
  Schema.brand("TokenDuration")
).annotate({
  identifier: "TokenDuration",
  examples: [
    "7 days" as TokenDuration,
    "30 days" as TokenDuration,
    "1 hour" as TokenDuration,
    "1 year" as TokenDuration
  ]
})
export type TokenDuration = typeof TokenDuration.Type

/**
 * A branded type representing the identifier for an authenticated user.
 */
export const UserId = NonEmptyTrimmedString.check(
  Schema.isPattern(/^(c[a-z0-9]{24}|did:privy:c[a-z0-9]{24})$/)
).pipe(
  Schema.brand("Amp/Models/UserId")
).annotate({ identifier: "UserId" })
export type UserId = typeof UserId.Type

/**
 * Represents authentication information obtained from the Amp.
 */
export const AuthInfo = Schema.Struct({
  accessToken: Schema.Redacted(AccessToken),
  refreshToken: Schema.Redacted(RefreshToken),
  userId: UserId,
  accounts: Schema.optional(Schema.Array(
    Schema.Union([NonEmptyTrimmedString, Address])
  )),
  expiry: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0)))
}).annotate({ identifier: "AuthInfo" })
export type AuthInfo = typeof AuthInfo.Type

/**
 * Represents a block number.
 */
export const BlockNumber = NonNegativeInt.pipe(
  Schema.brand("Amp/Models/BlockNumber")
).annotate({
  identifier: "BlockNumber",
  description: "A block number"
})
export type BlockNumber = typeof BlockNumber.Type

/**
 * Represents a block hash.
 */
export const BlockHash = NonEmptyTrimmedString.check(
  Schema.isPattern(/^0x[a-z0-9]{64}/)
).pipe(
  Schema.brand("Amp/Models/BlockHash")
).annotate({ identifier: "BlockHash" })
export type BlockHash = typeof BlockHash.Type

/**
 * Represents a blockchain network.
 */
export const Network = Schema.String.check(
  Schema.isLowercased()
).pipe(
  Schema.brand("Amp/Models/Network")
).annotate({
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
  prevHash: Schema.optional(BlockHash)
}).pipe(Schema.encodeKeys({ prevHash: "prev_hash" })).annotate({
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
  rangesComplete: Schema.Boolean
}).pipe(Schema.encodeKeys({ rangesComplete: "ranges_complete" })).annotate({
  identifier: "RecordBatchMetadata",
  description: "Metadata carrying information about the block ranges covered by this record batch"
})
export type RecordBatchMetadata = typeof RecordBatchMetadata.Type

/**
 * Represents the conversion of the binary `appMetadata` received from a
 * `FlightData` response into metadata about the associated Arrow Flight
 * RecordBatch.
 */
export const RecordBatchMetadataFromUint8Array = Schema.Uint8Array.pipe(
  Schema.decodeTo(
    Schema.fromJsonString(RecordBatchMetadata),
    SchemaTransformation.transformOrFail<string, Uint8Array>({
      encode: (decoded) =>
        Effect.try({
          try: () => new TextEncoder().encode(decoded),
          catch: () =>
            new SchemaIssue.InvalidValue(Option.some(decoded), { message: "Failed to encode record batch metadata" })
        }),
      decode: (encoded) =>
        Effect.try({
          try: () => new TextDecoder().decode(encoded),
          catch: () =>
            new SchemaIssue.InvalidValue(Option.some(encoded), { message: "Failed to decode record batch metadata" })
        })
    })
  )
)
export type RecordBatchMetadataFromUint8Array = typeof RecordBatchMetadataFromUint8Array.Type

/**
 * Represents the namespace or owner of the dataset.
 *
 * If not specified, defaults to `"_"`.
 */
export const DatasetNamespace = Schema.NonEmptyString.check(
  Schema.isPattern(/^[a-z0-9_]+$/)
).pipe(
  Schema.brand("Amp/Models/Address")
).annotate({
  identifier: "DatasetNamespace",
  description: "The namespace or owner of the dataset. If not specified, defaults to \"_\". " +
    "Must contain only lowercase letters, digits, and underscores.",
  examples: [
    "edgeandnode" as DatasetNamespace,
    "0xdeadbeef" as DatasetNamespace,
    "my_org" as DatasetNamespace,
    "_" as DatasetNamespace
  ]
})
export type DatasetNamespace = typeof DatasetNamespace.Type

/**
 * Represents the name of a dataset.
 */
export const DatasetName = Schema.NonEmptyString.check(
  Schema.isPattern(/^[a-z_][a-z0-9_]*$/)
).pipe(
  Schema.brand("Amp/Models/DatasetName")
).annotate({
  identifier: "DatasetName",
  description: "The name of the dataset. Must start with a lowercase letter or underscore, " +
    "followed by lowercase letters, digits, or underscores.",
  examples: ["uniswap" as DatasetName]
})
export type DatasetName = typeof DatasetName.Type

/**
 * Represents the kind of the dataset.
 *
 * Must be one of `"manifest"`, `"evm-rpc"`, `"eth-beacon"`, or `"firehose"`.
 */
export const DatasetKind = Schema.Literals([
  "manifest",
  "evm-rpc",
  "eth-beacon",
  "firehose"
]).pipe(Schema.brand("Amp/Models/DatasetKind")).annotate({
  identifier: "DatasetKind",
  description: "The kind of the dataset.",
  examples: [
    "manifest" as DatasetKind,
    "evm-rpc" as DatasetKind,
    "eth-beacon" as DatasetKind,
    "firehose" as DatasetKind
  ]
})
export type DatasetKind = typeof DatasetKind.Type

/**
 * Represents the semantic version of the dataset.
 */
export const DatasetVersion = Schema.String.check(
  Schema.isPattern(
    /^(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/
  )
).pipe(
  Schema.brand("Amp/Models/DatasetVersion")
).annotate({
  identifier: "DatasetVersion",
  description: "The semantic version number for the dataset.",
  examples: [
    "1.0.0" as DatasetVersion,
    "1.0.1" as DatasetVersion,
    "1.1.0" as DatasetVersion,
    "1.0.0-dev123" as DatasetVersion,
    "1.0.0+1234567890" as DatasetVersion
  ]
})
export type DatasetVersion = typeof DatasetVersion.Type

/**
 * Represents the 32-byte SHA-256 hash for the dataset.
 */
export const DatasetHash = Schema.String.check(
  Schema.isPattern(/^[0-9a-fA-F]{64}$/)
).pipe(
  Schema.brand("Amp/Models/DatasetHash")
).annotate({
  identifier: "DatasetHash",
  description: "A 32-byte SHA-256 hash (64 characters) for the dataset.",
  examples: ["b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" as DatasetHash]
})
export type DatasetHash = typeof DatasetHash.Type

/**
 * Represents a tag for a dataset version.
 */
export const DatasetTag = Schema.Literals(["latest", "dev"]).pipe(
  Schema.brand("Amp/Models/DatasetTag")
).annotate({
  identifier: "DatasetTag",
  description: "A tag for a dataset version.",
  examples: ["latest" as DatasetTag, "dev" as DatasetTag]
})
export type DatasetTag = typeof DatasetTag.Type

/**
 * Represents a dataset revision reference, which can be either a semver tag,
 * a 64-character hexadecimal hash, `"latest"`, or `"dev"`.
 */
export const DatasetRevision = Schema.Union([
  DatasetVersion,
  DatasetHash,
  DatasetTag
]).annotate({
  identifier: "DatasetRevision",
  description: "A dataset revision reference (semver tag, 64 character hexadecimal hash, \"latest\", or \"dev\").",
  examples: [
    DatasetVersion.make("1.0.0"),
    DatasetHash.make("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"),
    DatasetTag.make("latest"),
    DatasetTag.make("dev")
  ]
})
export type DatasetRevision = typeof DatasetRevision.Type

/**
 * Represents a dataset reference as a string in the format:
 *
 * `<namespace>/<name>@<revision>`
 *
 * The revision can be either a semver version, 64-character hexadecimal hash,
 * `"latest"`, or `"dev"`.
 */
export const DatasetReferenceString = Schema.String.check(
  Schema.isPattern(/^[a-z0-9_]+\/[a-z_][a-z0-9_]*@.+$/)
).annotate({
  identifier: "DatasetReferenceString",
  description: "A dataset reference as a string in the format `<namespace>/<name>@<revision>`, " +
    "where revision is a semver version, hash, \"latest\", or \"dev\"",
  examples: [
    "edgeandnode/mainnet@1.0.0",
    "edgeandnode/mainnet@latest",
    "edgeandnode/mainnet@dev"
  ]
})
export type DatasetReferenceString = typeof DatasetReferenceString.Type

/**
 * Represents a reference to a specific dataset.
 */
export const DatasetReference = Schema.Struct({
  namespace: DatasetNamespace,
  name: DatasetName,
  revision: DatasetRevision
}).annotate({
  identifier: "DatasetReference",
  description: "A reference to a specific dataset."
})
export type DatasetReference = typeof DatasetReference.Type

const decodeDatasetReference = SchemaParser.decodeEffect(DatasetReference)

/**
 * Represents a dataset reference parsed from a string in the format:
 *
 * `<namespace>/<name>@<revision>`
 */
export const DatasetReferenceFromString = Schema.String.pipe(
  Schema.decodeTo(DatasetReference, {
    encode: SchemaGetter.transform((ref) => `${ref.namespace}/${ref.name}@${ref.revision}`),
    decode: SchemaGetter.transformOrFail((str) => {
      const at = str.lastIndexOf("@")
      const slash = str.indexOf("/")

      const namespace = slash === -1 ? "_" : str.substring(0, slash)
      const name = str.substring(slash + 1, at === -1 ? undefined : at)
      const revision = at === -1 ? "dev" : str.substring(at + 1)

      return decodeDatasetReference({
        namespace,
        name,
        revision
      })
    })
  })
).annotate({
  identifier: "DatasetReferenceFromString",
  description: "A dataset reference parsed from a string in the format `<namespace>/<name>@<revision>`."
})
export type DatasetReferenceFromString = typeof DatasetReferenceFromString.Type

/**
 * Represents the name and version of the dataset.
 */
export const DatasetNameAndVersion = Schema.NonEmptyString.check(
  Schema.isPattern(
    /^\w+@(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$/
  )
).annotate({
  identifier: "DatasetNameAndVersion",
  title: "NameAndVersion",
  description: "The name and version of the dataset.",
  examples: ["uniswap@1.0.0", "uniswap@1.0.0+1234567890"]
})
export type DatasetNameAndVersion = typeof DatasetNameAndVersion.Type

/**
 * Represents the address of the dataset repository.
 */
export const DatasetRepository = Schema.URL.annotate({
  identifier: "DatasetRepository",
  title: "Repository",
  description: "The address of the dataset repository.",
  examples: [new URL("https://github.com/foo/bar")]
})
export type DatasetRepository = typeof DatasetRepository.Type

/**
 * Represents the documentation for the dataset.
 */
export const DatasetReadme = Schema.String.annotate({
  identifier: "DatasetReadme",
  title: "Readme",
  description: "The documentation for the dataset."
})
export type DatasetReadme = typeof DatasetReadme.Type

/**
 * Represents additional description and details about the dataset.
 */
export const DatasetDescription = Schema.String.check(
  Schema.isMaxLength(1024)
).annotate({
  identifier: "DatasetDescription",
  title: "Description",
  description: "Additional description and details about the dataset."
})
export type DatasetDescription = typeof DatasetDescription.Type

/**
 * Represents keywords, or traits, about the dataset for discoverability and
 * searching.
 */
export const DatasetKeyword = Schema.String.annotate({
  identifier: "DatasetKeyword",
  title: "Keyword",
  description: "Keywords, or traits, about the dataset for discoverability and searching.",
  examples: ["NFT", "Collectibles", "DeFi", "Transfers"]
})
export type DatasetKeyword = typeof DatasetKeyword.Type

/**
 * Represents the source of the dataset data.
 *
 * For example, this could be the block or logs table that powers the dataset,
 * or the 0x address of the smart contract being queried.
 */
export const DatasetSource = Schema.String.annotate({
  identifier: "DatasetSource",
  title: "Source",
  description: "Source of the dataset data. For example, the block or logs table that powers the " +
    "dataset, or the 0x address of the smart contract being queried.",
  examples: [
    "eth_mainnet_rpc.logs",
    "arbitrum_one_rpc.blocks",
    "0xc944e90c64b2c07662a292be6244bdf05cda44a7"
  ]
})
export type DatasetSource = typeof DatasetSource.Type

/**
 * Represents the license which covers the dataset.
 */
export const DatasetLicense = Schema.String.annotate({
  identifier: "DatasetLicense",
  title: "License",
  description: "License covering the dataset.",
  examples: ["MIT"]
})
export type DatasetLicense = typeof DatasetLicense.Type

/**
 * Represents the visibility of a dataset.
 */
export const DatasetVisibility = Schema.Literals(["public", "private"]).annotate({
  identifier: "DatasetVisibility"
})
export type DatasetVisibility = typeof DatasetVisibility.Type

/**
 * Represents metadata associated with a dataset.
 */
export const DatasetMetadata = Schema.Struct({
  namespace: DatasetNamespace,
  name: DatasetName,
  readme: Schema.optional(DatasetReadme),
  repository: Schema.optional(DatasetRepository),
  description: Schema.optional(DatasetDescription),
  keywords: Schema.optional(Schema.Array(DatasetKeyword)),
  sources: Schema.optional(Schema.Array(DatasetSource)),
  license: Schema.optional(DatasetLicense),
  visibility: Schema.optional(DatasetVisibility)
}).annotate({
  identifier: "DatasetMetadata",
  description: "Metadata associated with a dataset."
})
export type DatasetMetadata = typeof DatasetMetadata.Type

/**
 * Represents the source of a function.
 */
export const FunctionSource = Schema.Struct({
  source: Schema.String,
  filename: Schema.String
}).annotate({
  identifier: "FunctionSource",
  description: "The source of a function."
})
export type FunctionSource = typeof FunctionSource.Type

/**
 * Represents the data required to define of a function.
 */
export const FunctionDefinition = Schema.Struct({
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String
}).annotate({
  identifier: "FunctionDefinition",
  description: "The data required to define of a function."
})
export type FunctionDefinition = typeof FunctionDefinition.Type

/**
 * Represents the data required to define a table.
 */
export const TableDefinition = Schema.Struct({
  sql: Schema.String
}).annotate({
  identifier: "TableDefinition",
  description: "The data required to define a table."
})
export type TableDefinition = typeof TableDefinition.Type

/**
 * Represents configuration associated with a dataset.
 */
export const DatasetConfig = Schema.Struct({
  namespace: DatasetNamespace.pipe(Schema.optional),
  name: DatasetName,
  network: Network,
  readme: Schema.optional(DatasetReadme),
  repository: Schema.optional(DatasetRepository),
  description: Schema.optional(DatasetDescription),
  keywords: Schema.optional(Schema.Array(DatasetKeyword)),
  sources: Schema.optional(Schema.Array(DatasetSource)),
  license: Schema.optional(DatasetLicense),
  private: Schema.optional(Schema.Boolean),
  startBlock: Schema.optional(Schema.Number),
  dependencies: Schema.Record(
    Schema.String,
    DatasetReferenceFromString
  ),
  tables: Schema.optional(Schema.Record(
    Schema.String,
    TableDefinition
  )),
  functions: Schema.optional(Schema.Record(
    Schema.String,
    FunctionDefinition
  ))
}).annotate({
  identifier: "DatasetConfig",
  description: "Configuration associated with a dataset."
})
export type DatasetConfig = typeof DatasetConfig.Type

/**
 * Represents information about a table.
 */
export const TableInfo = Schema.Struct({
  name: Schema.String,
  network: Network,
  activeLocation: Schema.optional(Schema.String)
}).pipe(Schema.encodeKeys({ activeLocation: "active_location" })).annotate({
  identifier: "TableInfo",
  description: "Information about a table."
})
export type TableInfo = typeof TableInfo.Type

/**
 * Represents information about a table schema.
 */
export const TableSchemaInfo = Schema.Struct({
  name: Schema.String,
  network: Network,
  schema: Schema.Record(Schema.String, Schema.Any)
}).annotate({
  identifier: "TableSchemaInfo",
  description: "Information about a table schema."
})
export type TableSchemaInfo = typeof TableSchemaInfo.Type

/**
 * Represents information about a dataset.
 */
export const DatasetInfo = Schema.Struct({
  name: DatasetName,
  kind: DatasetKind,
  tables: Schema.Array(TableInfo)
}).annotate({
  identifier: "DatasetInfo",
  description: "Information about a dataset."
})
export type DatasetInfo = typeof DatasetInfo.Type

/**
 * Represents information about a field within an Apache Arrow schema.
 */
export const ArrowField = Schema.Struct({
  name: Schema.String,
  type: Schema.Any,
  nullable: Schema.Boolean
}).annotate({
  identifier: "ArrowField",
  description: "Information about a field within an Apache Arrow schema."
})
export type ArrowField = typeof ArrowField.Type

/**
 * Represents an Apache Arrow schema.
 */
export const ArrowSchema = Schema.Struct({
  fields: Schema.Array(ArrowField)
}).annotate({
  identifier: "ArrowSchema",
  description: "An Apache Arrow schema."
})
export type ArrowSchema = typeof ArrowSchema.Type

/**
 * Represents a table schema.
 */
export const TableSchema = Schema.Struct({
  arrow: ArrowSchema
}).annotate({
  identifier: "TableSchema",
  description: "A table schema."
})
export type TableSchema = typeof TableSchema.Type

/**
 * Represents a table schema with associated networks.
 */
export const TableSchemaWithNetworks = Schema.Struct({
  schema: TableSchema,
  networks: Schema.Array(Schema.String)
}).annotate({
  identifier: "TableSchemaWithNetworks",
  description: "A table schema with associated networks."
})
export type TableSchemaWithNetworks = typeof TableSchemaWithNetworks.Type

/**
 * Represents the input SQL for a table.
 */
export const TableInput = Schema.Struct({
  sql: Schema.String
}).annotate({
  identifier: "TableInput",
  description: "Input SQL for a table."
})
export type TableInput = typeof TableInput.Type

/**
 * Represents a table.
 */
export const Table = Schema.Struct({
  input: TableInput,
  schema: TableSchema,
  network: Network
}).annotate({
  identifier: "Table",
  description: "A table."
})
export type Table = typeof Table.Type

/**
 * Represents a table for a raw dataset.
 */
export const RawDatasetTable = Schema.Struct({
  schema: TableSchema,
  network: Network
}).annotate({
  identifier: "RawDatasetTable",
  description: "A table for a raw dataset."
})
export type RawDatasetTable = typeof RawDatasetTable.Type

/**
 * Represents the output schema for a query.
 */
export const OutputSchema = Schema.Struct({
  schema: TableSchema,
  networks: Schema.Array(Schema.String)
}).annotate({
  identifier: "OutputSchema",
  description: "The output schema for a query."
})
export type OutputSchema = typeof OutputSchema.Type

/**
 * Represents information associated with a function.
 */
export const FunctionManifest = Schema.Struct({
  name: Schema.String,
  source: FunctionSource,
  inputTypes: Schema.Array(Schema.String),
  outputType: Schema.String
}).annotate({
  identifier: "FunctionManifest",
  description: "Information associated with a function."
})
export type FunctionManifest = typeof FunctionManifest.Type

/**
 * Represents a SQL-based derived dataset.
 */
export const DatasetDerived = Schema.Struct({
  kind: Schema.Literal("manifest"),
  startBlock: Schema.optional(Schema.NullOr(Schema.Number)),
  dependencies: Schema.Record(Schema.String, DatasetReferenceFromString),
  tables: Schema.Record(Schema.String, Table),
  functions: Schema.Record(Schema.String, FunctionManifest)
}).pipe(Schema.encodeKeys({ startBlock: "start_block" })).annotate({
  identifier: "DatasetDerived",
  description: "A SQL-based derived datasets."
})
export type DatasetDerived = typeof DatasetDerived.Type

/**
 * Represents an EVM RPC extraction dataset.
 */
export const DatasetEvmRpc = Schema.Struct({
  kind: Schema.Literal("evm-rpc"),
  network: Network,
  startBlock: Schema.optional(Schema.Number),
  finalizedBlocksOnly: Schema.optional(Schema.Boolean),
  tables: Schema.Record(Schema.String, RawDatasetTable)
}).pipe(Schema.encodeKeys({
  startBlock: "start_block",
  finalizedBlocksOnly: "finalized_blocks_only"
})).annotate({
  identifier: "DatasetEvmRpc",
  description: "An EVM RPC extraction dataset."
})
export type DatasetEvmRpc = typeof DatasetEvmRpc.Type

/**
 * Represents an ETH beacon extraction dataset.
 */
export const DatasetEthBeacon = Schema.Struct({
  kind: Schema.Literal("eth-beacon"),
  network: Network,
  startBlock: Schema.optional(Schema.Number),
  finalizedBlocksOnly: Schema.optional(Schema.Boolean),
  tables: Schema.Record(Schema.String, RawDatasetTable)
}).pipe(Schema.encodeKeys({
  startBlock: "start_block",
  finalizedBlocksOnly: "finalized_blocks_only"
})).annotate({
  identifier: "DatasetEthBeacon",
  description: "An ETH beacon extraction dataset."
})
export type DatasetEthBeacon = typeof DatasetEthBeacon.Type

/**
 * Represents a Firehose extraction dataset.
 */
export const DatasetFirehose = Schema.Struct({
  kind: Schema.Literal("firehose"),
  network: Network,
  startBlock: Schema.optional(Schema.Number),
  finalizedBlocksOnly: Schema.optional(Schema.Boolean),
  tables: Schema.Record(Schema.String, RawDatasetTable)
}).pipe(Schema.encodeKeys({
  startBlock: "start_block",
  finalizedBlocksOnly: "finalized_blocks_only"
})).annotate({
  identifier: "DatasetFirehose",
  description: "A Firehose extraction dataset."
})
export type DatasetFirehose = typeof DatasetFirehose.Type

/**
 * Union type representing any dataset manifest kind.
 *
 * This type is used at API boundaries (registration, storage, retrieval).
 * Metadata (namespace, name, version) is passed separately to the API.
 *
 * Supported kinds:
 * - DatasetDerived (kind: "manifest") - SQL-based derived datasets
 * - DatasetEvmRpc (kind: "evm-rpc") - EVM RPC extraction datasets
 * - DatasetEthBeacon (kind: "eth-beacon") - ETH beacon extraction datasets
 * - DatasetFirehose (kind: "firehose") - Firehose extraction datasets
 */
export const DatasetManifest = Schema.Union([
  DatasetDerived,
  DatasetEvmRpc,
  DatasetEthBeacon,
  DatasetFirehose
])
export type DatasetManifest = typeof DatasetManifest.Type

/**
 * Represents the unique identifier for a job.
 */
export const JobId = Schema.Number.pipe(
  Schema.brand("Amp/Models/JobId")
).annotate({
  identifier: "JobId",
  description: "The unique identifier for a job."
})
export type JobId = typeof JobId.Type

/**
 * Represents the status of a job.
 */
export const JobStatus = Schema.Literals([
  "SCHEDULED",
  "RUNNING",
  "COMPLETED",
  "STOPPED",
  "STOP_REQUESTED",
  "STOPPING",
  "FAILED",
  "UNKNOWN"
]).pipe(
  Schema.brand("Amp/Models/JobStatus")
).annotate({
  identifier: "JobStatus",
  description: "The status of a job."
})
export type JobStatus = typeof JobStatus.Type

/**
 * Represents information about a job.
 */
export const JobInfo = Schema.Struct({
  id: JobId,
  status: JobStatus,
  createdAt: Schema.DateTimeUtc,
  updatedAt: Schema.DateTimeUtc,
  nodeId: Schema.String,
  descriptor: Schema.Any
}).pipe(Schema.encodeKeys({
  createdAt: "created_at",
  updatedAt: "updated_at",
  nodeId: "node_id"
})).annotate({
  identifier: "JobInfo",
  description: "Information about a job."
})
export type JobInfo = typeof JobInfo.Type
