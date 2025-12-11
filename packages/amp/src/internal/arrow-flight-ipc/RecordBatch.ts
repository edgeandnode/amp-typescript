/**
 * Arrow RecordBatch Types and Parser
 *
 * This module provides types and parsing utilities for Arrow IPC RecordBatch messages.
 * RecordBatch messages contain the metadata describing how to interpret
 * the binary body data (buffers and their layouts).
 *
 * References:
 * - RecordBatch.fbs: https://github.com/apache/arrow/blob/main/format/Message.fbs
 * - IPC Format: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 *
 * @internal
 */
import * as Effect from "effect/Effect"
import * as Predicate from "effect/Predicate"
import { MissingFieldError, UnexpectedMessageTypeError } from "./Errors.ts"
import { FlatBufferReader } from "./FlatBufferReader.ts"
import {
  type ArrowDataType,
  type ArrowField,
  type ArrowSchema,
  type FlightData,
  getMessageType,
  MessageHeaderType
} from "./Schema.ts"

// =============================================================================
// Constants
// =============================================================================

export const BufferType = {
  /** Validity bitmap (null / non-null) */
  VALIDITY: 0,
  /** Offset buffer for variable-length types */
  OFFSET: 1,
  /** Large offset offer (int64) for large types */
  LARGE_OFFSET: 2,
  /** Data buffer containing actual values */
  DATA: 3,
  /** Type IDs for union types */
  TYPE_ID: 4
} as const
export type BufferType = typeof BufferType[keyof typeof BufferType]

// =============================================================================
// Record Batch Types
// =============================================================================

/**
 * Represents a parsed `RecordBatch` message.
 *
 * The `RecordBatch` contains metadata about how to interpret the body:
 * - `nodes`: One `FieldNode` per field (including nested), describing validity/length
 * - `buffers`: Buffer locations within the body for all field data
 * - `compression`: Optional compression info
 */
export class RecordBatch {
  /**
   * The number of rows in this batch.
   */
  readonly length: bigint

  /**
   * One field node per field in depth-first order. For nested types, the
   * parent comes before its children.
   */
  readonly nodes: ReadonlyArray<FieldNode>
  /**
   * The location of all buffers in the message body, the order of which matches
   * the flattened schema (depth-first).
   */
  readonly buffers: ReadonlyArray<BufferDescriptor>

  /**
   * Optional compression information.
   */
  readonly compression: BodyCompression | undefined

  constructor(
    length: bigint,
    nodes: ReadonlyArray<FieldNode>,
    buffers: ReadonlyArray<BufferDescriptor>,
    compression?: BodyCompression | undefined
  ) {
    this.length = length
    this.nodes = nodes
    this.buffers = buffers
    this.compression = compression
  }
}

/**
 * Metadata about a single field's data in the `RecordBatch`. There is one
 * `FieldNode` per field (including nested children).
 */
export class FieldNode {
  /**
   * The number of values in this field (may differ from batch length for nested
   * fields).
   */
  readonly length: bigint

  /**
   * The number of null values.
   */
  readonly nullCount: bigint

  constructor(length: bigint, nullCount: bigint) {
    this.length = length
    this.nullCount = nullCount
  }
}

/**
 * Describes location of a buffer within the message body. Buffers are stored
 * contiguously in the body, aligned to 8-byte chunks.
 */
export class BufferDescriptor {
  /**
   * The offset of the buffer from the start of the message body.
   */
  readonly offset: bigint

  /**
   * The length of the buffer in bytes.
   */
  readonly length: bigint

  constructor(offset: bigint, length: bigint) {
    this.offset = offset
    this.length = length
  }
}

export const CompressionCodec = {
  LZ4_FRAME: 0,
  ZSTD: 1
} as const
export type CompressionCodec = typeof CompressionCodec[keyof typeof CompressionCodec]

export const BodyCompressionMethod = {
  /**
   * Indicates that each buffer is compressed individually.
   */
  BUFFER: 0
} as const
export type BodyCompressionMethod = typeof BodyCompressionMethod[keyof typeof BodyCompressionMethod]

export class BodyCompression {
  /**
   * The compression codec that was used.
   */
  readonly codec: CompressionCodec

  /**
   * The method that was used for compressing buffers.
   */
  readonly method: BodyCompressionMethod

  constructor(codec: CompressionCodec, method: BodyCompressionMethod) {
    this.codec = codec
    this.method = method
  }
}

// =============================================================================
// Buffer Types
// =============================================================================

/**
 * Calculates the expected buffer layout for a schema. This is needed to
 * correctly interpret the buffers in a RecordBatch.
 */
export class BufferLayout {
  /**
   * The field that this buffer belongs to.
   */
  readonly field: ArrowField

  /**
   * The buffer type (validity, offset, etc).
   */
  readonly bufferType: BufferType

  /**
   * The index of this buffer in the `RecordBatch.buffers` array.
   */
  readonly bufferIndex: number

  constructor(field: ArrowField, bufferType: BufferType, bufferIndex: number) {
    this.field = field
    this.bufferType = bufferType
    this.bufferIndex = bufferIndex
  }
}

/**
 * Get the buffer types required for a given Arrow data type. The buffer order
 * matches the Arrow IPC specification.
 */
export const getBufferTypesForType = (type: ArrowDataType): ReadonlyArray<BufferType> => {
  switch (type.typeId) {
    // The null type has no buffers
    case "null": {
      return []
    }

    // Fixed-width types: validity + data
    case "bool":
    case "int":
    case "float":
    case "decimal":
    case "date":
    case "time":
    case "timestamp":
    case "interval":
    case "duration":
    case "fixed-size-binary": {
      return [BufferType.VALIDITY, BufferType.DATA]
    }

    // Variable-length types: validity + offsets + data
    case "binary":
    case "utf8": {
      return [BufferType.VALIDITY, BufferType.OFFSET, BufferType.DATA]
    }

    // Large variable-length types: validity + large offsets + data
    case "large-binary":
    case "large-utf8": {
      return [BufferType.VALIDITY, BufferType.LARGE_OFFSET, BufferType.DATA]
    }

    // List: validity + offsets (children handled separately)
    case "list": {
      return [BufferType.VALIDITY, BufferType.OFFSET]
    }

    // Large list: validity + large offsets (children handled separately)
    case "large-list": {
      return [BufferType.VALIDITY, BufferType.LARGE_OFFSET]
    }

    // Fixed-size lists: validity only (no offset information needed)
    case "fixed-size-list": {
      return [BufferType.VALIDITY]
    }

    // Struct: validity only (children handled separately)
    case "struct": {
      return [BufferType.VALIDITY]
    }

    // Map: validity + offsets (children handled separately)
    case "map": {
      return [BufferType.VALIDITY, BufferType.OFFSET]
    }

    case "union": {
      if (type.mode === "SPARSE") {
        // Sparse union: type IDs only
        return [BufferType.TYPE_ID]
      } else {
        // Dense union: type IDs + offsets
        return [BufferType.TYPE_ID, BufferType.OFFSET]
      }
    }
  }
}

// =============================================================================
// Decoded Record Batch Types
// =============================================================================

/**
 * A fully decoded Arrow `RecordBatch`.
 */
export class DecodedRecordBatch {
  /**
   * The schema for this batch.
   */
  readonly schema: ArrowSchema

  /**
   * The number of rows in this batch.
   */
  readonly numRows: bigint

  /**
   * The columns in this batch.
   */
  readonly columns: ReadonlyArray<DecodedColumn>

  constructor(
    schema: ArrowSchema,
    numRows: bigint,
    columns: ReadonlyArray<DecodedColumn>
  ) {
    this.schema = schema
    this.numRows = numRows
    this.columns = columns
  }

  /**
   * Retrieve a column by name.
   */
  getColumn(name: string): DecodedColumn | undefined {
    return this.columns.find((column) => column.field.name === name)
  }

  /**
   * Retrieve a column by index.
   */
  getColumnAt(index: number): DecodedColumn | undefined {
    return this.columns[index]
  }
}

/**
 * Represents a decoded column with its data buffers.
 */
export class DecodedColumn {
  /**
   * The schema field definition.
   */
  readonly field: ArrowField

  /**
   * The field node with length / null count.
   */
  readonly node: FieldNode

  /**
   * The raw data buffers for this column.
   *
   * The layout of the buffers depends on the type:
   *   - Primitive: [validity, data]
   *   - Variable-length (utf8, binary): [validity, offsets, data]
   *   - List: [validity, offsets] + child buffers
   *   - Struct: [validity] + child buffers
   *   - etc.
   */
  readonly buffers: ReadonlyArray<Uint8Array>

  /**
   * Child columns for nested data types.
   */
  readonly children: ReadonlyArray<DecodedColumn>

  constructor(
    field: ArrowField,
    node: FieldNode,
    buffers: ReadonlyArray<Uint8Array>,
    children: ReadonlyArray<DecodedColumn>
  ) {
    this.field = field
    this.node = node
    this.buffers = buffers
    this.children = children
  }
}

// =============================================================================
// Parsers
// =============================================================================

/**
 * Parse an Arrow RecordBatch from the raw IPC message header bytes (FlatBuffer)
 * of a `FlightData` message.
 *
 * Message table structure (from https://github.com/apache/arrow/blob/main/format/Message.fbs):
 *   version: MetadataVersion (Int16 enum)
 *   header: MessageHeader (union)
 *   bodyLength: long
 *   custom_metadata: [KeyValue]
 *
 * In FlatBuffers, a union field generates TWO vtable entries:
 *   - The type discriminator (UInt8)
 *   - The offset to the union value
 *
 * So the vtable field indices are:
 *   0: version (Int16)
 *   1: header_type (UInt8 - union type discriminator)
 *   2: header (offset to union table)
 *   3: bodyLength (Int64)
 *   4: custom_metadata (vector offset)
 */
export const parseRecordBatch = Effect.fn(function*(flightData: FlightData) {
  const reader = new FlatBufferReader(flightData.dataHeader)

  // The flatbuffer root table offset is at position 0
  const rootOffset = reader.readOffset(0)

  // Read the position of the message header union type discriminator
  const headerTypePosition = reader.getFieldPosition(rootOffset, 1)
  if (Predicate.isNull(headerTypePosition)) {
    return yield* new MissingFieldError({
      fieldName: "header_type",
      fieldIndex: 1,
      tableOffset: rootOffset
    })
  }

  // Read the actual message header union type discriminator
  const headerType = reader.readUint8(headerTypePosition)
  if (headerType !== MessageHeaderType.RECORD_BATCH) {
    return yield* new UnexpectedMessageTypeError({
      expected: MessageHeaderType.RECORD_BATCH,
      received: headerType
    })
  }

  // Read the union value offset (field index 2)
  const headerPosition = reader.getFieldPosition(rootOffset, 2)
  if (Predicate.isNull(headerPosition)) {
    return yield* new MissingFieldError({
      fieldName: "header",
      fieldIndex: 2,
      tableOffset: rootOffset
    })
  }

  // Read the offset position of the schema relative to the header position
  const recordBatchOffset = reader.readOffset(headerPosition)

  return yield* parseRecordBatchTable(reader, recordBatchOffset)
})

/**
 * Parses a RecordBatch table.
 *
 * The structure of a RecordBatch table is as follows:
 *   0: length (Int64) - number of rows
 *   1: nodes ([FieldNode]) - one per field in DFS order
 *   2: buffers ([Buffer]) - buffer locations
 *   3: compression (BodyCompression) - optional
 */
export const parseRecordBatchTable = Effect.fn(function*(reader: FlatBufferReader, offset: number) {
  // Parse length
  const lengthPosition = reader.getFieldPosition(offset, 0)
  const length = Predicate.isNotNull(lengthPosition) ? reader.readInt64(lengthPosition) : 0n

  // Parse nodes vector
  const nodes: Array<FieldNode> = []
  const nodesPos = reader.getFieldPosition(offset, 1)
  if (Predicate.isNotNull(nodesPos)) {
    const nodesVectorOffset = reader.readOffset(nodesPos)
    const nodeCount = reader.readVectorLength(nodesVectorOffset)

    // FieldNode is a struct (inline in vector), not a table
    // Each FieldNode is 16 bytes: length (Int64) + null_count (Int64)
    const FIELD_NODE_SIZE = 16

    for (let i = 0; i < nodeCount; i++) {
      const nodeOffset = nodesVectorOffset + 4 + i * FIELD_NODE_SIZE
      const length = reader.readInt64(nodeOffset)
      const nullCount = reader.readInt64(nodeOffset + 8)
      nodes.push(new FieldNode(length, nullCount))
    }
  }

  // Parse buffers vector
  const buffers: Array<BufferDescriptor> = []
  const buffersPosition = reader.getFieldPosition(offset, 2)
  if (Predicate.isNotNull(buffersPosition)) {
    const buffersVectorOffset = reader.readOffset(buffersPosition)
    const numBuffers = reader.readVectorLength(buffersVectorOffset)

    // Buffer is a struct (inline in vector), not a table
    // Each Buffer is 16 bytes: offset (Int64) + length (Int64)
    const BUFFER_SIZE = 16

    for (let i = 0; i < numBuffers; i++) {
      const bufferOffset = buffersVectorOffset + 4 + i * BUFFER_SIZE
      const offset = reader.readInt64(bufferOffset)
      const length = reader.readInt64(bufferOffset + 8)
      buffers.push(new BufferDescriptor(offset, length))
    }
  }

  // Parse optional compression
  let compression: BodyCompression | undefined
  const compressionPosition = reader.getFieldPosition(offset, 3)
  if (Predicate.isNotNull(compressionPosition)) {
    const compressionOffset = reader.readOffset(compressionPosition)
    compression = parseBodyCompression(reader, compressionOffset)
  }

  return new RecordBatch(length, nodes, buffers, compression)
})

/**
 * Parses the `BodyCompression` table.
 *
 * The structure of the BodyCompression table is as follows:
 *   0: codec (CompressionType enum, Int8)
 *   1: method (BodyCompressionMethod enum, Int8)
 */
const parseBodyCompression = (reader: FlatBufferReader, offset: number): BodyCompression => {
  const codecPosition = reader.getFieldPosition(offset, 0)
  const codec = Predicate.isNotNull(codecPosition)
    ? reader.readInt8(codecPosition) as CompressionCodec
    : CompressionCodec.LZ4_FRAME

  const methodPosition = reader.getFieldPosition(offset, 1)
  const method = Predicate.isNotNull(methodPosition)
    ? reader.readInt8(methodPosition) as BodyCompressionMethod
    : BodyCompressionMethod.BUFFER

  return new BodyCompression(codec, method)
}

// =============================================================================
// Utilities
// =============================================================================

/**
 * Returns `true` if the provided `FlightData` header data buffer contains a
 * record batch message, otherwise returns `false`.
 */
export const isRecordBatchMessage = Effect.fn(function*(flightData: FlightData) {
  const messageType = yield* getMessageType(flightData)
  return messageType === MessageHeaderType.RECORD_BATCH
})
