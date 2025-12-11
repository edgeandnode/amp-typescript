/**
 * Pure TypeScript Arrow RecordBatch Parser for Flight Data
 *
 * Parses Arrow IPC RecordBatch messages directly from FlatBuffer format.
 * RecordBatch messages contain the metadata describing how to interpret
 * the binary body data (buffers and their layouts).
 *
 * References:
 * - RecordBatch.fbs: https://github.com/apache/arrow/blob/main/format/Message.fbs
 * - IPC Format: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 */
import * as Effect from "effect/Effect"
import * as Predicate from "effect/Predicate"
import { MissingFieldError, UnexpectedMessageTypeError } from "../core/errors.ts"
import { FlatBufferReader } from "../core/flatbuffer-reader.ts"
import type { FlightData } from "../core/types.ts"
import { getMessageType, MessageHeaderType } from "../core/types.ts"
import {
  BodyCompression,
  BodyCompressionMethod,
  BufferDescriptor,
  CompressionCodec,
  FieldNode,
  RecordBatch
} from "./types.ts"

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
