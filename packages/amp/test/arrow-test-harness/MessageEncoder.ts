/**
 * Message Encoder for Arrow Test Harness
 * @internal
 */
import {
  type ArrowDataType,
  ArrowDataTypeEnum,
  type ArrowField,
  type ArrowSchema,
  DateUnit,
  IntervalUnit,
  MessageHeaderType,
  Precision,
  TimeUnit,
  UnionMode
} from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import { FlatBufferWriter } from "./FlatBufferWriter.ts"

const METADATA_VERSION = 4

// =============================================================================
// Schema Message
// =============================================================================

export const encodeSchemaMessage = (schema: ArrowSchema): Uint8Array => {
  const writer = new FlatBufferWriter()

  const fieldOffsets = schema.fields.map((field) => encodeField(writer, field))
  const fieldsVectorOffset = writer.writeOffsetVector(fieldOffsets)

  let metadataOffset: number | null = null
  if (schema.metadata.size > 0) {
    metadataOffset = encodeKeyValueVector(writer, schema.metadata)
  }

  const schemaBuilder = writer.startTable()
  schemaBuilder.addInt16(0, schema.endianness)
  schemaBuilder.addOffset(1, fieldsVectorOffset)
  if (metadataOffset !== null) schemaBuilder.addOffset(2, metadataOffset)
  const schemaOffset = writer.finishTable(schemaBuilder)

  const messageBuilder = writer.startTable()
  messageBuilder.addInt16(0, METADATA_VERSION)
  messageBuilder.addUint8(1, MessageHeaderType.SCHEMA)
  messageBuilder.addOffset(2, schemaOffset)
  messageBuilder.addInt64(3, 0n)
  const messageOffset = writer.finishTable(messageBuilder)

  return writer.finish(messageOffset)
}

// =============================================================================
// RecordBatch Message
// =============================================================================

export interface FieldNodeData {
  readonly length: bigint
  readonly nullCount: bigint
}

export interface BufferData {
  readonly offset: bigint
  readonly length: bigint
}

export const encodeRecordBatchMessage = (
  numRows: bigint,
  fieldNodes: ReadonlyArray<FieldNodeData>,
  buffers: ReadonlyArray<BufferData>,
  bodyLength: bigint
): Uint8Array => {
  const writer = new FlatBufferWriter()

  // FieldNode structs (16 bytes: length i64 + nullCount i64)
  let nodesOffset: number | null = null
  if (fieldNodes.length > 0) {
    const nodesData = new Uint8Array(fieldNodes.length * 16)
    const nodesView = new DataView(nodesData.buffer)
    fieldNodes.forEach((node, i) => {
      nodesView.setBigInt64(i * 16, node.length, true)
      nodesView.setBigInt64(i * 16 + 8, node.nullCount, true)
    })
    nodesOffset = writer.writeStructVector(nodesData, 16)
  }

  // Buffer structs (16 bytes: offset i64 + length i64)
  let buffersOffset: number | null = null
  if (buffers.length > 0) {
    const buffersData = new Uint8Array(buffers.length * 16)
    const buffersView = new DataView(buffersData.buffer)
    buffers.forEach((buf, i) => {
      buffersView.setBigInt64(i * 16, buf.offset, true)
      buffersView.setBigInt64(i * 16 + 8, buf.length, true)
    })
    buffersOffset = writer.writeStructVector(buffersData, 16)
  }

  const rbBuilder = writer.startTable()
  rbBuilder.addInt64(0, numRows)
  if (nodesOffset !== null) rbBuilder.addOffset(1, nodesOffset)
  if (buffersOffset !== null) rbBuilder.addOffset(2, buffersOffset)
  const rbOffset = writer.finishTable(rbBuilder)

  const messageBuilder = writer.startTable()
  messageBuilder.addInt16(0, METADATA_VERSION)
  messageBuilder.addUint8(1, MessageHeaderType.RECORD_BATCH)
  messageBuilder.addOffset(2, rbOffset)
  messageBuilder.addInt64(3, bodyLength)
  const messageOffset = writer.finishTable(messageBuilder)

  return writer.finish(messageOffset)
}

// =============================================================================
// Field Encoding
// =============================================================================

const encodeField = (writer: FlatBufferWriter, field: ArrowField): number => {
  const childOffsets = field.children.map((child) => encodeField(writer, child))
  const childrenVectorOffset = childOffsets.length > 0 ? writer.writeOffsetVector(childOffsets) : null

  const nameOffset = writer.writeString(field.name)
  const [typeEnumValue, typeOffset] = encodeType(writer, field.type)

  let metadataOffset: number | null = null
  if (field.metadata.size > 0) {
    metadataOffset = encodeKeyValueVector(writer, field.metadata)
  }

  const fieldBuilder = writer.startTable()
  fieldBuilder.addOffset(0, nameOffset)
  fieldBuilder.addBool(1, field.nullable)
  fieldBuilder.addUint8(2, typeEnumValue)
  if (typeOffset !== null) fieldBuilder.addOffset(3, typeOffset)
  if (childrenVectorOffset !== null) fieldBuilder.addOffset(5, childrenVectorOffset)
  if (metadataOffset !== null) fieldBuilder.addOffset(6, metadataOffset)

  return writer.finishTable(fieldBuilder)
}

// =============================================================================
// Type Encoding
// =============================================================================

const PRECISION_MAP: Record<string, Precision> = {
  HALF: Precision.HALF,
  SINGLE: Precision.SINGLE,
  DOUBLE: Precision.DOUBLE
}
const DATE_UNIT_MAP: Record<string, DateUnit> = { DAY: DateUnit.DAY, MILLISECOND: DateUnit.MILLISECOND }
const TIME_UNIT_MAP: Record<string, TimeUnit> = {
  SECOND: TimeUnit.SECOND,
  MILLISECOND: TimeUnit.MILLISECOND,
  MICROSECOND: TimeUnit.MICROSECOND,
  NANOSECOND: TimeUnit.NANOSECOND
}
const INTERVAL_UNIT_MAP: Record<string, IntervalUnit> = {
  YEAR_MONTH: IntervalUnit.YEAR_MONTH,
  DAY_TIME: IntervalUnit.DAY_TIME,
  MONTH_DAY_NANO: IntervalUnit.MONTH_DAY_NANO
}
const UNION_MODE_MAP: Record<string, UnionMode> = { SPARSE: UnionMode.SPARSE, DENSE: UnionMode.DENSE }

const encodeType = (writer: FlatBufferWriter, type: ArrowDataType): [ArrowDataTypeEnum, number | null] => {
  const emptyTable = () => writer.finishTable(writer.startTable())

  switch (type.typeId) {
    case "null":
      return [ArrowDataTypeEnum.NULL, emptyTable()]
    case "bool":
      return [ArrowDataTypeEnum.BOOL, emptyTable()]
    case "binary":
      return [ArrowDataTypeEnum.BINARY, emptyTable()]
    case "large-binary":
      return [ArrowDataTypeEnum.LARGE_BINARY, emptyTable()]
    case "utf8":
      return [ArrowDataTypeEnum.UTF8, emptyTable()]
    case "large-utf8":
      return [ArrowDataTypeEnum.LARGE_UTF8, emptyTable()]
    case "list":
      return [ArrowDataTypeEnum.LIST, emptyTable()]
    case "large-list":
      return [ArrowDataTypeEnum.LARGE_LIST, emptyTable()]
    case "struct":
      return [ArrowDataTypeEnum.STRUCT, emptyTable()]

    case "int": {
      const b = writer.startTable()
      b.addInt32(0, type.bitWidth)
      b.addBool(1, type.isSigned)
      return [ArrowDataTypeEnum.INT, writer.finishTable(b)]
    }

    case "float": {
      const b = writer.startTable()
      b.addInt16(0, PRECISION_MAP[type.precision])
      return [ArrowDataTypeEnum.FLOATING_POINT, writer.finishTable(b)]
    }

    case "decimal": {
      const b = writer.startTable()
      b.addInt32(0, type.precision)
      b.addInt32(1, type.scale)
      b.addInt32(2, type.bitWidth)
      return [ArrowDataTypeEnum.DECIMAL, writer.finishTable(b)]
    }

    case "fixed-size-binary": {
      const b = writer.startTable()
      b.addInt32(0, type.byteWidth)
      return [ArrowDataTypeEnum.FIXED_SIZE_BINARY, writer.finishTable(b)]
    }

    case "date": {
      const b = writer.startTable()
      b.addInt16(0, DATE_UNIT_MAP[type.unit])
      return [ArrowDataTypeEnum.DATE, writer.finishTable(b)]
    }

    case "time": {
      const b = writer.startTable()
      b.addInt16(0, TIME_UNIT_MAP[type.unit])
      b.addInt32(1, type.bitWidth)
      return [ArrowDataTypeEnum.TIME, writer.finishTable(b)]
    }

    case "timestamp": {
      let tzOffset: number | null = null
      if (type.timezone !== null) tzOffset = writer.writeString(type.timezone)
      const b = writer.startTable()
      b.addInt16(0, TIME_UNIT_MAP[type.unit])
      if (tzOffset !== null) b.addOffset(1, tzOffset)
      return [ArrowDataTypeEnum.TIMESTAMP, writer.finishTable(b)]
    }

    case "interval": {
      const b = writer.startTable()
      b.addInt16(0, INTERVAL_UNIT_MAP[type.unit])
      return [ArrowDataTypeEnum.INTERVAL, writer.finishTable(b)]
    }

    case "duration": {
      const b = writer.startTable()
      b.addInt16(0, TIME_UNIT_MAP[type.unit])
      return [ArrowDataTypeEnum.DURATION, writer.finishTable(b)]
    }

    case "fixed-size-list": {
      const b = writer.startTable()
      b.addInt32(0, type.listSize)
      return [ArrowDataTypeEnum.FIXED_SIZE_LIST, writer.finishTable(b)]
    }

    case "map": {
      const b = writer.startTable()
      b.addBool(0, type.keysSorted)
      return [ArrowDataTypeEnum.MAP, writer.finishTable(b)]
    }

    case "union": {
      // Write typeIds as int32 vector: [length: u32] [id0: i32] [id1: i32] ...
      const typeIdsData = new Uint8Array(type.typeIds.length * 4)
      const typeIdsView = new DataView(typeIdsData.buffer)
      type.typeIds.forEach((id, i) => typeIdsView.setInt32(i * 4, id, true))
      const typeIdsOffset = writer.writeStructVector(typeIdsData, 4)
      const b = writer.startTable()
      b.addInt16(0, UNION_MODE_MAP[type.mode])
      b.addOffset(1, typeIdsOffset)
      return [ArrowDataTypeEnum.UNION, writer.finishTable(b)]
    }
  }
}

// =============================================================================
// Metadata Encoding
// =============================================================================

const encodeKeyValueVector = (writer: FlatBufferWriter, metadata: ReadonlyMap<string, string>): number => {
  const kvOffsets: Array<number> = []
  for (const [key, value] of metadata.entries()) {
    const keyOffset = writer.writeString(key)
    const valueOffset = writer.writeString(value)
    const kvBuilder = writer.startTable()
    kvBuilder.addOffset(0, keyOffset)
    kvBuilder.addOffset(1, valueOffset)
    kvOffsets.push(writer.finishTable(kvBuilder))
  }
  return writer.writeOffsetVector(kvOffsets)
}
