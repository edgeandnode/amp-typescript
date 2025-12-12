/**
 * Arrow Schema Types and Parser
 *
 * This module provides types and parsing utilities for Arrow IPC schema messages.
 *
 * References:
 * - Arrow IPC Format: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
 * - FlatBuffer encoding: https://flatbuffers.dev/flatbuffers_internals.html
 *
 * @internal
 */

import * as Effect from "effect/Effect"
import * as Predicate from "effect/Predicate"
import {
  InvalidArrowDataTypeError,
  InvalidMessageTypeError,
  MissingFieldError,
  UnexpectedMessageTypeError
} from "./Errors.ts"
import { FlatBufferReader } from "./FlatBufferReader.ts"

// =============================================================================
// Message Types
// =============================================================================

export const MessageHeaderType = {
  NONE: 0,
  SCHEMA: 1,
  DICTIONARY_BATCH: 2,
  RECORD_BATCH: 3,
  TENSOR: 4,
  SPARSE_TENSOR: 5
} as const
export type MessageHeaderType = typeof MessageHeaderType[keyof typeof MessageHeaderType]

/**
 * A partial representation of the `FlightData` type.
 */
export interface FlightData {
  readonly dataHeader: Uint8Array
  readonly dataBody: Uint8Array
}

// =============================================================================
// Arrow Data Type Enums
// =============================================================================

export const ArrowDataTypeEnum = {
  NONE: 0,
  NULL: 1,
  INT: 2,
  FLOATING_POINT: 3,
  BINARY: 4,
  UTF8: 5,
  BOOL: 6,
  DECIMAL: 7,
  DATE: 8,
  TIME: 9,
  TIMESTAMP: 10,
  INTERVAL: 11,
  LIST: 12,
  STRUCT: 13,
  UNION: 14,
  FIXED_SIZE_BINARY: 15,
  FIXED_SIZE_LIST: 16,
  MAP: 17,
  DURATION: 18,
  LARGE_BINARY: 19,
  LARGE_UTF8: 20,
  LARGE_LIST: 21
} as const
export type ArrowDataTypeEnum = typeof ArrowDataTypeEnum[keyof typeof ArrowDataTypeEnum]

export const DateUnit = {
  DAY: 0,
  MILLISECOND: 1
} as const
export type DateUnit = typeof DateUnit[keyof typeof DateUnit]

export const Endianness = {
  LITTLE: 0,
  BIG: 1
} as const
export type Endianness = typeof Endianness[keyof typeof Endianness]

export const IntervalUnit = {
  YEAR_MONTH: 0,
  DAY_TIME: 1,
  MONTH_DAY_NANO: 2
} as const
export type IntervalUnit = typeof IntervalUnit[keyof typeof IntervalUnit]

export const Precision = {
  HALF: 0,
  SINGLE: 1,
  DOUBLE: 2
} as const
export type Precision = typeof Precision[keyof typeof Precision]

export const TimeUnit = {
  SECOND: 0,
  MILLISECOND: 1,
  MICROSECOND: 2,
  NANOSECOND: 3
} as const
export type TimeUnit = typeof TimeUnit[keyof typeof TimeUnit]

export const UnionMode = {
  SPARSE: 0,
  DENSE: 1
} as const
export type UnionMode = typeof UnionMode[keyof typeof UnionMode]

// =============================================================================
// Schema Types
// =============================================================================

export class ArrowSchema {
  readonly fields: ReadonlyArray<ArrowField>
  readonly metadata: ReadonlyMap<string, string>
  readonly endianness: Endianness
  constructor(
    fields: ReadonlyArray<ArrowField>,
    metadata: ReadonlyMap<string, string>,
    endianness: Endianness
  ) {
    this.fields = fields
    this.metadata = metadata
    this.endianness = endianness
  }
}

export class ArrowField {
  readonly name: string
  readonly type: ArrowDataType
  readonly nullable: boolean
  readonly metadata: ReadonlyMap<string, string>
  readonly children: ReadonlyArray<ArrowField>
  readonly dictionaryEncoding?: DictionaryEncoding | undefined
  constructor(
    name: string,
    type: ArrowDataType,
    nullable: boolean,
    metadata: ReadonlyMap<string, string>,
    children: ReadonlyArray<ArrowField>,
    dictionaryEncoding?: DictionaryEncoding | undefined
  ) {
    this.name = name
    this.type = type
    this.nullable = nullable
    this.metadata = metadata
    this.children = children
    this.dictionaryEncoding = dictionaryEncoding
  }
}

export class DictionaryEncoding {
  readonly id: bigint
  readonly indexType: IntType
  readonly isOrdered: boolean
  constructor(id: bigint, indexType: IntType, isOrdered: boolean) {
    this.id = id
    this.indexType = indexType
    this.isOrdered = isOrdered
  }
}

// =============================================================================
// Arrow Data Types
// =============================================================================

export type ArrowDataType =
  | NullType
  | BoolType
  | IntType
  | FloatingPointType
  | DecimalType
  | BinaryType
  | LargeBinaryType
  | FixedSizeBinaryType
  | Utf8Type
  | LargeUtf8Type
  | DateType
  | TimeType
  | TimestampType
  | IntervalType
  | DurationType
  | ListType
  | LargeListType
  | FixedSizeListType
  | StructType
  | MapType
  | UnionType

export interface NullType {
  readonly typeId: "null"
}
export const NullType: NullType = { typeId: "null" }

export interface BoolType {
  readonly typeId: "bool"
}
export const BoolType: BoolType = { typeId: "bool" }

export type IntBitWidth = 8 | 16 | 32 | 64

export class IntType {
  readonly typeId = "int"
  readonly bitWidth: IntBitWidth
  readonly isSigned: boolean
  constructor(bitWidth: IntBitWidth, isSigned: boolean) {
    this.bitWidth = bitWidth
    this.isSigned = isSigned
  }
}

const PRECISION_MAPPING: Record<Precision, keyof typeof Precision> = {
  [Precision.HALF]: "HALF",
  [Precision.SINGLE]: "SINGLE",
  [Precision.DOUBLE]: "DOUBLE"
}

export class FloatingPointType {
  readonly typeId = "float"
  readonly precision: "HALF" | "SINGLE" | "DOUBLE"
  constructor(precision: Precision) {
    this.precision = PRECISION_MAPPING[precision]
  }
}

export class DecimalType {
  readonly typeId = "decimal"
  readonly precision: number
  readonly scale: number
  readonly bitWidth: number
  constructor(precision: number, scale: number, bitWidth: number) {
    this.precision = precision
    this.scale = scale
    this.bitWidth = bitWidth
  }
}

export interface BinaryType {
  readonly typeId: "binary"
}
export const BinaryType: BinaryType = { typeId: "binary" }

export interface LargeBinaryType {
  readonly typeId: "large-binary"
}
export const LargeBinaryType: LargeBinaryType = { typeId: "large-binary" }

export class FixedSizeBinaryType {
  readonly typeId = "fixed-size-binary"
  readonly byteWidth: number
  constructor(byteWidth: number) {
    this.byteWidth = byteWidth
  }
}

export interface Utf8Type {
  readonly typeId: "utf8"
}
export const Utf8Type: Utf8Type = { typeId: "utf8" }

export interface LargeUtf8Type {
  readonly typeId: "large-utf8"
}
export const LargeUtf8Type: LargeUtf8Type = { typeId: "large-utf8" }

export class DateType {
  readonly typeId = "date"
  readonly unit: keyof typeof DateUnit
  constructor(unit: DateUnit) {
    this.unit = unit === 0 ? "DAY" : "MILLISECOND"
  }
}

export type TimeBitWidth = 32 | 64

const TIME_UNIT_MAPPING: Record<TimeUnit, keyof typeof TimeUnit> = {
  [TimeUnit.SECOND]: "SECOND",
  [TimeUnit.MILLISECOND]: "MILLISECOND",
  [TimeUnit.MICROSECOND]: "MICROSECOND",
  [TimeUnit.NANOSECOND]: "NANOSECOND"
}

export class TimeType {
  readonly typeId = "time"
  readonly unit: keyof typeof TimeUnit
  readonly bitWidth: TimeBitWidth
  constructor(unit: TimeUnit, bitWidth: TimeBitWidth) {
    this.unit = TIME_UNIT_MAPPING[unit]
    this.bitWidth = bitWidth
  }
}

export class TimestampType {
  readonly typeId = "timestamp"
  readonly unit: keyof typeof TimeUnit
  readonly timezone: string | null
  constructor(unit: TimeUnit, timezone: string | null) {
    this.unit = TIME_UNIT_MAPPING[unit]
    this.timezone = timezone
  }
}

const INTERVAL_UNIT_MAPPING: Record<IntervalUnit, keyof typeof IntervalUnit> = {
  [IntervalUnit.DAY_TIME]: "DAY_TIME",
  [IntervalUnit.MONTH_DAY_NANO]: "MONTH_DAY_NANO",
  [IntervalUnit.YEAR_MONTH]: "YEAR_MONTH"
}

export class IntervalType {
  readonly typeId = "interval"
  readonly unit: keyof typeof IntervalUnit
  constructor(unit: IntervalUnit) {
    this.unit = INTERVAL_UNIT_MAPPING[unit]
  }
}

export class DurationType {
  readonly typeId = "duration"
  readonly unit: keyof typeof TimeUnit
  constructor(unit: TimeUnit) {
    this.unit = TIME_UNIT_MAPPING[unit]
  }
}

export interface ListType {
  readonly typeId: "list"
}
export const ListType: ListType = { typeId: "list" }

/**
 * Same as List, but with 64-bit offsets, allowing for representation of
 * extremely large data values.
 */
export interface LargeListType {
  readonly typeId: "large-list"
}
export const LargeListType: LargeListType = { typeId: "large-list" }

export class FixedSizeListType {
  readonly typeId = "fixed-size-list"
  readonly listSize: number
  constructor(listSize: number) {
    this.listSize = listSize
  }
}

/**
 * A `StructType` in the flatbuffer metadata is the same as an Arrow Struct
 * (according to the physical memory layout).
 */
export interface StructType {
  readonly typeId: "struct"
}
export const StructType: StructType = { typeId: "struct" }

export class MapType {
  readonly typeId = "map"
  readonly keysSorted: boolean
  constructor(keysSorted: boolean) {
    this.keysSorted = keysSorted
  }
}

const UNION_MODE_MAPPING: Record<UnionMode, keyof typeof UnionMode> = {
  [UnionMode.SPARSE]: "SPARSE",
  [UnionMode.DENSE]: "DENSE"
}

export class UnionType {
  readonly typeId = "union"
  readonly mode: keyof typeof UnionMode
  readonly typeIds: ReadonlyArray<number>
  constructor(mode: UnionMode, typeIds: ReadonlyArray<number>) {
    this.mode = UNION_MODE_MAPPING[mode]
    this.typeIds = typeIds
  }
}

// =============================================================================
// Message Type Utilities
// =============================================================================

/**
 * Returns the Arrow Flight message type from the `FlightData` header.
 */
export const getMessageType = Effect.fn(function*(flightData: FlightData) {
  const reader = new FlatBufferReader(flightData.dataHeader)

  // The flatbuffer root table offset is at position 0
  const rootOffset = reader.readOffset(0)

  // Read the position of the message header union type discriminator
  const headerTypePosition = reader.getFieldPosition(rootOffset, 1)

  if (Predicate.isNull(headerTypePosition)) {
    return yield* new MissingFieldError({
      fieldName: "header",
      fieldIndex: 1,
      tableOffset: rootOffset
    })
  }

  const headerType = reader.readUint8(headerTypePosition)

  if (headerType < 0 || headerType > 5) {
    return yield* new InvalidMessageTypeError({ value: headerType })
  }

  return headerType as MessageHeaderType
})

// =============================================================================
// Schema Parsing
// =============================================================================

/**
 * Parse an Arrow Schema from the raw IPC message header bytes (FlatBuffer) of
 * a `FlightData` message.
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
export const parseSchema = Effect.fn(function*(flightData: FlightData) {
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
  if (headerType !== MessageHeaderType.SCHEMA) {
    return yield* new UnexpectedMessageTypeError({
      expected: MessageHeaderType.SCHEMA,
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
  const schemaOffset = reader.readOffset(headerPosition)

  return yield* parseSchemaTable(reader, schemaOffset)
})

/**
 * Parse the Schema vtable from the FlatBuffer message header.
 *
 * The structure of the Schema vtable is as follows:
 *   0: endianness (Int16)
 *   1: fields (vector of Field)
 *   2: custom_metadata (vector of KeyValue)
 *   3: features (vector of Int64) - optional
 */
const parseSchemaTable = Effect.fn(function*(
  reader: FlatBufferReader,
  offset: number
) {
  // Parse the endianness
  const endiannessPosition = reader.getFieldPosition(offset, 0)
  const endianness = Predicate.isNotNull(endiannessPosition)
    ? (reader.readInt16(endiannessPosition) as Endianness)
    : Endianness.LITTLE

  // Parse fields
  const fields: Array<ArrowField> = []
  const fieldsPosition = reader.getFieldPosition(offset, 1)
  if (Predicate.isNotNull(fieldsPosition)) {
    const fieldsVectorOffset = reader.readOffset(fieldsPosition)
    const fieldCount = reader.readVectorLength(fieldsVectorOffset)

    for (let i = 0; i < fieldCount; i++) {
      const fieldOffsetPosition = fieldsVectorOffset + 4 + i * 4
      const fieldOffset = reader.readOffset(fieldOffsetPosition)
      fields.push(yield* parseField(reader, fieldOffset))
    }
  }
  // Parse metadata
  const metadata = new Map<string, string>()
  const metadataPosition = reader.getFieldPosition(offset, 2)
  if (Predicate.isNotNull(metadataPosition)) {
    parseKeyValueVector(reader, metadataPosition, metadata)
  }

  return new ArrowSchema(fields, metadata, endianness)
})

/**
 * Parse the Field vtable from the FlatBuffer message header.
 *
 * The structure of the Field vtable is as follows:
 *   0: name (string)
 *   1: nullable (bool)
 *   2: type_type (Type enum, UInt8)
 *   3: type (union - type-specific table)
 *   4: dictionary (DictionaryEncoding table)
 *   5: children (vector of Field)
 *   6: custom_metadata (vector of KeyValue)
 */
const parseField: (
  reader: FlatBufferReader,
  offset: number
) => Effect.Effect<ArrowField, InvalidArrowDataTypeError> = Effect.fn(
  function*(reader, offset) {
    // Parse field name
    const namePosition = reader.getFieldPosition(offset, 0)
    const name = Predicate.isNotNull(namePosition)
      ? reader.readString(namePosition)
      : ""

    // Parse field nullability
    const nullabilityPosition = reader.getFieldPosition(offset, 1)
    const nullable = Predicate.isNotNull(nullabilityPosition)
      ? reader.readUint8(nullabilityPosition) !== 0
      : false

    // Parse type
    const typeEnumPosition = reader.getFieldPosition(offset, 2)
    const typeEnum = Predicate.isNotNull(typeEnumPosition)
      ? (reader.readUint8(typeEnumPosition) as ArrowDataTypeEnum)
      : ArrowDataTypeEnum.NONE
    const typePosition = reader.getFieldPosition(offset, 3)
    const typeOffset = Predicate.isNotNull(typePosition)
      ? reader.readOffset(typePosition)
      : 0
    const type = yield* parseType(reader, typeEnum, typeOffset)

    // Parse dictionary encoding
    let dictionaryEncoding: DictionaryEncoding | undefined
    const dictPosition = reader.getFieldPosition(offset, 4)
    if (Predicate.isNotNull(dictPosition)) {
      const dictOffset = reader.readOffset(dictPosition)
      dictionaryEncoding = parseDictionaryEncoding(reader, dictOffset)
    }

    // Parse children
    const children: Array<ArrowField> = []
    const childrenPosition = reader.getFieldPosition(offset, 5)
    if (Predicate.isNotNull(childrenPosition)) {
      const childrenVectorOffset = reader.readOffset(childrenPosition)
      const childrenCount = reader.readVectorLength(childrenVectorOffset)

      for (let i = 0; i < childrenCount; i++) {
        const childOffsetPosition = childrenVectorOffset + 4 + i * 4
        const childOffset = reader.readOffset(childOffsetPosition)
        children.push(yield* parseField(reader, childOffset))
      }
    }

    // Parse metadata
    const metadata = new Map<string, string>()
    const metadataPosition = reader.getFieldPosition(offset, 6)
    if (Predicate.isNotNull(metadataPosition)) {
      parseKeyValueVector(reader, metadataPosition, metadata)
    }

    return new ArrowField(
      name,
      type,
      nullable,
      metadata,
      children,
      dictionaryEncoding
    )
  }
)

/**
 * Parse type union based on the type enum value.
 */
const parseType = Effect.fn(function*(
  reader: FlatBufferReader,
  typeEnum: ArrowDataTypeEnum,
  offset: number
) {
  switch (typeEnum) {
    case ArrowDataTypeEnum.NULL: {
      return NullType
    }
    case ArrowDataTypeEnum.BOOL: {
      return BoolType
    }
    case ArrowDataTypeEnum.INT: {
      return parseIntType(reader, offset)
    }
    case ArrowDataTypeEnum.FLOATING_POINT: {
      return parseFloatingPointType(reader, offset)
    }
    case ArrowDataTypeEnum.DECIMAL: {
      return parseDecimalType(reader, offset)
    }
    case ArrowDataTypeEnum.BINARY: {
      return BinaryType
    }
    case ArrowDataTypeEnum.LARGE_BINARY: {
      return LargeBinaryType
    }
    case ArrowDataTypeEnum.FIXED_SIZE_BINARY: {
      return parseFixedSizeBinaryType(reader, offset)
    }
    case ArrowDataTypeEnum.UTF8: {
      return Utf8Type
    }
    case ArrowDataTypeEnum.LARGE_UTF8: {
      return LargeUtf8Type
    }
    case ArrowDataTypeEnum.DATE: {
      return parseDateType(reader, offset)
    }
    case ArrowDataTypeEnum.TIME: {
      return parseTimeType(reader, offset)
    }
    case ArrowDataTypeEnum.TIMESTAMP: {
      return parseTimestampType(reader, offset)
    }
    case ArrowDataTypeEnum.INTERVAL: {
      return parseIntervalType(reader, offset)
    }
    case ArrowDataTypeEnum.DURATION: {
      return parseDurationType(reader, offset)
    }
    case ArrowDataTypeEnum.LIST: {
      return ListType
    }
    case ArrowDataTypeEnum.LARGE_LIST: {
      return LargeListType
    }
    case ArrowDataTypeEnum.FIXED_SIZE_LIST: {
      return parseFixedSizeListType(reader, offset)
    }
    case ArrowDataTypeEnum.STRUCT: {
      return StructType
    }
    case ArrowDataTypeEnum.MAP: {
      return parseMapType(reader, offset)
    }
    case ArrowDataTypeEnum.UNION: {
      return parseUnionType(reader, offset)
    }
    default: {
      return yield* new InvalidArrowDataTypeError({
        type: typeEnum,
        offset
      })
    }
  }
})

/**
 * Parses an `Int` schema.
 *
 * The structure of the Int vtable is as follows:
 *   0: bitWidth (Int32)
 *   1: is_signed (Bool)
 */
const parseIntType = (reader: FlatBufferReader, offset: number): IntType => {
  const bitWidthPosition = reader.getFieldPosition(offset, 0)
  const bitWidth = Predicate.isNotNull(bitWidthPosition)
    ? (reader.readInt32(bitWidthPosition) as IntBitWidth)
    : 32

  const isSignedPosition = reader.getFieldPosition(offset, 1)
  const isSigned = Predicate.isNotNull(isSignedPosition)
    ? reader.readUint8(isSignedPosition) !== 0
    : true

  return new IntType(bitWidth, isSigned)
}

/**
 * Parses a `FloatingPoint` schema.
 *
 * The structure of the FloatingPoint vtable is as follows:
 *   0: precision (Precision enum)
 */
const parseFloatingPointType = (
  reader: FlatBufferReader,
  offset: number
): FloatingPointType => {
  const precisionPosition = reader.getFieldPosition(offset, 0)
  const precisionEnum = Predicate.isNotNull(precisionPosition)
    ? (reader.readInt16(precisionPosition) as Precision)
    : Precision.DOUBLE

  return new FloatingPointType(precisionEnum)
}

/**
 * Parses a `Decimal` schema.
 *
 * The structure of the Decimal vtable is as follows:
 *   0: precision (Int32)
 *   1: scale (Int32)
 *   2: bitWidth (Int32)
 */
const parseDecimalType = (
  reader: FlatBufferReader,
  offset: number
): DecimalType => {
  const precisionPosition = reader.getFieldPosition(offset, 0)
  const precision = Predicate.isNotNull(precisionPosition)
    ? reader.readInt32(precisionPosition)
    : 0

  const scalePosition = reader.getFieldPosition(offset, 1)
  const scale = Predicate.isNotNull(scalePosition)
    ? reader.readInt32(scalePosition)
    : 0

  const bitWidthPosition = reader.getFieldPosition(offset, 2)
  const bitWidth = Predicate.isNotNull(bitWidthPosition)
    ? reader.readInt32(bitWidthPosition)
    : 128

  return new DecimalType(precision, scale, bitWidth)
}

/**
 * Parses a `FixedSizeBinary` schema.
 *
 * The structure of the FixedSizeBinary vtable is as follows:
 *   0: byteWidth (Int32)
 */
const parseFixedSizeBinaryType = (reader: FlatBufferReader, offset: number): FixedSizeBinaryType => {
  const byteWidthPosition = reader.getFieldPosition(offset, 0)
  const byteWidth = Predicate.isNotNull(byteWidthPosition)
    ? reader.readInt32(byteWidthPosition)
    : 0

  return new FixedSizeBinaryType(byteWidth)
}

/**
 * Parses a `Date` schema.
 *
 * The structure of the Date vtable is as follows:
 *   0: unit (DateUnit enum)
 */
const parseDateType = (reader: FlatBufferReader, offset: number): DateType => {
  const unitPosition = reader.getFieldPosition(offset, 0)
  const unitEnum = Predicate.isNotNull(unitPosition)
    ? (reader.readInt16(unitPosition) as DateUnit)
    : DateUnit.MILLISECOND

  return new DateType(unitEnum)
}

/**
 * Parses a `Time` schema.
 *
 * The structure of the Time vtable is as follows:
 *   0: unit (TimeUnit enum)
 *   1: bitWidth (Int32)
 */
const parseTimeType = (reader: FlatBufferReader, offset: number): TimeType => {
  const unitPosition = reader.getFieldPosition(offset, 0)
  const unitEnum = Predicate.isNotNull(unitPosition)
    ? (reader.readInt16(unitPosition) as TimeUnit)
    : TimeUnit.MILLISECOND

  const bitWidthPosition = reader.getFieldPosition(offset, 1)
  const bitWidth = Predicate.isNotNull(bitWidthPosition)
    ? (reader.readInt32(bitWidthPosition) as TimeBitWidth)
    : 32

  return new TimeType(unitEnum, bitWidth)
}

/**
 * Parses a `Timestamp` schema.
 *
 * The structure of the Timestamp vtable is as follows:
 *   0: unit (TimeUnit enum)
 *   1: timezone (string)
 */
const parseTimestampType = (
  reader: FlatBufferReader,
  offset: number
): TimestampType => {
  const unitPosition = reader.getFieldPosition(offset, 0)
  const unitEnum = Predicate.isNotNull(unitPosition)
    ? (reader.readInt16(unitPosition) as TimeUnit)
    : TimeUnit.MICROSECOND

  const timezonePosition = reader.getFieldPosition(offset, 1)
  const timezone = Predicate.isNotNull(timezonePosition)
    ? reader.readString(timezonePosition)
    : null

  return new TimestampType(unitEnum, timezone)
}

/**
 * Parses an `Interval` schema.
 *
 * The structure of the Interval vtable is as follows:
 *   0: unit (IntervalUnit enum)
 */
const parseIntervalType = (
  reader: FlatBufferReader,
  offset: number
): IntervalType => {
  const unitPosition = reader.getFieldPosition(offset, 0)
  const unitEnum = Predicate.isNotNull(unitPosition)
    ? (reader.readInt16(unitPosition) as IntervalUnit)
    : IntervalUnit.YEAR_MONTH

  return new IntervalType(unitEnum)
}

/**
 * Parses a `Duration` schema.
 *
 * The structure of the Duration vtable is as follows:
 *   0: unit (TimeUnit enum)
 */
const parseDurationType = (
  reader: FlatBufferReader,
  offset: number
): DurationType => {
  const unitPosition = reader.getFieldPosition(offset, 0)
  const unitEnum = Predicate.isNotNull(unitPosition)
    ? (reader.readInt16(unitPosition) as TimeUnit)
    : TimeUnit.MILLISECOND

  return new DurationType(unitEnum)
}

/**
 * Parses a `FixedSizeList` schema.
 *
 * The structure of the FixedSizeList vtable is as follows:
 *   0: listSize (Int32)
 */
const parseFixedSizeListType = (
  reader: FlatBufferReader,
  offset: number
): FixedSizeListType => {
  const listSizePosition = reader.getFieldPosition(offset, 0)
  const listSize = Predicate.isNotNull(listSizePosition)
    ? reader.readInt32(listSizePosition)
    : 0

  return new FixedSizeListType(listSize)
}

/**
 * Parses a `Map` schema.
 *
 * The structure of the Map vtable is as follows:
 *   0: keysSorted (Bool)
 */
const parseMapType = (reader: FlatBufferReader, offset: number): MapType => {
  const keysSortedPosition = reader.getFieldPosition(offset, 0)
  const keysSorted = Predicate.isNotNull(keysSortedPosition)
    ? reader.readUint8(keysSortedPosition) !== 0
    : false

  return new MapType(keysSorted)
}

/**
 * Parses a `Union` schema.
 *
 * The structure of the Union vtable is as follows:
 *   0: mode (UnionMode enum)
 *   1: typeIds (vector of Int32)
 */
const parseUnionType = (
  reader: FlatBufferReader,
  offset: number
): UnionType => {
  const modePosition = reader.getFieldPosition(offset, 0)
  const modeEnum = Predicate.isNotNull(modePosition)
    ? (reader.readInt16(modePosition) as UnionMode)
    : UnionMode.SPARSE

  const typeIds: Array<number> = []
  const typeIdsPosition = reader.getFieldPosition(offset, 1)
  if (Predicate.isNotNull(typeIdsPosition)) {
    const vectorOffset = reader.readOffset(typeIdsPosition)
    const typeIdCount = reader.readVectorLength(vectorOffset)
    for (let i = 0; i < typeIdCount; i++) {
      typeIds.push(reader.readInt32(vectorOffset + 4 + i * 4))
    }
  }

  return new UnionType(modeEnum, typeIds)
}

/**
 * Parses a `DictionaryEncoding` schema.
 *
 * The structure of the DictionaryEncoding vtable is as follows:
 *   0: id (Int64)
 *   1: indexType (Int table)
 *   2: isOrdered (Bool)
 */
const parseDictionaryEncoding = (
  reader: FlatBufferReader,
  offset: number
): DictionaryEncoding => {
  const idPosition = reader.getFieldPosition(offset, 0)
  const id = Predicate.isNotNull(idPosition)
    ? reader.readInt64(idPosition)
    : 0n

  const indexTypePosition = reader.getFieldPosition(offset, 1)
  let indexType: IntType
  if (Predicate.isNotNull(indexTypePosition)) {
    const indexTypeOffset = reader.readOffset(indexTypePosition)
    indexType = parseIntType(reader, indexTypeOffset)
  } else {
    indexType = new IntType(32, true)
  }

  const isOrderedPosition = reader.getFieldPosition(offset, 2)
  const isOrdered = Predicate.isNotNull(isOrderedPosition)
    ? reader.readUint8(isOrderedPosition) !== 0
    : false

  return new DictionaryEncoding(id, indexType, isOrdered)
}

/**
 * Parses a KeyValue vector into a Map.
 */
const parseKeyValueVector = (
  reader: FlatBufferReader,
  pos: number,
  map: Map<string, string>
): void => {
  const vectorOffset = reader.readOffset(pos)
  const itemCount = reader.readVectorLength(vectorOffset)

  for (let i = 0; i < itemCount; i++) {
    const kvOffsetPosition = vectorOffset + 4 + i * 4
    const kvOffset = reader.readOffset(kvOffsetPosition)

    // KeyValue table: key (string), value (string)
    const keyPosition = reader.getFieldPosition(kvOffset, 0)
    const valuePosition = reader.getFieldPosition(kvOffset, 1)

    if (Predicate.isNotNull(keyPosition)) {
      const key = reader.readString(keyPosition)
      const value = Predicate.isNotNull(valuePosition)
        ? reader.readString(valuePosition)
        : ""
      map.set(key, value)
    }
  }
}

// =============================================================================
// Schema Utilities
// =============================================================================

/**
 * Returns `true` if the provided `FlightData` header data buffer contains a
 * schema message, otherwise returns `false`.
 */
export const isSchemaMessage = Effect.fn(function*(flightData: FlightData) {
  const messageType = yield* getMessageType(flightData)
  return messageType === MessageHeaderType.SCHEMA
})
