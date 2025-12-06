// =============================================================================
// Constants
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
