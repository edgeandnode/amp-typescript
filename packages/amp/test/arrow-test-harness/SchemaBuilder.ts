/**
 * Fluent Schema Builder
 * @internal
 */
import {
  ArrowField,
  ArrowSchema,
  BinaryType,
  BoolType,
  DateType,
  DateUnit,
  DecimalType,
  DurationType,
  Endianness,
  FixedSizeBinaryType,
  FixedSizeListType,
  FloatingPointType,
  IntervalType,
  IntervalUnit,
  IntType,
  LargeBinaryType,
  LargeListType,
  LargeUtf8Type,
  ListType,
  MapType,
  NullType,
  Precision,
  StructType,
  TimestampType,
  TimeType,
  TimeUnit,
  UnionMode,
  UnionType,
  Utf8Type
} from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"

export class SchemaBuilder {
  private readonly fields: Array<ArrowField> = []
  private readonly schemaMetadata: Map<string, string> = new Map()

  null(name: string, options?: FieldOptions): this {
    return this.addField(name, NullType, options)
  }

  bool(name: string, options?: FieldOptions): this {
    return this.addField(name, BoolType, options)
  }

  int8(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(8, true), options)
  }

  int16(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(16, true), options)
  }

  int32(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(32, true), options)
  }

  int64(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(64, true), options)
  }

  uint8(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(8, false), options)
  }

  uint16(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(16, false), options)
  }

  uint32(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(32, false), options)
  }

  uint64(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntType(64, false), options)
  }

  float16(name: string, options?: FieldOptions): this {
    return this.addField(name, new FloatingPointType(Precision.HALF), options)
  }

  float32(name: string, options?: FieldOptions): this {
    return this.addField(name, new FloatingPointType(Precision.SINGLE), options)
  }

  float64(name: string, options?: FieldOptions): this {
    return this.addField(name, new FloatingPointType(Precision.DOUBLE), options)
  }

  decimal(name: string, precision: number, scale: number, bitWidth: 128 | 256 = 128, options?: FieldOptions): this {
    return this.addField(name, new DecimalType(precision, scale, bitWidth), options)
  }

  utf8(name: string, options?: FieldOptions): this {
    return this.addField(name, Utf8Type, options)
  }

  largeUtf8(name: string, options?: FieldOptions): this {
    return this.addField(name, LargeUtf8Type, options)
  }

  binary(name: string, options?: FieldOptions): this {
    return this.addField(name, BinaryType, options)
  }

  largeBinary(name: string, options?: FieldOptions): this {
    return this.addField(name, LargeBinaryType, options)
  }

  fixedSizeBinary(name: string, byteWidth: number, options?: FieldOptions): this {
    return this.addField(name, new FixedSizeBinaryType(byteWidth), options)
  }

  dateDay(name: string, options?: FieldOptions): this {
    return this.addField(name, new DateType(DateUnit.DAY), options)
  }

  dateMillisecond(name: string, options?: FieldOptions): this {
    return this.addField(name, new DateType(DateUnit.MILLISECOND), options)
  }

  timeSecond(name: string, options?: FieldOptions): this {
    return this.addField(name, new TimeType(TimeUnit.SECOND, 32), options)
  }

  timeMillisecond(name: string, options?: FieldOptions): this {
    return this.addField(name, new TimeType(TimeUnit.MILLISECOND, 32), options)
  }

  timeMicrosecond(name: string, options?: FieldOptions): this {
    return this.addField(name, new TimeType(TimeUnit.MICROSECOND, 64), options)
  }

  timeNanosecond(name: string, options?: FieldOptions): this {
    return this.addField(name, new TimeType(TimeUnit.NANOSECOND, 64), options)
  }

  timestamp(
    name: string,
    unit: "SECOND" | "MILLISECOND" | "MICROSECOND" | "NANOSECOND" = "MICROSECOND",
    timezone: string | null = null,
    options?: FieldOptions
  ): this {
    const unitEnum = TimeUnit[unit]
    return this.addField(name, new TimestampType(unitEnum, timezone), options)
  }

  duration(
    name: string,
    unit: "SECOND" | "MILLISECOND" | "MICROSECOND" | "NANOSECOND" = "MICROSECOND",
    options?: FieldOptions
  ): this {
    const unitEnum = TimeUnit[unit]
    return this.addField(name, new DurationType(unitEnum), options)
  }

  intervalYearMonth(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntervalType(IntervalUnit.YEAR_MONTH), options)
  }

  intervalDayTime(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntervalType(IntervalUnit.DAY_TIME), options)
  }

  intervalMonthDayNano(name: string, options?: FieldOptions): this {
    return this.addField(name, new IntervalType(IntervalUnit.MONTH_DAY_NANO), options)
  }

  list(name: string, itemBuilder: (b: FieldBuilder) => FieldBuilder, options?: FieldOptions): this {
    const itemField = itemBuilder(new FieldBuilder("item")).build()
    return this.addField(name, ListType, { ...options, children: [itemField] })
  }

  largeList(name: string, itemBuilder: (b: FieldBuilder) => FieldBuilder, options?: FieldOptions): this {
    const itemField = itemBuilder(new FieldBuilder("item")).build()
    return this.addField(name, LargeListType, { ...options, children: [itemField] })
  }

  fixedSizeList(
    name: string,
    listSize: number,
    itemBuilder: (b: FieldBuilder) => FieldBuilder,
    options?: FieldOptions
  ): this {
    const itemField = itemBuilder(new FieldBuilder("item")).build()
    return this.addField(name, new FixedSizeListType(listSize), { ...options, children: [itemField] })
  }

  struct(name: string, structBuilder: (b: SchemaBuilder) => SchemaBuilder, options?: FieldOptions): this {
    const nestedBuilder = structBuilder(new SchemaBuilder())
    return this.addField(name, StructType, { ...options, children: nestedBuilder.fields })
  }

  map(
    name: string,
    keyBuilder: (b: FieldBuilder) => FieldBuilder,
    valueBuilder: (b: FieldBuilder) => FieldBuilder,
    options?: MapFieldOptions
  ): this {
    const keyField = keyBuilder(new FieldBuilder("key")).nullable(false).build()
    const valueField = valueBuilder(new FieldBuilder("value")).build()

    // Map's child is an "entries" struct with "key" and "value" fields
    const entriesField = new ArrowField(
      "entries",
      StructType,
      false,
      new Map(),
      [keyField, valueField],
      undefined
    )

    return this.addField(name, new MapType(options?.keysSorted ?? false), { ...options, children: [entriesField] })
  }

  union(
    name: string,
    mode: "SPARSE" | "DENSE",
    variantBuilder: (b: UnionBuilder) => UnionBuilder,
    options?: FieldOptions
  ): this {
    const builder = variantBuilder(new UnionBuilder())
    const { children, typeIds } = builder.build()
    const modeEnum = mode === "SPARSE" ? UnionMode.SPARSE : UnionMode.DENSE
    return this.addField(name, new UnionType(modeEnum, typeIds), { ...options, children })
  }

  metadata(key: string, value: string): this {
    this.schemaMetadata.set(key, value)
    return this
  }

  build(): ArrowSchema {
    return new ArrowSchema(this.fields, this.schemaMetadata, Endianness.LITTLE)
  }

  private addField(
    name: string,
    type: ArrowField["type"],
    options?: FieldOptions & { children?: ReadonlyArray<ArrowField> }
  ): this {
    const field = new ArrowField(
      name,
      type,
      options?.nullable ?? true,
      options?.metadata ?? new Map(),
      options?.children ?? [],
      undefined
    )
    this.fields.push(field)
    return this
  }
}

export class FieldBuilder {
  private fieldName: string
  private fieldType: ArrowField["type"] = NullType
  private fieldNullable: boolean = true
  private fieldMetadata: Map<string, string> = new Map()
  private fieldChildren: ReadonlyArray<ArrowField> = []

  constructor(name: string) {
    this.fieldName = name
  }

  null(): this {
    this.fieldType = NullType
    return this
  }

  bool(): this {
    this.fieldType = BoolType
    return this
  }

  int8(): this {
    this.fieldType = new IntType(8, true)
    return this
  }

  int16(): this {
    this.fieldType = new IntType(16, true)
    return this
  }

  int32(): this {
    this.fieldType = new IntType(32, true)
    return this
  }

  int64(): this {
    this.fieldType = new IntType(64, true)
    return this
  }

  uint8(): this {
    this.fieldType = new IntType(8, false)
    return this
  }

  uint16(): this {
    this.fieldType = new IntType(16, false)
    return this
  }

  uint32(): this {
    this.fieldType = new IntType(32, false)
    return this
  }

  uint64(): this {
    this.fieldType = new IntType(64, false)
    return this
  }

  float16(): this {
    this.fieldType = new FloatingPointType(Precision.HALF)
    return this
  }

  float32(): this {
    this.fieldType = new FloatingPointType(Precision.SINGLE)
    return this
  }

  float64(): this {
    this.fieldType = new FloatingPointType(Precision.DOUBLE)
    return this
  }

  decimal(precision: number, scale: number, bitWidth: 128 | 256 = 128): this {
    this.fieldType = new DecimalType(precision, scale, bitWidth)
    return this
  }

  utf8(): this {
    this.fieldType = Utf8Type
    return this
  }

  largeUtf8(): this {
    this.fieldType = LargeUtf8Type
    return this
  }

  binary(): this {
    this.fieldType = BinaryType
    return this
  }

  largeBinary(): this {
    this.fieldType = LargeBinaryType
    return this
  }

  fixedSizeBinary(byteWidth: number): this {
    this.fieldType = new FixedSizeBinaryType(byteWidth)
    return this
  }

  nullable(value: boolean = true): this {
    this.fieldNullable = value
    return this
  }

  metadata(key: string, value: string): this {
    this.fieldMetadata.set(key, value)
    return this
  }

  build(): ArrowField {
    return new ArrowField(
      this.fieldName,
      this.fieldType,
      this.fieldNullable,
      this.fieldMetadata,
      this.fieldChildren,
      undefined
    )
  }
}

export class UnionBuilder {
  private variants: Array<ArrowField> = []
  private ids: Array<number> = []
  private nextId: number = 0

  variant(name: string, fieldBuilder: (b: FieldBuilder) => FieldBuilder, typeId?: number): this {
    const id = typeId ?? this.nextId++
    const field = fieldBuilder(new FieldBuilder(name)).build()
    this.variants.push(field)
    this.ids.push(id)
    return this
  }

  build(): { children: ReadonlyArray<ArrowField>; typeIds: ReadonlyArray<number> } {
    return { children: this.variants, typeIds: this.ids }
  }
}

interface FieldOptions {
  readonly nullable?: boolean
  readonly metadata?: Map<string, string>
}

interface MapFieldOptions extends FieldOptions {
  readonly keysSorted?: boolean
}

export const schema = (): SchemaBuilder => new SchemaBuilder()
