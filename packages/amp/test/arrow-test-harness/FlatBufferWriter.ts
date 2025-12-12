/**
 * FlatBuffer Writer for Arrow Test Harness
 *
 * Writes FlatBuffer-encoded data that can be read by FlatBufferReader.
 *
 * @internal
 */

export class FlatBufferWriter {
  private buffer: Uint8Array
  private position: number = 0
  private vtableCache: Map<string, number> = new Map()

  constructor(initialSize: number = 4096) {
    this.buffer = new Uint8Array(initialSize)
  }

  private grow(needed: number): void {
    if (this.position + needed <= this.buffer.length) return
    const newSize = Math.max(this.buffer.length * 2, this.position + needed)
    const newBuffer = new Uint8Array(newSize)
    newBuffer.set(this.buffer)
    this.buffer = newBuffer
  }

  private pad(alignment: number): void {
    const mask = alignment - 1
    while (this.position & mask) {
      this.grow(1)
      this.buffer[this.position++] = 0
    }
  }

  currentOffset(): number {
    return this.position
  }

  writeByte(value: number): void {
    this.grow(1)
    this.buffer[this.position++] = value & 0xff
  }

  writeInt16(value: number): void {
    this.pad(2)
    this.grow(2)
    new DataView(this.buffer.buffer, this.position, 2).setInt16(0, value, true)
    this.position += 2
  }

  writeInt32(value: number): void {
    this.pad(4)
    this.grow(4)
    new DataView(this.buffer.buffer, this.position, 4).setInt32(0, value, true)
    this.position += 4
  }

  writeInt64(value: bigint): void {
    this.pad(8)
    this.grow(8)
    new DataView(this.buffer.buffer, this.position, 8).setBigInt64(0, value, true)
    this.position += 8
  }

  // Raw write methods without alignment (for table field writing)
  private writeInt16Raw(value: number): void {
    this.grow(2)
    new DataView(this.buffer.buffer, this.position, 2).setInt16(0, value, true)
    this.position += 2
  }

  private writeInt32Raw(value: number): void {
    this.grow(4)
    new DataView(this.buffer.buffer, this.position, 4).setInt32(0, value, true)
    this.position += 4
  }

  private writeInt64Raw(value: bigint): void {
    this.grow(8)
    new DataView(this.buffer.buffer, this.position, 8).setBigInt64(0, value, true)
    this.position += 8
  }

  writeBytes(bytes: Uint8Array): void {
    this.grow(bytes.length)
    this.buffer.set(bytes, this.position)
    this.position += bytes.length
  }

  writeString(value: string): number {
    const bytes = new TextEncoder().encode(value)
    this.pad(4)
    const offset = this.position

    // Length prefix
    this.writeInt32(bytes.length)

    // String bytes + null terminator
    this.grow(bytes.length + 1)
    this.buffer.set(bytes, this.position)
    this.position += bytes.length
    this.buffer[this.position++] = 0

    this.pad(4)
    return offset
  }

  writeOffsetVector(offsets: ReadonlyArray<number>): number {
    this.pad(4)
    const vectorOffset = this.position

    // Write count
    this.writeInt32(offsets.length)

    // Write each offset as relative offset from its position
    for (const target of offsets) {
      const relOffset = target - this.position
      this.writeInt32(relOffset)
    }

    return vectorOffset
  }

  writeStructVector(data: Uint8Array, structSize: number): number {
    this.pad(4)
    const offset = this.position
    const count = data.length / structSize

    this.writeInt32(count)
    this.writeBytes(data)

    return offset
  }

  startTable(): TableBuilder {
    return new TableBuilder()
  }

  finishTable(builder: TableBuilder): number {
    const { fields, maxFieldIndex } = builder.build()

    if (maxFieldIndex < 0) {
      // Empty table
      this.pad(2)
      const vtableOffset = this.position
      this.writeInt16(4) // vtable size
      this.writeInt16(4) // table size

      this.pad(4)
      const tableOffset = this.position
      this.writeInt32(tableOffset - vtableOffset) // soffset to vtable
      return tableOffset
    }

    // Calculate field layout within table (after the 4-byte vtable pointer)
    const fieldCount = maxFieldIndex + 1
    const fieldLayout: Array<{ index: number; offset: number; field: TableField }> = []

    // Sort by size descending for alignment efficiency
    const sortedFields = [...fields.entries()].sort((a, b) => b[1].size - a[1].size)

    let tableContentOffset = 4 // Start after vtable pointer
    for (const [index, field] of sortedFields) {
      // Align to field size
      const align = field.size
      tableContentOffset = (tableContentOffset + align - 1) & ~(align - 1)
      fieldLayout.push({ index, offset: tableContentOffset, field })
      tableContentOffset += field.size
    }

    const tableSize = tableContentOffset

    // Build vtable: [vtableSize: i16] [tableSize: i16] [field0: i16] [field1: i16] ...
    const vtableSize = 4 + fieldCount * 2
    const vtable = new Uint8Array(vtableSize)
    const vtableView = new DataView(vtable.buffer)
    vtableView.setInt16(0, vtableSize, true)
    vtableView.setInt16(2, tableSize, true)

    // Fill field offsets in vtable
    for (const { index, offset } of fieldLayout) {
      vtableView.setInt16(4 + index * 2, offset, true)
    }

    // Check vtable cache for deduplication
    const vtableKey = Array.from(vtable).join(",")
    let vtableOffset = this.vtableCache.get(vtableKey)

    if (vtableOffset === undefined) {
      this.pad(2)
      vtableOffset = this.position
      this.writeBytes(vtable)
      this.vtableCache.set(vtableKey, vtableOffset)
    }

    // Write table
    this.pad(4)
    const tableOffset = this.position

    // soffset_t pointing back to vtable
    this.writeInt32(tableOffset - vtableOffset)

    // Write fields in offset order
    const byOffset = [...fieldLayout].sort((a, b) => a.offset - b.offset)

    for (const { field, offset } of byOffset) {
      const targetPos = tableOffset + offset

      // Pad to target position
      while (this.position < targetPos) {
        this.writeByte(0)
      }

      // Write field value (using raw methods to avoid re-alignment)
      if (field.type === "offset") {
        const relOffset = (field.value as number) - this.position
        this.writeInt32Raw(relOffset)
      } else if (field.size === 1) {
        this.writeByte(field.value as number)
      } else if (field.size === 2) {
        this.writeInt16Raw(field.value as number)
      } else if (field.size === 4) {
        this.writeInt32Raw(field.value as number)
      } else if (field.size === 8) {
        this.writeInt64Raw(field.value as bigint)
      }
    }

    // Pad to full table size
    while (this.position < tableOffset + tableSize) {
      this.writeByte(0)
    }

    return tableOffset
  }

  finish(rootTableOffset: number): Uint8Array {
    this.pad(4)

    // FlatBuffer: first 4 bytes are uoffset_t to root table
    const finalBuffer = new Uint8Array(this.position + 4)
    const view = new DataView(finalBuffer.buffer)

    // Root offset is relative to byte 0, pointing to rootTableOffset + 4
    view.setUint32(0, rootTableOffset + 4, true)
    finalBuffer.set(this.buffer.subarray(0, this.position), 4)

    return finalBuffer
  }
}

interface TableField {
  value: number | bigint
  size: number
  type: "scalar" | "offset"
}

export class TableBuilder {
  private fields: Map<number, TableField> = new Map()

  addInt8(fieldIndex: number, value: number): this {
    this.fields.set(fieldIndex, { value, size: 1, type: "scalar" })
    return this
  }

  addUint8(fieldIndex: number, value: number): this {
    this.fields.set(fieldIndex, { value, size: 1, type: "scalar" })
    return this
  }

  addInt16(fieldIndex: number, value: number): this {
    this.fields.set(fieldIndex, { value, size: 2, type: "scalar" })
    return this
  }

  addInt32(fieldIndex: number, value: number): this {
    this.fields.set(fieldIndex, { value, size: 4, type: "scalar" })
    return this
  }

  addInt64(fieldIndex: number, value: bigint): this {
    this.fields.set(fieldIndex, { value, size: 8, type: "scalar" })
    return this
  }

  addOffset(fieldIndex: number, offset: number): this {
    this.fields.set(fieldIndex, { value: offset, size: 4, type: "offset" })
    return this
  }

  addBool(fieldIndex: number, value: boolean): this {
    this.fields.set(fieldIndex, { value: value ? 1 : 0, size: 1, type: "scalar" })
    return this
  }

  build(): { fields: Map<number, TableField>; maxFieldIndex: number } {
    const maxFieldIndex = this.fields.size > 0 ? Math.max(...this.fields.keys()) : -1
    return { fields: this.fields, maxFieldIndex }
  }
}
