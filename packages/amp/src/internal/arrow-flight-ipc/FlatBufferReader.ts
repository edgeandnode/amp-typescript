/**
 * A utility class for reading FlatBuffer-encoded data.
 *
 * FlatBuffers are a serialization format that allows efficient reading of
 * structured data without parsing or unpacking.
 *
 * @internal
 */
export class FlatBufferReader {
  private view: DataView
  private bytes: Uint8Array

  constructor(bytes: Uint8Array) {
    this.bytes = bytes
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  }

  readInt8(offset: number): number {
    return this.view.getInt8(offset)
  }

  readUint8(offset: number): number {
    return this.view.getUint8(offset)
  }

  readInt16(offset: number): number {
    return this.view.getInt16(offset, true)
  }

  readUint16(offset: number): number {
    return this.view.getUint16(offset, true)
  }

  readInt32(offset: number): number {
    return this.view.getInt32(offset, true)
  }

  readUint32(offset: number): number {
    return this.view.getUint32(offset, true)
  }

  readInt64(offset: number): bigint {
    return this.view.getBigInt64(offset, true)
  }

  readUint64(offset: number): bigint {
    return this.view.getBigUint64(offset, true)
  }

  /**
   * Read offset to a table/vector (indirect offset)
   */
  readOffset(offset: number): number {
    return offset + this.readInt32(offset)
  }

  /**
   * Read a string from a FlatBuffer offset
   */
  readString(offset: number): string {
    const stringOffset = this.readOffset(offset)
    const length = this.readInt32(stringOffset)
    const stringStart = stringOffset + 4
    const stringBytes = this.bytes.subarray(stringStart, stringStart + length)
    return new TextDecoder().decode(stringBytes)
  }

  // Read vector length
  readVectorLength(offset: number): number {
    return this.readInt32(offset)
  }

  /**
   * Get table field offset using vtable
   */
  getFieldOffset(tableOffset: number, fieldIndex: number): number {
    const vtableOffset = tableOffset - this.readInt32(tableOffset)
    const vtableSize = this.readInt16(vtableOffset)
    const fieldVtableOffset = 4 + fieldIndex * 2

    if (fieldVtableOffset >= vtableSize) {
      return 0 // Field not present
    }

    return this.readInt16(vtableOffset + fieldVtableOffset)
  }

  /**
   * Get absolute position of a field in a table
   */
  getFieldPosition(tableOffset: number, fieldIndex: number): number | null {
    const fieldOffset = this.getFieldOffset(tableOffset, fieldIndex)
    if (fieldOffset === 0) {
      return null
    }
    return tableOffset + fieldOffset
  }
}
