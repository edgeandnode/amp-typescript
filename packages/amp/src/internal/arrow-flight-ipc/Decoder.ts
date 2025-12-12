/**
 * Arrow RecordBatch Decoder
 *
 * This module provides utilities for decoding Arrow RecordBatch messages
 * by combining metadata with body data.
 *
 * @internal
 */
import {
  type BufferType,
  DecodedColumn,
  DecodedRecordBatch,
  type DictionaryBatch,
  getBufferTypesForType,
  type RecordBatch
} from "./RecordBatch.ts"
import type { ArrowField, ArrowSchema, IntType } from "./Schema.ts"

// =============================================================================
// Dictionary Registry
// =============================================================================

/**
 * A decoded dictionary containing the values that dictionary-encoded columns
 * reference via indices.
 */
export class DecodedDictionary {
  /**
   * The dictionary ID that matches the field's `dictionaryEncoding.id`.
   */
  readonly id: bigint

  /**
   * The decoded values that indices reference into.
   */
  readonly values: ReadonlyArray<unknown>

  /**
   * The value type of this dictionary (e.g., utf8, int, etc.).
   */
  readonly valueType: ArrowField

  constructor(id: bigint, values: ReadonlyArray<unknown>, valueType: ArrowField) {
    this.id = id
    this.values = values
    this.valueType = valueType
  }
}

/**
 * A registry for storing decoded dictionaries. Dictionary-encoded columns
 * reference dictionaries by ID, and this registry maintains the mapping
 * from ID to decoded dictionary values.
 *
 * This class is mutable and should be used within a streaming context where
 * dictionary batches may arrive before or interleaved with record batches.
 */
export class DictionaryRegistry {
  private readonly dictionaries = new Map<bigint, DecodedDictionary>()

  /**
   * Register a decoded dictionary. If `isDelta` is true, the values are
   * appended to an existing dictionary with the same ID. Otherwise, the
   * dictionary replaces any existing one.
   */
  register(dictionary: DecodedDictionary, isDelta: boolean): void {
    if (isDelta) {
      const existing = this.dictionaries.get(dictionary.id)
      if (existing) {
        // Append delta values to existing dictionary
        const combinedValues = [...existing.values, ...dictionary.values]
        this.dictionaries.set(
          dictionary.id,
          new DecodedDictionary(dictionary.id, combinedValues, dictionary.valueType)
        )
      } else {
        // No existing dictionary, just set it
        this.dictionaries.set(dictionary.id, dictionary)
      }
    } else {
      // Replace any existing dictionary
      this.dictionaries.set(dictionary.id, dictionary)
    }
  }

  /**
   * Get a dictionary by ID.
   */
  get(id: bigint): DecodedDictionary | undefined {
    return this.dictionaries.get(id)
  }

  /**
   * Check if a dictionary with the given ID exists.
   */
  has(id: bigint): boolean {
    return this.dictionaries.has(id)
  }

  /**
   * Clear all dictionaries from the registry.
   */
  clear(): void {
    this.dictionaries.clear()
  }
}

// =============================================================================
// Dictionary Batch Decoding
// =============================================================================

/**
 * Find a field in the schema that references the given dictionary ID.
 * Dictionary batches need to know the value type, which is stored in the
 * schema field's type definition.
 */
const findFieldByDictionaryId = (
  fields: ReadonlyArray<ArrowField>,
  dictionaryId: bigint
): ArrowField | undefined => {
  for (const field of fields) {
    if (field.dictionaryEncoding?.id === dictionaryId) {
      return field
    }
    // Check children recursively
    const found = findFieldByDictionaryId(field.children, dictionaryId)
    if (found) {
      return found
    }
  }
  return undefined
}

/**
 * A function type for reading column values from a decoded column.
 * This is passed as a parameter to avoid circular dependencies with Readers.ts.
 */
export type ColumnValueReader = (column: DecodedColumn) => ReadonlyArray<unknown>

/**
 * Decodes a `DictionaryBatch` by combining metadata with body data and
 * registers it in the provided dictionary registry.
 *
 * @param dictionaryBatch - The parsed dictionary batch metadata
 * @param body - The binary body data containing the dictionary values
 * @param schema - The Arrow schema (used to find the field that references this dictionary)
 * @param registry - The dictionary registry to store the decoded dictionary
 * @param readColumnValues - Function to read column values (passed to avoid circular deps)
 */
export const decodeDictionaryBatch = (
  dictionaryBatch: DictionaryBatch,
  body: Uint8Array,
  schema: ArrowSchema,
  registry: DictionaryRegistry,
  readColumnValues: ColumnValueReader
): void => {
  const { data, id, isDelta } = dictionaryBatch

  // Find the field that references this dictionary to get the value type
  const field = findFieldByDictionaryId(schema.fields, id)
  if (!field) {
    throw new Error(`No field found referencing dictionary ID ${id}`)
  }

  // Extract buffers from the dictionary batch body
  const extractedBuffers = data.buffers.map((descriptor) => {
    const start = Number(descriptor.offset)
    const end = start + Number(descriptor.length)
    return body.subarray(start, end)
  })

  // Dictionary batches have a single field with the dictionary values
  // The field type is the actual value type (e.g., utf8 for string dictionaries)
  const node = data.nodes[0]
  const bufferTypes = getBufferTypesForType(field.type)

  const buffers: Array<Uint8Array> = []
  for (let i = 0; i < bufferTypes.length; i++) {
    buffers.push(extractedBuffers[i])
  }

  // Create a decoded column for the dictionary values
  const dictionaryColumn = new DecodedColumn(field, node, buffers, [])

  // Read the dictionary values using the provided reader function
  const values = readColumnValues(dictionaryColumn)

  // Register the dictionary
  const decodedDictionary = new DecodedDictionary(id, values, field)
  registry.register(decodedDictionary, isDelta)
}

// =============================================================================
// Record Batch Decoding
// =============================================================================

/**
 * Decodes a `RecordBatch` by combining metadata with body data.
 *
 * @throws {Error} If the record batch uses compression (not currently supported)
 */
export const decodeRecordBatch = (
  recordBatch: RecordBatch,
  body: Uint8Array,
  schema: ArrowSchema
): DecodedRecordBatch => {
  // Check for compression - not currently supported
  if (recordBatch.compression) {
    const codecName = recordBatch.compression.codec === 0 ? "LZ4_FRAME" : "ZSTD"
    throw new Error(
      `Compressed record batches are not currently supported. ` +
        `This batch uses ${codecName} compression. ` +
        `To process this data, the server should be configured to send uncompressed data, ` +
        `or compression support needs to be added to this library.`
    )
  }

  const extractedBuffers = recordBatch.buffers.map((descriptor) => {
    // TODO: figure out if this can lead to bugs due to loss of precision when
    // converting bigint to number
    const start = Number(descriptor.offset)
    const end = start + Number(descriptor.length)
    return body.subarray(start, end)
  })

  let nodeIndex = 0
  let bufferIndex = 0

  function decodeField(field: ArrowField): DecodedColumn {
    const node = recordBatch.nodes[nodeIndex++]

    // For dictionary-encoded fields, the buffer layout is based on the index type,
    // not the value type. Dictionary-encoded columns have [validity, data] layout
    // where data contains integer indices.
    const bufferTypes = field.dictionaryEncoding
      ? getBufferTypesForDictionaryEncoding(field.dictionaryEncoding.indexType)
      : getBufferTypesForType(field.type)

    const buffers: Array<Uint8Array> = []
    for (let i = 0; i < bufferTypes.length; i++) {
      buffers.push(extractedBuffers[bufferIndex++])
    }

    // Decode children recursively
    const children: ReadonlyArray<DecodedColumn> = field.children.map((field) => decodeField(field))

    return new DecodedColumn(field, node, buffers, children)
  }

  const numRows = recordBatch.length
  const columns = schema.fields.map((field) => decodeField(field))

  return new DecodedRecordBatch(schema, numRows, columns)
}

/**
 * Get the buffer types for a dictionary-encoded field.
 * Dictionary-encoded fields store integer indices, so they have the same
 * buffer layout as an integer type: [validity, data].
 */
const getBufferTypesForDictionaryEncoding = (
  _indexType: IntType
): ReadonlyArray<BufferType> => {
  // Dictionary-encoded columns always have: validity bitmap + index data
  // The index type determines the bit width but not the buffer layout
  return [
    0, // BufferType.VALIDITY
    3 // BufferType.DATA
  ]
}
