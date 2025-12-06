import type { ArrowField, ArrowSchema } from "../schema/types.ts"
import { DecodedColumn, DecodedRecordBatch, getBufferTypesForType, type RecordBatch } from "./types.ts"

/**
 * Decodes a `RecordBatch` by combining metadata with body data.
 */
export const decodeRecordBatch = (
  recordBatch: RecordBatch,
  body: Uint8Array,
  schema: ArrowSchema
): DecodedRecordBatch => {
  const extractedBuffers = recordBatch.buffers.map((descriptor) => {
    // TODO: figure out if this can lead to bugs by converting to number
    const start = Number(descriptor.offset)
    const end = start + Number(descriptor.length)
    return body.subarray(start, end)
  })

  let nodeIndex = 0
  let bufferIndex = 0

  function decodeField(field: ArrowField): DecodedColumn {
    const node = recordBatch.nodes[nodeIndex++]
    const bufferTypes = getBufferTypesForType(field.type)

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
