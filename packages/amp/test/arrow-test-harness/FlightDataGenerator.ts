/**
 * FlightData Generator - Main Orchestrator
 * @internal
 */
import type { ArrowField, ArrowSchema } from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Random from "effect/Random"
import * as BufferUtils from "./BufferUtils.ts"
import * as GeneratorRegistry from "./GeneratorRegistry.ts"
import * as MessageEncoder from "./MessageEncoder.ts"
import * as Types from "./Types.ts"

export const generateFlightData = (
  schema: ArrowSchema,
  options: Types.FlightDataGeneratorOptions = {}
): Effect.Effect<Types.GeneratedFlightData> => {
  const numRows = options.numRows ?? 100
  const seed = typeof options.seed === "string" ? hashString(options.seed) : (options.seed ?? 42)

  return Effect.gen(function*() {
    const registry = yield* Types.GeneratorRegistry

    // Generate data for each field
    const columnResults: Array<Types.GeneratorResult> = []
    const expectedValues: Record<string, ReadonlyArray<unknown>> = {}

    for (const field of schema.fields) {
      const config = getFieldConfig(field.name, options)
      const generator = registry.getGenerator(field.type.typeId)
      const result = yield* generator.generate(field, numRows, config)
      columnResults.push(result)
      expectedValues[field.name] = result.values
    }

    // Collect field nodes and buffers in depth-first order
    const fieldNodes: Array<MessageEncoder.FieldNodeData> = []
    const buffers: Array<{ data: Uint8Array }> = []

    for (let i = 0; i < schema.fields.length; i++) {
      collectBuffers(schema.fields[i], columnResults[i], fieldNodes, buffers)
    }

    // Calculate aligned offsets and build body
    let bodyLength = 0n
    const bufferDescriptors: Array<MessageEncoder.BufferData> = []

    for (const buf of buffers) {
      const alignedLength = BufferUtils.align8(buf.data.length)
      bufferDescriptors.push({
        offset: bodyLength,
        length: BigInt(buf.data.length)
      })
      bodyLength += BigInt(alignedLength)
    }

    // Concatenate buffers into body (with alignment padding)
    const body = new Uint8Array(Number(bodyLength))
    let offset = 0
    for (const buf of buffers) {
      body.set(buf.data, offset)
      offset += BufferUtils.align8(buf.data.length)
    }

    // Encode messages
    const schemaHeader = MessageEncoder.encodeSchemaMessage(schema)
    const recordBatchHeader = MessageEncoder.encodeRecordBatchMessage(
      BigInt(numRows),
      fieldNodes,
      bufferDescriptors,
      bodyLength
    )

    return {
      schemaFlightData: {
        dataHeader: schemaHeader,
        dataBody: new Uint8Array(0)
      },
      recordBatchFlightData: {
        dataHeader: recordBatchHeader,
        dataBody: body
      },
      expectedValues,
      schema
    }
  }).pipe(
    Effect.provide(Layer.mergeAll(
      Layer.succeed(Random.Random, Random.make(seed)),
      GeneratorRegistry.Live
    ))
  )
}

const collectBuffers = (
  field: ArrowField,
  result: Types.GeneratedBuffers,
  fieldNodes: Array<MessageEncoder.FieldNodeData>,
  buffers: Array<{ data: Uint8Array }>
): void => {
  // Add field node
  fieldNodes.push(result.fieldNode)

  // Add buffers based on type
  const typeId = field.type.typeId

  // Validity bitmap (most types have this)
  if (typeId !== "null") {
    buffers.push({ data: result.validity })
  }

  // Offset buffer (variable-length and some nested types)
  if (result.offsets !== null) {
    buffers.push({ data: result.offsets })
  }

  // Data buffer (for types that have inline data)
  if (hasDataBuffer(typeId)) {
    buffers.push({ data: result.data })
  }

  // Recurse into children
  for (let i = 0; i < result.children.length; i++) {
    const childField = field.children[i]
    collectBuffers(childField, result.children[i], fieldNodes, buffers)
  }
}

const hasDataBuffer = (typeId: string): boolean => {
  switch (typeId) {
    case "null":
    case "list":
    case "large-list":
    case "fixed-size-list":
    case "struct":
    case "map":
    case "union":
      return false
    default:
      return true
  }
}

const getFieldConfig = (
  fieldName: string,
  options: Types.BaseGeneratorOptions
): Types.FieldGeneratorConfig => {
  const fieldConfig = options.fields?.[fieldName] ?? {}
  return {
    nullRate: fieldConfig.nullRate ?? options.defaultNullRate ?? 0.2,
    ...fieldConfig
  }
}

/**
 * Generate multiple record batches with the same schema.
 *
 * Useful for testing streaming/chunked scenarios where data arrives in batches.
 */
export const generateMultiBatchFlightData = (
  schema: ArrowSchema,
  options: Types.MultiBatchGeneratorOptions
): Effect.Effect<Types.GeneratedMultiBatchFlightData> => {
  const rowsPerBatch = Array.isArray(options.rowsPerBatch)
    ? options.rowsPerBatch
    : [options.rowsPerBatch]
  const seed = typeof options.seed === "string" ? hashString(options.seed) : (options.seed ?? 42)

  return Effect.gen(function*() {
    const registry = yield* Types.GeneratorRegistry

    const batches: Array<Types.GeneratedBatch> = []
    let totalRows = 0

    for (let batchIndex = 0; batchIndex < rowsPerBatch.length; batchIndex++) {
      const numRows = rowsPerBatch[batchIndex]
      totalRows += numRows

      // Generate data for each field
      const columnResults: Array<Types.GeneratorResult> = []
      const expectedValues: Record<string, ReadonlyArray<unknown>> = {}

      for (const field of schema.fields) {
        const config = getFieldConfig(field.name, options)
        const generator = registry.getGenerator(field.type.typeId)
        const result = yield* generator.generate(field, numRows, config)
        columnResults.push(result)
        expectedValues[field.name] = result.values
      }

      // Collect field nodes and buffers
      const fieldNodes: Array<MessageEncoder.FieldNodeData> = []
      const buffers: Array<{ data: Uint8Array }> = []

      for (let i = 0; i < schema.fields.length; i++) {
        collectBuffers(schema.fields[i], columnResults[i], fieldNodes, buffers)
      }

      // Calculate aligned offsets and build body
      let bodyLength = 0n
      const bufferDescriptors: Array<MessageEncoder.BufferData> = []

      for (const buf of buffers) {
        const alignedLength = BufferUtils.align8(buf.data.length)
        bufferDescriptors.push({
          offset: bodyLength,
          length: BigInt(buf.data.length)
        })
        bodyLength += BigInt(alignedLength)
      }

      // Concatenate buffers into body
      const body = new Uint8Array(Number(bodyLength))
      let offset = 0
      for (const buf of buffers) {
        body.set(buf.data, offset)
        offset += BufferUtils.align8(buf.data.length)
      }

      // Encode record batch message
      const recordBatchHeader = MessageEncoder.encodeRecordBatchMessage(
        BigInt(numRows),
        fieldNodes,
        bufferDescriptors,
        bodyLength
      )

      batches.push({
        flightData: {
          dataHeader: recordBatchHeader,
          dataBody: body
        },
        expectedValues,
        numRows
      })
    }

    // Encode schema message
    const schemaHeader = MessageEncoder.encodeSchemaMessage(schema)

    return {
      schemaFlightData: {
        dataHeader: schemaHeader,
        dataBody: new Uint8Array(0)
      },
      batches,
      schema,
      totalRows
    }
  }).pipe(
    Effect.provide(Layer.mergeAll(
      Layer.succeed(Random.Random, Random.make(seed)),
      GeneratorRegistry.Live
    ))
  )
}

const hashString = (str: string): number => {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash // Convert to 32-bit integer
  }
  return Math.abs(hash)
}
