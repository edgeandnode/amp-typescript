/**
 * Nested Type Generators (List, Struct, Map, Union)
 * @internal
 */
import type * as Schema from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Effect from "effect/Effect"
import * as Random from "effect/Random"
import * as BufferUtils from "../BufferUtils.ts"
import * as Rand from "../RandomUtils.ts"
import * as Types from "../Types.ts"

// =============================================================================
// List Generator
// =============================================================================

export const listGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const registry = yield* Types.GeneratorRegistry
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minItems = config.minLength ?? 0
      const maxItems = config.maxLength ?? 10

      const childField = field.children[0]
      const childConfig = (config as Types.NestedFieldConfig).children?.[childField.name] ?? {}

      const values: Array<Array<unknown> | null> = []
      const offsets: Array<number> = [0]
      let currentOffset = 0

      // First pass: determine list lengths
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
          offsets.push(currentOffset)
        } else {
          // Handle case where minItems === maxItems (avoids empty range in nextIntBetween)
          const listLength = minItems === maxItems
            ? minItems
            : yield* Random.nextIntBetween(minItems, maxItems)
          currentOffset += listLength
          offsets.push(currentOffset)
          values.push([]) // placeholder
        }
      }

      // Generate child data
      const totalChildElements = currentOffset
      const childGenerator = registry.getGenerator(childField.type.typeId)
      const childResult = yield* childGenerator.generate(childField, totalChildElements, childConfig)

      // Populate list values from child values
      for (let i = 0; i < numRows; i++) {
        if (values[i] !== null) {
          const start = offsets[i]
          const end = offsets[i + 1]
          values[i] = childResult.values.slice(start, end) as Array<unknown>
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const offsetBuffer = BufferUtils.createInt32OffsetBuffer(offsets)

      return {
        validity: bitmap,
        offsets: offsetBuffer,
        data: new Uint8Array(0),
        children: [childResult],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Large List Generator
// =============================================================================

export const largeListGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const registry = yield* Types.GeneratorRegistry
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minItems = config.minLength ?? 0
      const maxItems = config.maxLength ?? 10

      const childField = field.children[0]
      const childConfig = (config as Types.NestedFieldConfig).children?.[childField.name] ?? {}

      const values: Array<Array<unknown> | null> = []
      const offsets: Array<bigint> = [0n]
      let currentOffset = 0n

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
          offsets.push(currentOffset)
        } else {
          // Handle case where minItems === maxItems (avoids empty range in nextIntBetween)
          const listLength = minItems === maxItems
            ? minItems
            : yield* Random.nextIntBetween(minItems, maxItems)
          currentOffset += BigInt(listLength)
          offsets.push(currentOffset)
          values.push([])
        }
      }

      const totalChildElements = Number(currentOffset)
      const childGenerator = registry.getGenerator(childField.type.typeId)
      const childResult = yield* childGenerator.generate(childField, totalChildElements, childConfig)

      for (let i = 0; i < numRows; i++) {
        if (values[i] !== null) {
          const start = Number(offsets[i])
          const end = Number(offsets[i + 1])
          values[i] = childResult.values.slice(start, end) as Array<unknown>
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const offsetBuffer = BufferUtils.createInt64OffsetBuffer(offsets)

      return {
        validity: bitmap,
        offsets: offsetBuffer,
        data: new Uint8Array(0),
        children: [childResult],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Fixed-Size List Generator
// =============================================================================

export const fixedSizeListGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const registry = yield* Types.GeneratorRegistry
      const type = field.type as Schema.FixedSizeListType
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0

      const childField = field.children[0]
      const childConfig = (config as Types.NestedFieldConfig).children?.[childField.name] ?? {}

      // Generate validity first
      const validity: Array<boolean> = []
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        validity.push(!isNull)
      }

      // Generate all child elements (even for null parents, they exist in memory)
      const totalChildElements = numRows * type.listSize
      const childGenerator = registry.getGenerator(childField.type.typeId)
      const childResult = yield* childGenerator.generate(childField, totalChildElements, childConfig)

      // Build values
      const values: Array<Array<unknown> | null> = []
      for (let i = 0; i < numRows; i++) {
        if (!validity[i]) {
          values.push(null)
        } else {
          const start = i * type.listSize
          const end = start + type.listSize
          values.push(childResult.values.slice(start, end) as Array<unknown>)
        }
      }

      const nullCount = validity.filter((v) => !v).length
      const bitmap = BufferUtils.createValidityBitmapFromFlags(validity)

      return {
        validity: bitmap,
        offsets: null,
        data: new Uint8Array(0),
        children: [childResult],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Struct Generator
// =============================================================================

export const structGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const registry = yield* Types.GeneratorRegistry
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const nestedConfig = config as Types.NestedFieldConfig

      // Determine row validity
      const validity: Array<boolean> = []
      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        validity.push(!isNull)
      }

      // Generate all child columns
      const childResults: Array<Types.GeneratorResult> = []
      for (const childField of field.children) {
        const childConfig = nestedConfig.children?.[childField.name] ?? {}
        const childGenerator = registry.getGenerator(childField.type.typeId)
        childResults.push(yield* childGenerator.generate(childField, numRows, childConfig))
      }

      // Build struct values
      const values: Array<Record<string, unknown> | null> = []
      for (let i = 0; i < numRows; i++) {
        if (!validity[i]) {
          values.push(null)
        } else {
          const struct: Record<string, unknown> = {}
          for (let j = 0; j < field.children.length; j++) {
            struct[field.children[j].name] = childResults[j].values[i]
          }
          values.push(struct)
        }
      }

      const nullCount = validity.filter((v) => !v).length
      const bitmap = BufferUtils.createValidityBitmapFromFlags(validity)

      return {
        validity: bitmap,
        offsets: null,
        data: new Uint8Array(0),
        children: childResults,
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Map Generator
// =============================================================================

export const mapGenerator: Types.DataGenerator = {
  generate: (field, numRows, config) =>
    Effect.gen(function*() {
      const registry = yield* Types.GeneratorRegistry
      const nullRate = field.nullable ? (config.nullRate ?? 0.2) : 0
      const minItems = config.minLength ?? 0
      const maxItems = config.maxLength ?? 5

      // Map has one child: entries struct with "key" and "value" fields
      const entriesField = field.children[0]
      const keyField = entriesField.children[0]
      const valueField = entriesField.children[1]

      const values: Array<Array<{ key: unknown; value: unknown }> | null> = []
      const offsets: Array<number> = [0]
      let currentOffset = 0

      for (let i = 0; i < numRows; i++) {
        const isNull = yield* Rand.nextBoolWithProbability(nullRate)
        if (isNull) {
          values.push(null)
          offsets.push(currentOffset)
        } else {
          // Handle case where minItems === maxItems (avoids empty range in nextIntBetween)
          const mapSize = minItems === maxItems
            ? minItems
            : yield* Random.nextIntBetween(minItems, maxItems)
          currentOffset += mapSize
          offsets.push(currentOffset)
          values.push([])
        }
      }

      // Generate keys and values
      const totalEntries = currentOffset
      const keyGenerator = registry.getGenerator(keyField.type.typeId)
      const valueGenerator = registry.getGenerator(valueField.type.typeId)

      const keyResult = yield* keyGenerator.generate(keyField, totalEntries, {})
      const valueResult = yield* valueGenerator.generate(valueField, totalEntries, {})

      // Build entries struct result
      const entriesValues: Array<{ key: unknown; value: unknown } | null> = []
      for (let i = 0; i < totalEntries; i++) {
        entriesValues.push({ key: keyResult.values[i], value: valueResult.values[i] })
      }

      const entriesResult: Types.GeneratorResult = {
        validity: new Uint8Array(0),
        offsets: null,
        data: new Uint8Array(0),
        children: [keyResult, valueResult],
        fieldNode: { length: BigInt(totalEntries), nullCount: 0n },
        values: entriesValues
      }

      // Populate map values
      for (let i = 0; i < numRows; i++) {
        if (values[i] !== null) {
          const start = offsets[i]
          const end = offsets[i + 1]
          values[i] = entriesValues.slice(start, end) as Array<{ key: unknown; value: unknown }>
        }
      }

      const { bitmap, nullCount } = BufferUtils.createValidityBitmap(values)
      const offsetBuffer = BufferUtils.createInt32OffsetBuffer(offsets)

      return {
        validity: bitmap,
        offsets: offsetBuffer,
        data: new Uint8Array(0),
        children: [entriesResult],
        fieldNode: { length: BigInt(numRows), nullCount: BigInt(nullCount) },
        values
      }
    })
}

// =============================================================================
// Union Generator
// =============================================================================

export const unionGenerator: Types.DataGenerator = {
  generate: (field, numRows) =>
    Effect.gen(function*() {
      const registry = yield* Types.GeneratorRegistry
      const type = field.type as Schema.UnionType
      const isSparse = type.mode === "SPARSE"

      // Generate type IDs for each row
      const typeIds: Array<number> = []
      for (let i = 0; i < numRows; i++) {
        const idx = yield* Random.nextIntBetween(0, type.typeIds.length - 1)
        typeIds.push(type.typeIds[idx])
      }

      const typeIdBuffer = BufferUtils.createTypeIdBuffer(typeIds)

      if (isSparse) {
        // Sparse union: all children have full length
        const childResults: Array<Types.GeneratorResult> = []
        for (const childField of field.children) {
          const childGenerator = registry.getGenerator(childField.type.typeId)
          childResults.push(yield* childGenerator.generate(childField, numRows, {}))
        }

        // Build values based on type IDs
        const typeIdToChildIdx = new Map(type.typeIds.map((id, idx) => [id, idx]))
        const values: Array<unknown> = []
        for (let i = 0; i < numRows; i++) {
          const childIdx = typeIdToChildIdx.get(typeIds[i])!
          values.push(childResults[childIdx].values[i])
        }

        return {
          validity: typeIdBuffer,
          offsets: null,
          data: new Uint8Array(0),
          children: childResults,
          fieldNode: { length: BigInt(numRows), nullCount: 0n },
          values
        }
      }

      // Dense union: children have variable lengths, need offset buffer
      const childCounts = new Map<number, number>()
      for (const id of type.typeIds) childCounts.set(id, 0)

      const denseOffsets: Array<number> = []
      for (let i = 0; i < numRows; i++) {
        const typeId = typeIds[i]
        denseOffsets.push(childCounts.get(typeId)!)
        childCounts.set(typeId, childCounts.get(typeId)! + 1)
      }

      const offsetBuffer = BufferUtils.createInt32OffsetBuffer(denseOffsets)

      // Generate child data with appropriate lengths
      const childResults: Array<Types.GeneratorResult> = []
      for (let i = 0; i < field.children.length; i++) {
        const childField = field.children[i]
        const childTypeId = type.typeIds[i]
        const childLength = childCounts.get(childTypeId)!
        const childGenerator = registry.getGenerator(childField.type.typeId)
        childResults.push(yield* childGenerator.generate(childField, childLength, {}))
      }

      // Build values
      const typeIdToChildIdx = new Map(type.typeIds.map((id, idx) => [id, idx]))
      const values: Array<unknown> = []
      for (let i = 0; i < numRows; i++) {
        const childIdx = typeIdToChildIdx.get(typeIds[i])!
        const offset = denseOffsets[i]
        values.push(childResults[childIdx].values[offset])
      }

      return {
        validity: typeIdBuffer,
        offsets: offsetBuffer,
        data: new Uint8Array(0),
        children: childResults,
        fieldNode: { length: BigInt(numRows), nullCount: 0n },
        values
      }
    })
}
