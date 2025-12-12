/**
 * Arrow FlightData Roundtrip Tests
 *
 * Tests that generated FlightData can be parsed by the arrow-flight-ipc package
 * and that decoded values match the expected generated values.
 */
import { decodeRecordBatch } from "@edgeandnode/amp/internal/arrow-flight-ipc/Decoder"
import { parseRecordBatch } from "@edgeandnode/amp/internal/arrow-flight-ipc/RecordBatch"
import { parseSchema } from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import { describe, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as FlightDataGenerator from "../arrow-test-harness/FlightDataGenerator.ts"
import * as SchemaBuilder from "../arrow-test-harness/SchemaBuilder.ts"
import { formatComparisonErrors, verifyDecodedValues } from "../arrow-test-harness/ValueComparison.ts"

describe("FlightData roundtrip", () => {
  it.effect("a simple int32 column", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 42,
        defaultNullRate: 0
      })

      // Parse the schema message
      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      expect(parsedSchema.fields.length).toBe(1)
      expect(parsedSchema.fields[0].name).toBe("id")
      expect(parsedSchema.fields[0].type.typeId).toBe("int")

      // Parse the record batch message
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      expect(recordBatch.length).toBe(10n)

      // Decode the record batch
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)
      expect(decoded.numRows).toBe(10n)
      expect(decoded.columns.length).toBe(1)

      // Verify decoded values match expected values
      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("nullable int32 column", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 20,
        seed: 123,
        defaultNullRate: 0.3
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // Verify null count is tracked correctly
      const idColumn = decoded.getColumn("id")!
      const expectedNulls = generated.expectedValues.id.filter((v) => v === null).length
      expect(idColumn.node.nullCount).toBe(BigInt(expectedNulls))

      // Verify all values match
      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("bool column", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .bool("flag")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 16,
        seed: 456,
        defaultNullRate: 0
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // Verify values match
      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)

      // Should have a mix of true and false
      const expectedValues = generated.expectedValues.flag
      expect(expectedValues.some((v) => v === true)).toBe(true)
      expect(expectedValues.some((v) => v === false)).toBe(true)
    }))

  it.effect("utf8 column", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .utf8("name")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 5,
        seed: 789,
        defaultNullRate: 0
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // Verify values match
      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("multiple columns", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .bool("active")
        .int64("score")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 999,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      expect(parsedSchema.fields.length).toBe(4)
      expect(parsedSchema.fields[0].name).toBe("id")
      expect(parsedSchema.fields[1].name).toBe("name")
      expect(parsedSchema.fields[2].name).toBe("active")
      expect(parsedSchema.fields[3].name).toBe("score")

      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      expect(recordBatch.length).toBe(10n)

      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)
      expect(decoded.columns.length).toBe(4)

      // Verify each column has correct structure
      const idColumn = decoded.getColumn("id")!
      expect(idColumn.buffers.length).toBe(2) // validity + data

      const nameColumn = decoded.getColumn("name")!
      expect(nameColumn.buffers.length).toBe(3) // validity + offsets + data

      const activeColumn = decoded.getColumn("active")!
      expect(activeColumn.buffers.length).toBe(2) // validity + data

      const scoreColumn = decoded.getColumn("score")!
      expect(scoreColumn.buffers.length).toBe(2) // validity + data

      // Verify all values match
      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("all integer types", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int8("i8")
        .int16("i16")
        .int32("i32")
        .int64("i64")
        .uint8("u8")
        .uint16("u16")
        .uint32("u32")
        .uint64("u64")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 15,
        seed: 111,
        defaultNullRate: 0.15
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("float types", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .float32("f32")
        .float64("f64")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 20,
        seed: 222,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("binary types", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .binary("bin")
        .fixedSizeBinary("fixed", 8)
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 333,
        defaultNullRate: 0.2
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("decimal type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .decimal("amount", 10, 2)
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 444,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("temporal types", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .dateDay("day")
        .dateMillisecond("ms_date")
        .timestamp("ts")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 555,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("list type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .list("items", (b) => b.int32())
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 8,
        seed: 666,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("struct type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .struct("person", (b) =>
          b
            .utf8("name")
            .int32("age"))
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 8,
        seed: 777,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles empty record batch (0 rows)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .bool("active")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 0,
        seed: 1000
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      expect(parsedSchema.fields.length).toBe(3)

      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      expect(recordBatch.length).toBe(0n)

      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)
      expect(decoded.numRows).toBe(0n)
      expect(decoded.columns.length).toBe(3)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles empty strings (not null)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .utf8("text")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 1001,
        defaultNullRate: 0,
        fields: {
          text: { minLength: 0, maxLength: 0 }
        }
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // All values should be empty strings, not null
      expect(generated.expectedValues.text.every((v) => v === "")).toBe(true)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles empty binary (not null)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .binary("data")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 1002,
        defaultNullRate: 0,
        fields: {
          data: { minLength: 0, maxLength: 0 }
        }
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // All values should be empty Uint8Arrays
      expect(generated.expectedValues.data.every((v) => v instanceof Uint8Array && v.length === 0)).toBe(true)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles all-null columns", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 1003,
        defaultNullRate: 1.0 // 100% nulls
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // Verify all values are null
      expect(generated.expectedValues.id.every((v) => v === null)).toBe(true)
      expect(generated.expectedValues.name.every((v) => v === null)).toBe(true)

      // Verify null counts
      expect(decoded.getColumn("id")!.node.nullCount).toBe(10n)
      expect(decoded.getColumn("name")!.node.nullCount).toBe(10n)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles empty lists (not null)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .list("items", (b) => b.int32())
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 5,
        seed: 1004,
        defaultNullRate: 0,
        fields: {
          items: { minLength: 0, maxLength: 0 }
        }
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // All values should be empty arrays
      expect(generated.expectedValues.items.every((v) => Array.isArray(v) && v.length === 0)).toBe(true)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles deeply nested structures", ({ expect }) =>
    Effect.gen(function*() {
      // Struct containing a list of structs
      const testSchema = SchemaBuilder.schema()
        .struct("outer", (s) =>
          s
            .utf8("name")
            .list("items", (l) => l.int32())
            .struct("nested", (n) =>
              n
                .int32("value")
                .utf8("label")))
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 5,
        seed: 1005,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles single row", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 1,
        seed: 1006,
        defaultNullRate: 0
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      expect(decoded.numRows).toBe(1n)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles large number of rows", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10000,
        seed: 1007,
        defaultNullRate: 0.05
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      expect(decoded.numRows).toBe(10000n)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles large number of columns", ({ expect }) =>
    Effect.gen(function*() {
      // Create schema with 50 columns
      let builder = SchemaBuilder.schema()
      for (let i = 0; i < 50; i++) {
        builder = builder.int32(`col_${i}`)
      }
      const testSchema = builder.build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 100,
        seed: 1008,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      expect(parsedSchema.fields.length).toBe(50)

      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      expect(decoded.columns.length).toBe(50)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("handles map type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .map("attributes", (k) => k.utf8(), (v) => v.int32())
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 5,
        seed: 1009,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("multiple batches with varying row counts", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .bool("active")
        .build()

      const generated = yield* FlightDataGenerator.generateMultiBatchFlightData(testSchema, {
        rowsPerBatch: [5, 20, 1, 100, 50],
        seed: 2001,
        defaultNullRate: 0.15
      })

      expect(generated.batches.length).toBe(5)
      expect(generated.totalRows).toBe(176)

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)

      for (let i = 0; i < generated.batches.length; i++) {
        const batch = generated.batches[i]
        const recordBatch = yield* parseRecordBatch(batch.flightData)
        const decoded = decodeRecordBatch(recordBatch, batch.flightData.dataBody, parsedSchema)

        const comparison = verifyDecodedValues(testSchema, decoded, batch.expectedValues)
        expect(comparison.success, `Batch ${i}: ${formatComparisonErrors(comparison.errors)}`).toBe(true)
      }
    }))

  it.effect("batch with empty batch in middle", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .utf8("name")
        .build()

      const generated = yield* FlightDataGenerator.generateMultiBatchFlightData(testSchema, {
        rowsPerBatch: [10, 0, 10],
        seed: 2002,
        defaultNullRate: 0
      })

      expect(generated.batches.length).toBe(3)
      expect(generated.totalRows).toBe(20)
      expect(generated.batches[1].numRows).toBe(0)

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)

      for (let i = 0; i < generated.batches.length; i++) {
        const batch = generated.batches[i]
        const recordBatch = yield* parseRecordBatch(batch.flightData)
        const decoded = decodeRecordBatch(recordBatch, batch.flightData.dataBody, parsedSchema)

        const comparison = verifyDecodedValues(testSchema, decoded, batch.expectedValues)
        expect(comparison.success, `Batch ${i}: ${formatComparisonErrors(comparison.errors)}`).toBe(true)
      }
    }))

  it.effect("many small batches", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("value")
        .build()

      // 100 batches of 10 rows each
      const generated = yield* FlightDataGenerator.generateMultiBatchFlightData(testSchema, {
        rowsPerBatch: Array(100).fill(10),
        seed: 2003,
        defaultNullRate: 0.05
      })

      expect(generated.batches.length).toBe(100)
      expect(generated.totalRows).toBe(1000)

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)

      for (let i = 0; i < generated.batches.length; i++) {
        const batch = generated.batches[i]
        const recordBatch = yield* parseRecordBatch(batch.flightData)
        const decoded = decodeRecordBatch(recordBatch, batch.flightData.dataBody, parsedSchema)

        const comparison = verifyDecodedValues(testSchema, decoded, batch.expectedValues)
        expect(comparison.success, `Batch ${i}: ${formatComparisonErrors(comparison.errors)}`).toBe(true)
      }
    }))

  it.effect("multi-batch with complex nested types", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .int32("id")
        .list("tags", (b) => b.utf8())
        .struct("metadata", (s) =>
          s
            .utf8("key")
            .int32("count"))
        .build()

      const generated = yield* FlightDataGenerator.generateMultiBatchFlightData(testSchema, {
        rowsPerBatch: [5, 10, 5],
        seed: 2004,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)

      for (let i = 0; i < generated.batches.length; i++) {
        const batch = generated.batches[i]
        const recordBatch = yield* parseRecordBatch(batch.flightData)
        const decoded = decodeRecordBatch(recordBatch, batch.flightData.dataBody, parsedSchema)

        const comparison = verifyDecodedValues(testSchema, decoded, batch.expectedValues)
        expect(comparison.success, `Batch ${i}: ${formatComparisonErrors(comparison.errors)}`).toBe(true)
      }
    }))

  it.effect("float32 with special values (NaN, Infinity, -0)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .float32("value")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 100,
        seed: 3000,
        defaultNullRate: 0.1,
        fields: {
          value: { includeSpecialFloats: true, specialFloatRate: 0.2 }
        }
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // Verify we have some special values
      const values = generated.expectedValues.value as Array<number | null>
      const hasNaN = values.some((v) => v !== null && Number.isNaN(v))
      const hasInfinity = values.some((v) => v === Infinity || v === -Infinity)

      expect(hasNaN || hasInfinity).toBe(true)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("float64 with special values", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .float64("value")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 100,
        seed: 3001,
        defaultNullRate: 0.1,
        fields: {
          value: { includeSpecialFloats: true, specialFloatRate: 0.2 }
        }
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("floats with extreme values", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .float32("f32")
        .float64("f64")
        .build()

      // Generate with special floats to test extreme values like MAX_VALUE, MIN_VALUE
      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 200,
        seed: 3002,
        defaultNullRate: 0,
        fields: {
          f32: { includeSpecialFloats: true, specialFloatRate: 0.3 },
          f64: { includeSpecialFloats: true, specialFloatRate: 0.3 }
        }
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("float32 with wide range values", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .float32("value")
        .build()

      // Test without special floats but with the expanded range
      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 100,
        seed: 3003,
        defaultNullRate: 0
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      // Verify values span a wide range (not just -1000 to 1000)
      const values = generated.expectedValues.value as Array<number>
      const maxAbs = Math.max(...values.map(Math.abs))
      expect(maxAbs).toBeGreaterThan(1000)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("float16 type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .float16("half")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 20,
        seed: 4000,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("large-utf8 type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .largeUtf8("bigText")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4001,
        defaultNullRate: 0.2
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("large-binary type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .largeBinary("bigData")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4002,
        defaultNullRate: 0.2
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("large-list type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .largeList("bigList", (b) => b.int32())
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 8,
        seed: 4003,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("fixed-size-list type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .fixedSizeList("fixed3", 3, (b) => b.int32())
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4004,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("time types (all units)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .timeSecond("timeSec")
        .timeMillisecond("timeMs")
        .timeMicrosecond("timeUs")
        .timeNanosecond("timeNs")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4005,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("duration type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .duration("durSec", "SECOND")
        .duration("durMs", "MILLISECOND")
        .duration("durUs", "MICROSECOND")
        .duration("durNs", "NANOSECOND")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4006,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("interval types (all units)", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .intervalYearMonth("intervalYM")
        .intervalDayTime("intervalDT")
        .intervalMonthDayNano("intervalMDN")
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4007,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("sparse union type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .union("choice", "SPARSE", (u) =>
          u
            .variant("intVal", (b) => b.int32())
            .variant("strVal", (b) => b.utf8()))
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 10,
        seed: 4008,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))

  it.effect("dense union type", ({ expect }) =>
    Effect.gen(function*() {
      const testSchema = SchemaBuilder.schema()
        .union("tagged", "DENSE", (u) =>
          u
            .variant("number", (b) => b.float64())
            .variant("text", (b) => b.utf8())
            .variant("flag", (b) => b.bool()))
        .build()

      const generated = yield* FlightDataGenerator.generateFlightData(testSchema, {
        numRows: 15,
        seed: 4009,
        defaultNullRate: 0.1
      })

      const parsedSchema = yield* parseSchema(generated.schemaFlightData)
      const recordBatch = yield* parseRecordBatch(generated.recordBatchFlightData)
      const decoded = decodeRecordBatch(recordBatch, generated.recordBatchFlightData.dataBody, parsedSchema)

      const comparison = verifyDecodedValues(testSchema, decoded, generated.expectedValues)
      expect(comparison.success, formatComparisonErrors(comparison.errors)).toBe(true)
    }))
})
