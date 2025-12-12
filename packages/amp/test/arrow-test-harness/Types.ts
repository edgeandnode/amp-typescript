/**
 * Arrow Test Harness Types
 *
 * Shared interfaces for the Arrow RecordBatch test data producer.
 *
 * @internal
 */
import type { ArrowField, ArrowSchema, FlightData } from "@edgeandnode/amp/internal/arrow-flight-ipc/Schema"
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import type * as Random from "effect/Random"

// Re-export FlightData for convenience
export type { FlightData }

// =============================================================================
// Generator Configuration
// =============================================================================

/**
 * Configuration for generating test data for a single field.
 */
export interface FieldGeneratorConfig {
  /** Probability of generating null values (0-1). Default: 0.2 */
  readonly nullRate?: number
  /** Minimum number of items for variable-length types (strings, lists). Default: 0 */
  readonly minLength?: number
  /** Maximum number of items for variable-length types. Default: varies by type */
  readonly maxLength?: number
  /** Custom value generator function (overrides default random generation) */
  readonly valueGenerator?: (index: number) => Effect.Effect<unknown, never, Random.Random>
  /** Include special float values (NaN, Infinity, -Infinity, -0). Default: false */
  readonly includeSpecialFloats?: boolean
  /** Probability of generating a special float value when includeSpecialFloats is true. Default: 0.1 */
  readonly specialFloatRate?: number
}

/**
 * Configuration for nested field generation (lists, structs, maps, unions).
 */
export interface NestedFieldConfig extends FieldGeneratorConfig {
  /** Configuration for child fields, keyed by field name */
  readonly children?: Record<string, FieldGeneratorConfig | NestedFieldConfig>
}

/**
 * Base options shared by all generator option types.
 */
export interface BaseGeneratorOptions {
  /** Seed for deterministic random generation. Default: 42 */
  readonly seed?: number | string
  /** Per-field configuration, keyed by field name */
  readonly fields?: Record<string, FieldGeneratorConfig | NestedFieldConfig>
  /** Global default null rate for all fields. Default: 0.2 */
  readonly defaultNullRate?: number
}

/**
 * Options for generating FlightData.
 */
export interface FlightDataGeneratorOptions extends BaseGeneratorOptions {
  /** Number of rows to generate. Default: 100 */
  readonly numRows?: number
}

/**
 * Options for generating multi-batch FlightData.
 */
export interface MultiBatchGeneratorOptions extends BaseGeneratorOptions {
  /** Number of rows per batch. Can be a single number (same for all) or array. */
  readonly rowsPerBatch: number | ReadonlyArray<number>
}

// =============================================================================
// Generated Data Types
// =============================================================================

/**
 * Result of generating test FlightData.
 */
export interface GeneratedFlightData {
  /** FlightData containing Schema message */
  readonly schemaFlightData: FlightData
  /** FlightData containing RecordBatch message */
  readonly recordBatchFlightData: FlightData
  /** The generated values for verification, keyed by field name */
  readonly expectedValues: Record<string, ReadonlyArray<unknown>>
  /** The schema used for generation */
  readonly schema: ArrowSchema
}

/**
 * A single generated record batch with its expected values.
 */
export interface GeneratedBatch {
  /** FlightData containing RecordBatch message */
  readonly flightData: FlightData
  /** The generated values for verification, keyed by field name */
  readonly expectedValues: Record<string, ReadonlyArray<unknown>>
  /** Number of rows in this batch */
  readonly numRows: number
}

/**
 * Result of generating multi-batch test FlightData.
 */
export interface GeneratedMultiBatchFlightData {
  /** FlightData containing Schema message */
  readonly schemaFlightData: FlightData
  /** Array of generated record batches */
  readonly batches: ReadonlyArray<GeneratedBatch>
  /** The schema used for generation */
  readonly schema: ArrowSchema
  /** Total number of rows across all batches */
  readonly totalRows: number
}

/**
 * Buffer layout and metadata for a generated column.
 */
export interface GeneratedBuffers {
  /** Validity bitmap (empty Uint8Array if all values are valid) */
  readonly validity: Uint8Array
  /** Offset buffer for variable-length types (null if not applicable) */
  readonly offsets: Uint8Array | null
  /** Data buffer containing the actual values */
  readonly data: Uint8Array
  /** Child buffers for nested types (empty array for leaf types) */
  readonly children: ReadonlyArray<GeneratedBuffers>
  /** Field node metadata (length and null count) */
  readonly fieldNode: FieldNodeInfo
}

/**
 * Field node information for RecordBatch metadata.
 */
export interface FieldNodeInfo {
  /** Number of values in this field */
  readonly length: bigint
  /** Number of null values */
  readonly nullCount: bigint
}

/**
 * Result from a data generator including both buffers and decoded values.
 */
export interface GeneratorResult extends GeneratedBuffers {
  /** The generated values (for test verification) */
  readonly values: ReadonlyArray<unknown>
}

// =============================================================================
// Generator Registry Service
// =============================================================================

/**
 * Service for looking up data generators by Arrow type ID.
 *
 * This eliminates the circular dependency between the registry and nested
 * generators by using Effect's service pattern for dependency injection.
 */
export interface GeneratorRegistry {
  readonly getGenerator: (typeId: string) => DataGenerator
}

export const GeneratorRegistry = Context.GenericTag<GeneratorRegistry>("GeneratorRegistry")

// =============================================================================
// Data Generator Interface
// =============================================================================

/**
 * Interface for type-specific data generators.
 *
 * Generators use Effect's Random service for deterministic random generation.
 * Nested type generators also require the GeneratorRegistry service to look up
 * child type generators.
 */
export interface DataGenerator {
  /**
   * Generate buffers and values for a specific Arrow data type.
   *
   * @param field - The Arrow field definition
   * @param numRows - Number of rows to generate
   * @param config - Field generation configuration
   * @returns Effect that produces generated buffers and expected values
   */
  generate(
    field: ArrowField,
    numRows: number,
    config: FieldGeneratorConfig
  ): Effect.Effect<GeneratorResult, never, Random.Random | GeneratorRegistry>
}
