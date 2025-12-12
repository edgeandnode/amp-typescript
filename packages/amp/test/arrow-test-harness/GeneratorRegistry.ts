/**
 * Generator Registry - maps type IDs to data generators
 * @internal
 */
import * as Layer from "effect/Layer"
import * as Decimal from "./generators/decimal.ts"
import * as Nested from "./generators/nested.ts"
import * as Primitives from "./generators/primitives.ts"
import * as Strings from "./generators/strings.ts"
import * as Temporal from "./generators/temporal.ts"
import * as Types from "./Types.ts"

const generators: Record<string, Types.DataGenerator> = {
  // Primitives (typeId values from Schema.ts)
  "null": Primitives.nullGenerator,
  "bool": Primitives.boolGenerator,
  "int": Primitives.intGenerator,
  "float": Primitives.floatGenerator,

  // Decimal
  "decimal": Decimal.decimalGenerator,

  // Strings & Binary
  "utf8": Strings.utf8Generator,
  "large-utf8": Strings.largeUtf8Generator,
  "binary": Strings.binaryGenerator,
  "large-binary": Strings.largeBinaryGenerator,
  "fixed-size-binary": Strings.fixedSizeBinaryGenerator,

  // Temporal
  "date": Temporal.dateGenerator,
  "time": Temporal.timeGenerator,
  "timestamp": Temporal.timestampGenerator,
  "duration": Temporal.durationGenerator,
  "interval": Temporal.intervalGenerator,

  // Nested
  "list": Nested.listGenerator,
  "large-list": Nested.largeListGenerator,
  "fixed-size-list": Nested.fixedSizeListGenerator,
  "struct": Nested.structGenerator,
  "map": Nested.mapGenerator,
  "union": Nested.unionGenerator
}

const getGenerator = (typeId: string): Types.DataGenerator => {
  const generator = generators[typeId]
  if (!generator) {
    throw new Error(`No generator for type: ${typeId}`)
  }
  return generator
}

/**
 * Live layer providing the GeneratorRegistry service.
 */
export const Live: Layer.Layer<Types.GeneratorRegistry> = Layer.succeed(
  Types.GeneratorRegistry,
  Types.GeneratorRegistry.of({ getGenerator })
)
