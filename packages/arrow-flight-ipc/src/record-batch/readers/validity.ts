import type { Predicate } from "effect/Predicate"

/**
 * Read a validity bitmap and return a function to check if index is valid.
 */
export const createValidityChecker = (validityBuffer: Uint8Array): Predicate<number> => {
  if (validityBuffer.length === 0) {
    // No validity buffer means all values are valid
    return () => true
  }

  return (index) => {
    const byteIndex = Math.floor(index / 8)
    const bitIndex = index % 8
    return (validityBuffer[byteIndex] & (1 << bitIndex)) !== 0
  }
}
