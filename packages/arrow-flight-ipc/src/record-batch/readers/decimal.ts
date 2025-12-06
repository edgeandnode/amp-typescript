import { createValidityChecker } from "./validity.ts"

const bytesToBigInt = (bytes: Uint8Array, signed: boolean): bigint => {
  let result = 0n
  for (let i = bytes.length - 1; i >= 0; i--) {
    result = (result << 8n) | BigInt(bytes[i])
  }
  if (signed && bytes.length > 0 && (bytes[bytes.length - 1] & 0x80) !== 0) {
    result = result - (1n << BigInt(bytes.length * 8))
  }
  return result
}

const formatDecimal = (value: bigint, scale: number): string => {
  const isNegative = value < 0n
  const absValue = isNegative ? -value : value
  const str = absValue.toString()
  if (scale === 0) return isNegative ? `-${str}` : str
  const paddedStr = str.padStart(scale + 1, "0")
  const intPart = paddedStr.slice(0, -scale) || "0"
  const fracPart = paddedStr.slice(-scale)
  return `${isNegative ? "-" : ""}${intPart}.${fracPart}`
}

export const readDecimalValues = (
  validityBuffer: Uint8Array,
  dataBuffer: Uint8Array,
  length: number,
  precision: number,
  scale: number,
  bitWidth: number
): ReadonlyArray<string | null> => {
  const validityChecker = createValidityChecker(validityBuffer)
  const byteWidth = bitWidth / 8
  const values: Array<string | null> = []
  for (let i = 0; i < length; i++) {
    if (validityChecker(i)) {
      const start = i * byteWidth
      const bigIntValue = bytesToBigInt(dataBuffer.subarray(start, start + byteWidth), true)
      values.push(formatDecimal(bigIntValue, scale))
    } else {
      values.push(null)
    }
  }
  return values
}
