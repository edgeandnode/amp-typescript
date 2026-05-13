import * as Schema from "effect/Schema"

/**
 * An error which occurs when attempting to parse an invalid Arrow data type.
 *
 * @internal
 */
export class InvalidArrowDataTypeError extends Schema.TaggedErrorClass<InvalidArrowDataTypeError>(
  "Amp/InvalidArrowDataTypeError"
)("InvalidArrowDataTypeError", {
  type: Schema.Number,
  offset: Schema.Number
}) {}

/**
 * An error which occurs when the expected FlatBuffer message header type is not
 * valid.
 *
 * @internal
 */
export class InvalidMessageTypeError extends Schema.TaggedErrorClass<InvalidMessageTypeError>(
  "Amp/InvalidMessageTypeError"
)("InvalidMessageTypeError", {
  value: Schema.Number
}) {
  override get message(): string {
    return `Received invalid value for Arrow Flight message type: ${this.value} `
  }
}

/**
 * An error which occurs when attempting to access a FlatBuffers field which
 * does not exist.
 *
 * @internal
 */
export class MissingFieldError extends Schema.TaggedErrorClass<MissingFieldError>(
  "Amp/MissingFieldError"
)("MissingFieldError", {
  fieldName: Schema.String,
  fieldIndex: Schema.Number,
  tableOffset: Schema.Number
}) {
  override get message(): string {
    return `Failed to find message field '${this.fieldName}' at index ${this.fieldIndex} (offset: ${this.tableOffset})`
  }
}

/**
 * An error which occurs when the expected FlatBuffer message header type is not
 * the same as the received message header type.
 *
 * @internal
 */
export class UnexpectedMessageTypeError extends Schema.TaggedErrorClass<UnexpectedMessageTypeError>(
  "Amp/UnexpectedMessageTypeError"
)("UnexpectedMessageTypeError", {
  expected: Schema.Number,
  received: Schema.Number
}) {
  override get message(): string {
    return `Expected to receive message header type ${this.expected} - received: ${this.received}`
  }
}
