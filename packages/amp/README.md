# Amp TypeScript SDK

## Known Issues

**Enabling TypeScript's `erasableSyntaxOnly` Feature**

For some completely absurd reason, the `@bufbuild/protoc-gen-es` plugin does not support generating enum values as anything other than TypeScript enums at this time, so we cannot yet enable this flag.

See the [related `protobuf-es` issue](https://github.com/bufbuild/protobuf-es/issues/1139) for more information.
