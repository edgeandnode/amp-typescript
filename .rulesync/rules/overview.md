---
root: true
targets: ["*"]
description: "Project overview, architecture, and development guidelines"
globs: ["**/*"]
---

# Amp TypeScript SDK

## What Is This

A pnpm workspace monorepo for the **Amp TypeScript SDK** — a toolkit for building and managing blockchain datasets, built on the [Effect](https://effect.website) library.

| Package                | Path                  | Description                                       |
| ---------------------- | --------------------- | ------------------------------------------------- |
| `@edgeandnode/amp`     | `packages/amp/`       | Core SDK                                          |
| `@edgeandnode/amp-cli` | `packages/cli/`       | CLI tool (`amp` command) via `@effect/cli`        |
| `@amp/oxc`             | `packages/tools/oxc/` | Custom oxlint rules for Effect import conventions |

## Core SDK Module Map (`packages/amp/src/`)

```
src/
├── index.ts                     # Package entrypoint (namespace re-exports)
├── core.ts            → core/   # Domain models: branded types, schemas (BlockRange, Network, AuthInfo, Dataset*, etc.)
├── arrow-flight.ts    → arrow-flight/   # Arrow Flight SQL client (Transport, ArrowFlight service, errors)
├── protocol-stream.ts → protocol-stream/  # Stateless reorg detection on top of ArrowFlight streams
├── transactional-stream.ts → transactional-stream/  # Exactly-once semantics, crash recovery, commit control
├── auth/                        # OAuth2 auth (device flow, token refresh, caching)
├── admin/                       # Admin API (datasets, jobs, workers, providers)
├── registry/                    # Registry API (dataset discovery)
├── manifest-builder/            # Dataset manifest construction
├── protobuf/                    # Generated protobuf (Flight, FlightSql)
└── internal/                    # Private: Arrow IPC parsing, PKCE
```

Each public module has a **root-level barrel `.ts` file** in `src/` that re-exports from its subdirectory. No `index.ts` barrel files in subdirectories. `src/internal/` is blocked from external import.

## Commands

```bash
pnpm check                    # Type check root project
pnpm check:recursive          # Type check all packages
pnpm lint                     # Check with oxlint + dprint
pnpm lint:fix                 # Auto-fix with oxlint + dprint
pnpm test                     # Run all tests (vitest)
pnpm vitest run <file>        # Single test file
pnpm build                    # Full build (tsc + babel)
```

## Code Style

- **Formatter**: dprint — no semicolons (ASI), double quotes, no trailing commas, 120 char line width
- **Linter**: oxlint with custom `@amp/oxc` plugin
- **Array syntax**: `Array<T>` and `ReadonlyArray<T>`, never `T[]` (enforced by `typescript/array-type`)
- **Effect imports**: Always namespace imports from subpaths — `import * as Effect from "effect/Effect"`, never `from "effect"`
- **Type imports**: Inline — `import { type Foo, bar } from "..."` (enforced by `typescript/consistent-type-imports`)
- **TypeScript strictness**: `strict`, `exactOptionalPropertyTypes`, `noUnusedLocals`, `verbatimModuleSyntax`
- **File extensions**: `.ts` in all imports (rewritten to `.js` at build time)
- **Unused variables**: Prefix with `_`

## Agent Rules

All coding agent rules are managed via [rulesync](https://github.com/dyoshikawa/rulesync) in `.rulesync/rules/`. Edit rules there, then run `pnpm exec rulesync generate` to regenerate agent-specific config files. Do not edit generated files (e.g. `.claude/rules/`, `.opencode/rules/`) directly.
