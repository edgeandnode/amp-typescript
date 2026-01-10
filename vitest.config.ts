import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    globals: true,
    projects: [
      "packages/*/vitest.config.ts",
      "packages/tools/*/vitest.config.ts"
    ]
  }
})
