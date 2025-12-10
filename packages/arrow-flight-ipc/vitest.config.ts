import { defineConfig } from "vitest/config"

import config from "../../vitest.shared.ts"

export default defineConfig({
  test: {
    ...(config.test ?? {}),
    include: ["test/**/*.test.ts"],
    environment: "node"
  }
})
