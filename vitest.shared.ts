import * as path from "node:path"

import type { ViteUserConfig } from "vitest/config"

const alias = (dir: string, name = `@edgeandnode/${dir}`) => ({
  [`${name}/test`]: path.join(__dirname, "packages", dir, "test"),
  [`${name}`]: path.join(__dirname, "packages", dir, "src")
})

const config: ViteUserConfig = {
  test: {
    alias: {
      ...alias("arrow-flight-ipc")
    },
    watch: false,
    globals: true,
    environment: "node",
    include: ["test/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}"],
    reporters: ["default"],
    coverage: {
      reportsDirectory: "./test-output/vitest/coverage",
      provider: "v8" as const
    }
  }
}

export default config
