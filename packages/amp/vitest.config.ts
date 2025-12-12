/// <reference types="vitest" />

import { mergeConfig, type ViteUserConfigExport } from "vitest/config"

import shared from "../../vitest.shared.ts"

const config: ViteUserConfigExport = {
  root: __dirname,
  cacheDir: "../../node_modules/.vite/@edgeandnode/amp",
  test: {
    ...shared.test
  }
}

export default mergeConfig(shared, config)
