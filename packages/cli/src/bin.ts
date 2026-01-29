#!/usr/bin/env node

import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Cli } from "./cli.ts"

NodeRuntime.runMain(Cli, {
  disableErrorReporting: true,
  disablePrettyLogger: true
})
