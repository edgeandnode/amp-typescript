import * as Command from "@effect/cli/Command"
import { LoginCommand } from "./auth/login.ts"
import { LogoutCommand } from "./auth/logout.ts"
import { TokenCommand } from "./auth/token.ts"

export const AuthCommand = Command.make("auth").pipe(
  Command.withDescription("Commands used to login to, logout of, and obtain tokens to interact with Amp."),
  Command.withSubcommands([LoginCommand, LogoutCommand, TokenCommand])
)
