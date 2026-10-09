#!/usr/bin/env node

import path from "node:path";
import { fileURLToPath } from "node:url";
import { Command, InvalidArgumentError } from "commander";
import { DEFAULT_TENANT, validateTarget } from "./azure-cli.js";
import type { CliTarget } from "./contracts.js";
import { createRuntimeDependencies } from "./runtime.js";
import { AuthAutomationService } from "./auth-service.js";

interface CommonCliOptions {
  readonly target: CliTarget;
  readonly tenant: string;
  readonly wslDistro?: string;
  readonly timeoutMs: number;
}

interface LoginCliOptions extends CommonCliOptions {
  readonly account?: string;
  readonly promptTimeoutMs: number;
}

interface BrowserLoginCliOptions {
  readonly tenant: string;
  readonly wslDistro: string;
  readonly account: string;
  readonly timeoutMs: number;
  readonly urlTimeoutMs: number;
}

function parsePositiveInteger(value: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new InvalidArgumentError("Expected a positive integer.");
  }
  return parsed;
}

function parseTarget(value: string): CliTarget {
  try {
    return validateTarget(value);
  } catch (error) {
    throw new InvalidArgumentError(
      error instanceof Error ? error.message : String(error),
    );
  }
}

function addCommonOptions(command: Command): Command {
  return command
    .option(
      "--target <target>",
      "Azure CLI target: current; windows or wsl only from a Windows host",
      parseTarget,
      "current",
    )
    .option("--tenant <tenant>", "Azure tenant GUID or domain", DEFAULT_TENANT)
    .option(
      "--wsl-distro <name>",
      "Named WSL distribution used with --target wsl",
    )
    .option(
      "--timeout-ms <milliseconds>",
      "Overall command timeout",
      parsePositiveInteger,
      300_000,
    );
}

export function createProgram(
  service = new AuthAutomationService(createRuntimeDependencies()),
): Command {
  const program = new Command()
    .name("auth-automation")
    .description(
      "Safe Playwright automation for Azure CLI device-code and browser login.",
    )
    .showHelpAfterError();

  addCommonOptions(
    program
      .command("login")
      .description(
        "Start Azure CLI device-code login and automate only the safe browser steps.",
      ),
  )
    .option(
      "--account <tile>",
      "Accessible name or unique text of an existing account tile",
    )
    .option(
      "--prompt-timeout-ms <milliseconds>",
      "Time allowed for Azure CLI to emit the device-code prompt",
      parsePositiveInteger,
      60_000,
    )
    .action(async (options: LoginCliOptions) => {
      await service.login({
        target: options.target,
        tenant: options.tenant,
        wslDistro: options.wslDistro,
        accountHint: options.account,
        timeoutMs: options.timeoutMs,
        promptTimeoutMs: options.promptTimeoutMs,
      });
    });

  program
    .command("login-browser")
    .description(
      "Run policy-compliant Azure CLI browser login in a named WSL distribution from Windows.",
    )
    .requiredOption(
      "--wsl-distro <name>",
      "Named WSL distribution containing Azure CLI",
    )
    .requiredOption(
      "--account <tile>",
      "Accessible name or unique text of an existing account tile",
    )
    .option("--tenant <tenant>", "Azure tenant GUID or domain", DEFAULT_TENANT)
    .option(
      "--timeout-ms <milliseconds>",
      "Overall command timeout",
      parsePositiveInteger,
      300_000,
    )
    .option(
      "--url-timeout-ms <milliseconds>",
      "Time allowed for Azure CLI to produce its browser authorization request",
      parsePositiveInteger,
      60_000,
    )
    .action(async (options: BrowserLoginCliOptions) => {
      await service.loginBrowser({
        wslDistro: options.wslDistro,
        accountHint: options.account,
        tenant: options.tenant,
        timeoutMs: options.timeoutMs,
        urlTimeoutMs: options.urlTimeoutMs,
      });
    });

  addCommonOptions(
    program
      .command("status")
      .description(
        "Verify that the selected Azure CLI target is signed in to the requested tenant.",
      ),
  ).action(async (options: CommonCliOptions) => {
    await service.status({
      target: options.target,
      tenant: options.tenant,
      wslDistro: options.wslDistro,
      timeoutMs: options.timeoutMs,
    });
  });

  return program;
}

export async function main(argv = process.argv): Promise<void> {
  const dependencies = createRuntimeDependencies();
  const program = createProgram(new AuthAutomationService(dependencies));
  try {
    await program.parseAsync(argv);
  } catch (error) {
    dependencies.logger.error(
      error instanceof Error ? error.message : String(error),
    );
    process.exitCode = 1;
  }
}

const entryPath = process.argv[1];
if (
  entryPath &&
  path.resolve(entryPath) === path.resolve(fileURLToPath(import.meta.url))
) {
  await main();
}
