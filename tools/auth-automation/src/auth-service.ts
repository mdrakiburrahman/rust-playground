import {
  buildAccountShowCommand,
  buildLoginCommand,
  DEFAULT_TENANT,
  validateTenant,
} from "./azure-cli.js";
import { decideBrowserAction } from "./browser-policy.js";
import type {
  BrowserAdapter,
  BrowserSession,
  FileSystemAdapter,
  HostEnvironment,
  ProcessAdapter,
  SafeLogger,
  TargetOptions,
  TimerAdapter,
} from "./contracts.js";
import {
  DeviceCodeParser,
  type DeviceCodeDetails,
} from "./device-code.js";
import { resolveBrowserProfile } from "./profile.js";
import {
  assertSafeVerificationUrl,
  describeProcessFailure,
} from "./security.js";
import { verifyAzureAccount, type AzureAccount } from "./tenant.js";
import { withTimeout } from "./timing.js";

const DEFAULT_LOGIN_TIMEOUT_MS = 5 * 60_000;
const DEFAULT_PROMPT_TIMEOUT_MS = 60_000;
const DEFAULT_ACTION_TIMEOUT_MS = 10_000;
const STATUS_TIMEOUT_MS = 30_000;
const POLL_INTERVAL_MS = 250;
const MAX_SAFE_ACTIONS = 20;

export interface LoginOptions extends TargetOptions {
  readonly tenant?: string;
  readonly accountHint?: string;
  readonly timeoutMs?: number;
  readonly promptTimeoutMs?: number;
}

export interface StatusOptions extends TargetOptions {
  readonly tenant?: string;
  readonly timeoutMs?: number;
}

export interface AuthAutomationDependencies {
  readonly environment: HostEnvironment;
  readonly processes: ProcessAdapter;
  readonly browser: BrowserAdapter;
  readonly fileSystem: FileSystemAdapter;
  readonly timer: TimerAdapter;
  readonly logger: SafeLogger;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
}

function deferred<T>(): Deferred<T> {
  let resolvePromise!: (value: T) => void;
  return {
    promise: new Promise<T>((resolve) => {
      resolvePromise = resolve;
    }),
    resolve: (value) => resolvePromise(value),
  };
}

function positiveTimeout(value: number | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error("Timeout values must be positive finite numbers.");
  }
  return value;
}

function pageDiagnostic(sessionSnapshot: {
  readonly url: string;
  readonly deviceCodeInputVisible: boolean;
  readonly accountSelectionRequired: boolean;
  readonly controls: readonly string[];
}): string {
  let host = "unknown";
  try {
    host = new URL(sessionSnapshot.url).hostname;
  } catch {
    // The diagnostic intentionally omits paths and query strings.
  }
  return `host=${host}, deviceCodeInput=${sessionSnapshot.deviceCodeInputVisible}, accountSelection=${sessionSnapshot.accountSelectionRequired}, safeControls=${sessionSnapshot.controls.join(",") || "none"}`;
}

export class AuthAutomationService {
  constructor(private readonly dependencies: AuthAutomationDependencies) {}

  private async automateBrowser(
    session: BrowserSession,
    details: DeviceCodeDetails,
    accountHint: string | undefined,
    timeoutMs: number,
  ): Promise<void> {
    const { logger, timer } = this.dependencies;
    const deadline = timer.now() + timeoutMs;
    let codeEntered = false;
    let codeSubmitted = false;
    let accountSelected = false;
    let safeActions = 0;
    let lastSnapshot:
      | Awaited<ReturnType<BrowserSession["observe"]>>
      | undefined;

    await withTimeout(
      session.navigate(details.verificationUrl),
      Math.min(DEFAULT_ACTION_TIMEOUT_MS, timeoutMs),
      timer,
      "Timed out opening the Microsoft device-login page.",
    );

    while (timer.now() < deadline) {
      lastSnapshot = await session.observe();
      const decision = decideBrowserAction(lastSnapshot, {
        codeEntered,
        codeSubmitted,
        accountHint,
        accountSelected,
      });

      if (decision.kind === "fail") {
        throw new Error(decision.reason);
      }
      if (decision.kind === "complete") {
        logger.info("Microsoft device-code page reported successful sign-in.");
        return;
      }
      if (decision.kind === "fill-device-code") {
        await session.fillDeviceCode(details.userCode);
        codeEntered = true;
        safeActions += 1;
        logger.info("Entered the short-lived device code.");
      } else if (decision.kind === "click-control") {
        await session.clickControl(decision.control);
        if (codeEntered && lastSnapshot.deviceCodeInputVisible) {
          codeSubmitted = true;
        }
        safeActions += 1;
        logger.info(`Selected the safe ${decision.control} control.`);
      } else if (decision.kind === "click-account") {
        await session.clickAccount(decision.accountName);
        accountSelected = true;
        safeActions += 1;
        logger.info("Selected the requested existing account tile.");
      }

      if (safeActions > MAX_SAFE_ACTIONS) {
        throw new Error(
          "The device-login page exceeded the safe automation action limit.",
        );
      }
      await timer.sleep(POLL_INTERVAL_MS);
    }

    const diagnostic = lastSnapshot
      ? ` (${pageDiagnostic(lastSnapshot)})`
      : "";
    throw new Error(
      `Timed out waiting for the safe device-code browser flow${diagnostic}.`,
    );
  }

  async login(options: LoginOptions): Promise<AzureAccount> {
    const {
      environment,
      processes,
      browser,
      fileSystem,
      timer,
      logger,
    } = this.dependencies;
    const tenant = validateTenant(options.tenant ?? DEFAULT_TENANT);
    const timeoutMs = positiveTimeout(
      options.timeoutMs,
      DEFAULT_LOGIN_TIMEOUT_MS,
    );
    const promptTimeoutMs = positiveTimeout(
      options.promptTimeoutMs,
      DEFAULT_PROMPT_TIMEOUT_MS,
    );
    const deadline = timer.now() + timeoutMs;
    const remaining = (): number => Math.max(1, deadline - timer.now());
    const command = buildLoginCommand(tenant, options, environment);
    const parser = new DeviceCodeParser();
    const deviceCode = deferred<DeviceCodeDetails>();
    let detailsFound = false;

    const onOutput = (chunk: string): void => {
      const parsed = parser.push(chunk);
      if (parsed && !detailsFound) {
        detailsFound = true;
        deviceCode.resolve(parsed);
      }
    };

    logger.info(
      `Starting ${command.label} device-code login for tenant ${tenant}.`,
    );
    const running = processes.start(command, {
      onStdout: onOutput,
      onStderr: onOutput,
    });
    let cliFinished = false;
    const completion = running.completion.then(
      (result) => {
        cliFinished = true;
        return result;
      },
      (error: unknown) => {
        cliFinished = true;
        throw error;
      },
    );
    let session: BrowserSession | undefined;

    try {
      const details = await withTimeout(
        Promise.race([
          deviceCode.promise,
          completion.then((result) => {
            throw new Error(
              `Azure CLI exited before emitting a device code. ${describeProcessFailure(command.label, result, logger)}`,
            );
          }),
        ]),
        Math.min(promptTimeoutMs, remaining()),
        timer,
        `Timed out waiting for ${command.label} to emit a device code.`,
        () => running.terminate(),
      );
      logger.addSecret(details.userCode);
      const verificationUrl = assertSafeVerificationUrl(
        details.verificationUrl,
      );
      logger.info(
        `Received a Microsoft device-code prompt from ${new URL(verificationUrl).hostname}.`,
      );

      const profile = resolveBrowserProfile(environment);
      await fileSystem.ensureDirectory(profile.profilePath);
      logger.info(
        `Launching ${profile.browserKind === "edge" ? "Microsoft Edge" : "Playwright Chromium"} with the dedicated auth-automation profile.`,
      );
      session = await browser.launch({
        browserKind: profile.browserKind,
        profilePath: profile.profilePath,
        actionTimeoutMs: Math.min(DEFAULT_ACTION_TIMEOUT_MS, remaining()),
      });
      try {
        await this.automateBrowser(
          session,
          { ...details, verificationUrl },
          options.accountHint?.trim() || undefined,
          remaining(),
        );
      } finally {
        await session.close();
        session = undefined;
      }

      const loginResult = await withTimeout(
        completion,
        remaining(),
        timer,
        `${command.label} did not finish before the login timeout.`,
        () => running.terminate(),
      );
      if (loginResult.exitCode !== 0) {
        throw new Error(
          describeProcessFailure(command.label, loginResult, logger),
        );
      }

      const account = await this.status({
        target: options.target,
        wslDistro: options.wslDistro,
        tenant,
        timeoutMs: Math.min(STATUS_TIMEOUT_MS, remaining()),
      });
      logger.info(`Azure CLI tenant verified: ${account.tenantId}.`);
      return account;
    } catch (error) {
      let terminationFailure: unknown;
      if (!cliFinished) {
        try {
          await running.terminate();
        } catch (terminationError) {
          terminationFailure = terminationError;
        }
      }
      if (session) {
        await session.close().catch(() => undefined);
      }
      if (terminationFailure) {
        const detail =
          terminationFailure instanceof Error
            ? terminationFailure.message
            : String(terminationFailure);
        throw new Error(
          `Authentication failed and launched-process exit could not be confirmed: ${detail}`,
          { cause: error },
        );
      }
      throw error;
    }
  }

  async status(options: StatusOptions): Promise<AzureAccount> {
    const { environment, processes, logger } = this.dependencies;
    const tenant = validateTenant(options.tenant ?? DEFAULT_TENANT);
    const timeoutMs = positiveTimeout(options.timeoutMs, STATUS_TIMEOUT_MS);
    const command = buildAccountShowCommand(options, environment);
    const result = await processes.run(command, timeoutMs);
    if (result.exitCode !== 0) {
      throw new Error(describeProcessFailure(command.label, result, logger));
    }
    const account = verifyAzureAccount(result.stdout, tenant);
    logger.info(
      `Azure CLI status: signed in to tenant ${account.tenantId}${account.subscriptionName ? ` (${account.subscriptionName})` : ""}.`,
    );
    return account;
  }
}
