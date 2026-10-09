import path from "node:path";
import {
  buildAccountShowCommand,
  buildBrowserLoginCommand,
  buildLoginCommand,
  buildWslPathCommand,
  DEFAULT_TENANT,
  validateTenant,
  validateWslDistro,
} from "./azure-cli.js";
import {
  BROWSER_CAPTURE_HELPER,
  BROWSER_CAPTURE_HELPER_FILE,
  BROWSER_CAPTURE_URL_FILE,
  parseCapturedUrlFile,
} from "./browser-login.js";
import {
  decideBrowserAction,
  decideBrowserRedirectAction,
} from "./browser-policy.js";
import type {
  BrowserAdapter,
  BrowserSession,
  CommandSpec,
  FileSystemAdapter,
  HostEnvironment,
  PageSnapshot,
  ProcessAdapter,
  ProcessResult,
  RunningProcess,
  SafeLogger,
  TargetOptions,
  TimerAdapter,
} from "./contracts.js";
import {
  DeviceCodeParser,
  type DeviceCodeDetails,
} from "./device-code.js";
import {
  resolveBrowserProfile,
  resolveRuntimeRoot,
} from "./profile.js";
import {
  assertSafeBrowserAuthorizationUrl,
  assertSafeVerificationUrl,
  describeProcessFailure,
  isExpectedLocalhostRedirect,
  type BrowserAuthorizationRequest,
} from "./security.js";
import { verifyAzureAccount, type AzureAccount } from "./tenant.js";
import { withTimeout } from "./timing.js";

const DEFAULT_LOGIN_TIMEOUT_MS = 5 * 60_000;
const DEFAULT_PROMPT_TIMEOUT_MS = 60_000;
const DEFAULT_ACTION_TIMEOUT_MS = 10_000;
const STATUS_TIMEOUT_MS = 30_000;
const POLL_INTERVAL_MS = 250;
const MAX_SAFE_ACTIONS = 20;
const WSL_PATH_TIMEOUT_MS = 15_000;
const BROWSER_CLI_COMPLETION_GRACE_MS = 3_000;

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

export interface BrowserLoginOptions {
  readonly wslDistro: string;
  readonly tenant?: string;
  readonly accountHint: string;
  readonly timeoutMs?: number;
  readonly urlTimeoutMs?: number;
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

interface ActiveOperation {
  cancelled: boolean;
  failure?: unknown;
  running?: RunningProcess;
  session?: BrowserSession;
  readonly completion: Promise<void>;
  complete(): void;
}

interface CliCompletionState {
  result?: ProcessResult;
  error?: unknown;
}

type CliGraceOutcome =
  | { readonly kind: "success" }
  | { readonly kind: "failed" }
  | { readonly kind: "pending" };

export class AuthOperationCancelledError extends Error {
  constructor() {
    super("Authentication operation was cancelled.");
    this.name = "AuthOperationCancelledError";
  }
}

export function isAuthOperationCancelled(
  error: unknown,
): error is AuthOperationCancelledError {
  return error instanceof AuthOperationCancelledError;
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
  private activeOperation: ActiveOperation | undefined;

  constructor(private readonly dependencies: AuthAutomationDependencies) {}

  private beginOperation(): ActiveOperation {
    if (this.activeOperation) {
      throw new Error("Another authentication operation is already active.");
    }
    const completed = deferred<void>();
    const operation: ActiveOperation = {
      cancelled: false,
      completion: completed.promise,
      complete: () => completed.resolve(),
    };
    this.activeOperation = operation;
    return operation;
  }

  private finishOperation(operation: ActiveOperation): void {
    operation.complete();
    if (this.activeOperation === operation) {
      this.activeOperation = undefined;
    }
  }

  private throwIfCancelled(operation: ActiveOperation): void {
    if (operation.cancelled) {
      throw new AuthOperationCancelledError();
    }
  }

  async cancelActive(): Promise<void> {
    const operation = this.activeOperation;
    if (!operation) {
      return;
    }
    operation.cancelled = true;
    const cleanupResults = await Promise.allSettled([
      operation.running?.terminate(),
      operation.session?.close(),
    ]);
    await operation.completion;
    const rejected = cleanupResults.find(
      (result): result is PromiseRejectedResult =>
        result.status === "rejected",
    );
    if (rejected) {
      throw rejected.reason;
    }
    if (
      operation.failure &&
      !isAuthOperationCancelled(operation.failure)
    ) {
      throw operation.failure;
    }
  }

  private async runTrackedProcess(
    command: CommandSpec,
    timeoutMs: number,
    operation: ActiveOperation,
  ): Promise<ProcessResult> {
    this.throwIfCancelled(operation);
    const running = this.dependencies.processes.start(command);
    operation.running = running;
    try {
      const result = await withTimeout(
        running.completion,
        timeoutMs,
        this.dependencies.timer,
        `${command.label} timed out after ${timeoutMs}ms.`,
        () => running.terminate(),
      );
      this.throwIfCancelled(operation);
      return result;
    } catch (error) {
      if (operation.cancelled) {
        throw new AuthOperationCancelledError();
      }
      throw error;
    } finally {
      if (operation.running === running) {
        operation.running = undefined;
      }
    }
  }

  private async automateBrowser(
    session: BrowserSession,
    details: DeviceCodeDetails,
    accountHint: string | undefined,
    timeoutMs: number,
    operation: ActiveOperation,
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
    this.throwIfCancelled(operation);

    while (timer.now() < deadline) {
      this.throwIfCancelled(operation);
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
      this.throwIfCancelled(operation);
    }

    const diagnostic = lastSnapshot
      ? ` (${pageDiagnostic(lastSnapshot)})`
      : "";
    throw new Error(
      `Timed out waiting for the safe device-code browser flow${diagnostic}.`,
    );
  }

  private async convertWindowsPathToWsl(
    windowsPath: string,
    wslDistro: string,
    operation: ActiveOperation,
  ): Promise<string> {
    this.throwIfCancelled(operation);
    const { environment, processes } = this.dependencies;
    const result = await processes.run(
      buildWslPathCommand(windowsPath, wslDistro, environment),
      WSL_PATH_TIMEOUT_MS,
    );
    this.throwIfCancelled(operation);
    if (result.exitCode !== 0) {
      throw new Error(
        "Unable to convert the browser-login runtime path for WSL.",
      );
    }
    const converted = result.stdout.trim();
    if (
      !converted.startsWith("/") ||
      converted.includes("\r") ||
      converted.includes("\n") ||
      converted.includes("\0")
    ) {
      throw new Error("WSL returned an invalid browser-login runtime path.");
    }
    return converted;
  }

  private async waitForBrowserAuthorizationRequest(
    urlFilePath: string,
    tenant: string,
    completion: Promise<ProcessResult>,
    timeoutMs: number,
    operation: ActiveOperation,
  ): Promise<BrowserAuthorizationRequest> {
    const { fileSystem, timer } = this.dependencies;
    const deadline = timer.now() + timeoutMs;

    while (timer.now() < deadline) {
      this.throwIfCancelled(operation);
      const content = await fileSystem.readTextFile(urlFilePath);
      if (content !== undefined) {
        return assertSafeBrowserAuthorizationUrl(
          parseCapturedUrlFile(content),
          tenant,
        );
      }

      const remaining = Math.max(1, deadline - timer.now());
      const outcome = await Promise.race([
        completion.then((result) => ({
          kind: "completed" as const,
          result,
        })),
        timer
          .sleep(Math.min(POLL_INTERVAL_MS, remaining))
          .then(() => ({ kind: "poll" as const })),
      ]);
      if (outcome.kind === "completed") {
        this.throwIfCancelled(operation);
        const finalContent = await fileSystem.readTextFile(urlFilePath);
        if (finalContent !== undefined) {
          return assertSafeBrowserAuthorizationUrl(
            parseCapturedUrlFile(finalContent),
            tenant,
          );
        }
        const status =
          outcome.result.exitCode === null
            ? "terminated"
            : `exited with code ${outcome.result.exitCode}`;
        throw new Error(
          `Azure CLI browser login ${status} before producing an authorization request; process output was suppressed.`,
        );
      }
    }
    throw new Error(
      "Timed out waiting for Azure CLI to produce a browser authorization request.",
    );
  }

  private browserSnapshotReachedCallback(
    snapshot: PageSnapshot,
    request: BrowserAuthorizationRequest,
  ): boolean {
    return [snapshot.url, ...snapshot.navigationUrls].some((url) =>
      isExpectedLocalhostRedirect(
        url,
        request.redirectUri,
        request.state,
      ),
    );
  }

  private async waitForBrowserCliGrace(
    completion: Promise<ProcessResult>,
    completionState: CliCompletionState,
  ): Promise<CliGraceOutcome> {
    if (completionState.result) {
      return completionState.result.exitCode === 0
        ? { kind: "success" }
        : { kind: "failed" };
    }
    if (completionState.error) {
      return { kind: "failed" };
    }

    const { timer } = this.dependencies;
    return Promise.race([
      completion.then(
        (result): CliGraceOutcome =>
          result.exitCode === 0
            ? { kind: "success" }
            : { kind: "failed" },
        (): CliGraceOutcome => ({ kind: "failed" }),
      ),
      timer
        .sleep(BROWSER_CLI_COMPLETION_GRACE_MS)
        .then((): CliGraceOutcome => ({ kind: "pending" })),
    ]);
  }

  private browserCliSucceeded(state: CliCompletionState): boolean {
    return state.result?.exitCode === 0;
  }

  private browserCliFinishedWithFailure(
    state: CliCompletionState,
  ): boolean {
    return Boolean(
      state.error ||
        (state.result && state.result.exitCode !== 0),
    );
  }

  private async automateBrowserRedirect(
    session: BrowserSession,
    request: BrowserAuthorizationRequest,
    accountHint: string,
    timeoutMs: number,
    completion: Promise<ProcessResult>,
    completionState: CliCompletionState,
    operation: ActiveOperation,
  ): Promise<void> {
    const { logger, timer } = this.dependencies;
    const deadline = timer.now() + timeoutMs;
    let accountSelected = false;
    let safeActions = 0;

    try {
      await withTimeout(
        session.navigate(request.authorizationUrl),
        Math.min(DEFAULT_ACTION_TIMEOUT_MS, timeoutMs),
        timer,
        "Timed out opening the Microsoft browser authorization page.",
      );
      this.throwIfCancelled(operation);
    } catch {
      const snapshot = await session.observe().catch(() => undefined);
      if (snapshot && this.browserSnapshotReachedCallback(snapshot, request)) {
        const outcome = await this.waitForBrowserCliGrace(
          completion,
          completionState,
        );
        if (outcome.kind === "failed") {
          throw new Error(
            "Azure CLI browser login failed after the localhost callback; process output was suppressed.",
          );
        }
        return;
      }
      throw new Error(
        "Unable to open the Microsoft browser authorization page.",
      );
    }

    while (timer.now() < deadline) {
      this.throwIfCancelled(operation);
      const snapshot = await session.observe().catch(() => {
        throw new Error(
          "Unable to inspect the browser authorization page safely.",
        );
      });
      if (this.browserCliSucceeded(completionState)) {
        logger.info(
          "Azure CLI browser login completed before the browser page settled.",
        );
        return;
      }
      if (this.browserCliFinishedWithFailure(completionState)) {
        throw new Error(
          "Azure CLI browser login failed before the localhost callback; process output was suppressed.",
        );
      }
      if (this.browserSnapshotReachedCallback(snapshot, request)) {
        const outcome = await this.waitForBrowserCliGrace(
          completion,
          completionState,
        );
        if (outcome.kind === "failed") {
          throw new Error(
            "Azure CLI browser login failed after the localhost callback; process output was suppressed.",
          );
        }
        logger.info(
          "Browser authorization reached the expected localhost callback.",
        );
        return;
      }
      const decision = decideBrowserRedirectAction(snapshot, {
        accountHint,
        accountSelected,
        expectedRedirectUri: request.redirectUri,
        expectedState: request.state,
      });
      if (decision.kind === "complete") {
        logger.info(
          "Browser authorization reached the expected localhost callback.",
        );
        return;
      }
      if (decision.kind === "fail") {
        await Promise.resolve();
        if (this.browserCliSucceeded(completionState)) {
          logger.info(
            "Azure CLI browser login completed before the browser page settled.",
          );
          return;
        }
        if (
          decision.category === "unsafe-url" &&
          (accountSelected || safeActions > 0)
        ) {
          const outcome = await this.waitForBrowserCliGrace(
            completion,
            completionState,
          );
          if (outcome.kind === "success") {
            logger.info(
              "Azure CLI browser login completed during the post-account redirect grace period.",
            );
            return;
          }
          if (outcome.kind === "failed") {
            throw new Error(
              "Azure CLI browser login failed during the post-account redirect; process output was suppressed.",
            );
          }
        }
        throw new Error(decision.reason);
      }

      try {
        if (decision.kind === "click-account") {
          await session.clickAccount(decision.accountName);
          accountSelected = true;
          safeActions += 1;
          logger.info("Selected the requested existing account tile.");
        } else if (decision.kind === "click-control") {
          await session.clickControl(decision.control);
          safeActions += 1;
          logger.info(`Selected the safe ${decision.control} control.`);
        }
      } catch {
        const afterAction = await session.observe().catch(() => undefined);
        if (
          afterAction &&
          this.browserSnapshotReachedCallback(afterAction, request)
        ) {
          const outcome = await this.waitForBrowserCliGrace(
            completion,
            completionState,
          );
          if (outcome.kind === "failed") {
            throw new Error(
              "Azure CLI browser login failed after the localhost callback; process output was suppressed.",
            );
          }
          logger.info(
            "Browser authorization reached the expected localhost callback.",
          );
          return;
        }
        if (afterAction) {
          await Promise.resolve();
          if (this.browserCliSucceeded(completionState)) {
            logger.info(
              "Azure CLI browser login completed before the browser page settled.",
            );
            return;
          }
          const afterDecision = decideBrowserRedirectAction(afterAction, {
            accountHint,
            accountSelected,
            expectedRedirectUri: request.redirectUri,
            expectedState: request.state,
          });
          if (afterDecision.kind === "fail") {
            if (
              afterDecision.category === "unsafe-url" &&
              (accountSelected || safeActions > 0)
            ) {
              const outcome = await this.waitForBrowserCliGrace(
                completion,
                completionState,
              );
              if (outcome.kind === "success") {
                logger.info(
                  "Azure CLI browser login completed during the post-account redirect grace period.",
                );
                return;
              }
            }
            throw new Error(afterDecision.reason);
          }
        }
        throw new Error(
          "A safe browser authorization action failed before the localhost callback.",
        );
      }

      if (safeActions > MAX_SAFE_ACTIONS) {
        throw new Error(
          "The browser authorization page exceeded the safe automation action limit.",
        );
      }
      await timer.sleep(POLL_INTERVAL_MS);
      this.throwIfCancelled(operation);
    }
    throw new Error(
      "Timed out waiting for the browser authorization localhost callback.",
    );
  }

  async login(options: LoginOptions): Promise<AzureAccount> {
    const operation = this.beginOperation();
    try {
      return await this.loginInternal(options, operation);
    } catch (error) {
      operation.failure = error;
      throw error;
    } finally {
      this.finishOperation(operation);
    }
  }

  private async loginInternal(
    options: LoginOptions,
    operation: ActiveOperation,
  ): Promise<AzureAccount> {
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
    this.throwIfCancelled(operation);
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
    operation.running = running;
    let cliFinished = false;
    const completion = running.completion.then(
      (result) => {
        cliFinished = true;
        if (operation.running === running) {
          operation.running = undefined;
        }
        return result;
      },
      (error: unknown) => {
        cliFinished = true;
        if (operation.running === running) {
          operation.running = undefined;
        }
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
      this.throwIfCancelled(operation);
      logger.info(
        `Launching ${profile.browserKind === "edge" ? "Microsoft Edge" : "Playwright Chromium"} with the dedicated auth-automation profile.`,
      );
      session = await browser.launch({
        browserKind: profile.browserKind,
        profilePath: profile.profilePath,
        actionTimeoutMs: Math.min(DEFAULT_ACTION_TIMEOUT_MS, remaining()),
      });
      operation.session = session;
      try {
        this.throwIfCancelled(operation);
        await this.automateBrowser(
          session,
          { ...details, verificationUrl },
          options.accountHint?.trim() || undefined,
          remaining(),
          operation,
        );
      } finally {
        await session.close();
        if (operation.session === session) {
          operation.session = undefined;
        }
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

      const account = await this.statusInternal({
        target: options.target,
        wslDistro: options.wslDistro,
        tenant,
        timeoutMs: Math.min(STATUS_TIMEOUT_MS, remaining()),
      }, operation);
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
        if (operation.session === session) {
          operation.session = undefined;
        }
      }
      if (operation.cancelled) {
        throw new AuthOperationCancelledError();
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

  async loginBrowser(options: BrowserLoginOptions): Promise<AzureAccount> {
    const operation = this.beginOperation();
    try {
      return await this.loginBrowserInternal(options, operation);
    } catch (error) {
      operation.failure = error;
      throw error;
    } finally {
      this.finishOperation(operation);
    }
  }

  private async loginBrowserInternal(
    options: BrowserLoginOptions,
    operation: ActiveOperation,
  ): Promise<AzureAccount> {
    const {
      environment,
      processes,
      browser,
      fileSystem,
      timer,
      logger,
    } = this.dependencies;
    if (environment.platform !== "win32") {
      throw new Error(
        "login-browser must run on native Windows and target a named WSL distribution.",
      );
    }

    const tenant = validateTenant(options.tenant ?? DEFAULT_TENANT);
    const wslDistro = validateWslDistro(options.wslDistro);
    const accountHint = options.accountHint.trim();
    if (!accountHint) {
      throw new Error(
        "login-browser requires --account matching an existing account tile.",
      );
    }
    const timeoutMs = positiveTimeout(
      options.timeoutMs,
      DEFAULT_LOGIN_TIMEOUT_MS,
    );
    const urlTimeoutMs = positiveTimeout(
      options.urlTimeoutMs,
      DEFAULT_PROMPT_TIMEOUT_MS,
    );
    const deadline = timer.now() + timeoutMs;
    const remaining = (): number => Math.max(1, deadline - timer.now());
    const runtimeDirectory = await fileSystem.createUniqueDirectory(
      resolveRuntimeRoot(environment),
      "browser-login-",
    );
    const helperWindowsPath = path.win32.join(
      runtimeDirectory,
      BROWSER_CAPTURE_HELPER_FILE,
    );
    const urlFileWindowsPath = path.win32.join(
      runtimeDirectory,
      BROWSER_CAPTURE_URL_FILE,
    );
    let running: RunningProcess | undefined;
    let completion: Promise<ProcessResult> | undefined;
    let cliFinished = false;
    const cliCompletionState: CliCompletionState = {};
    let session: BrowserSession | undefined;
    let result: AzureAccount | undefined;
    let operationError: unknown;

    try {
      this.throwIfCancelled(operation);
      await fileSystem.writeTextFile(
        helperWindowsPath,
        BROWSER_CAPTURE_HELPER,
      );
      this.throwIfCancelled(operation);
      const helperWslPath = await this.convertWindowsPathToWsl(
        helperWindowsPath,
        wslDistro,
        operation,
      );
      const urlFileWslPath = await this.convertWindowsPathToWsl(
        urlFileWindowsPath,
        wslDistro,
        operation,
      );
      const command = buildBrowserLoginCommand(
        tenant,
        wslDistro,
        helperWslPath,
        urlFileWslPath,
        environment,
      );

      logger.info(
        `Starting Azure CLI browser login in WSL distribution ${wslDistro}.`,
      );
      running = processes.start(command);
      operation.running = running;
      completion = running.completion.then(
        (processResult) => {
          cliFinished = true;
          if (operation.running === running) {
            operation.running = undefined;
          }
          cliCompletionState.result = processResult;
          return processResult;
        },
        (error: unknown) => {
          cliFinished = true;
          if (operation.running === running) {
            operation.running = undefined;
          }
          cliCompletionState.error = error;
          throw error;
        },
      );

      const authorizationRequest =
        await this.waitForBrowserAuthorizationRequest(
          urlFileWindowsPath,
          tenant,
          completion,
          Math.min(urlTimeoutMs, remaining()),
          operation,
        );
      this.throwIfCancelled(operation);
      logger.addSecret(authorizationRequest.authorizationUrl);
      logger.addSecret(authorizationRequest.redirectUri);
      logger.addSecret(authorizationRequest.state);
      logger.info(
        "Captured and validated the Azure CLI browser authorization request.",
      );

      const profile = resolveBrowserProfile(environment);
      await fileSystem.ensureDirectory(profile.profilePath);
      this.throwIfCancelled(operation);
      session = await browser.launch({
        browserKind: "edge",
        profilePath: profile.profilePath,
        actionTimeoutMs: Math.min(DEFAULT_ACTION_TIMEOUT_MS, remaining()),
      });
      operation.session = session;
      let browserFlowError: unknown;
      try {
        this.throwIfCancelled(operation);
        await this.automateBrowserRedirect(
          session,
          authorizationRequest,
          accountHint,
          remaining(),
          completion,
          cliCompletionState,
          operation,
        );
      } catch (error) {
        browserFlowError = error;
      }
      try {
        await session.close();
      } catch {
        if (!browserFlowError) {
          browserFlowError = new Error(
            "Unable to close the browser-login context safely.",
          );
        }
      } finally {
        if (operation.session === session) {
          operation.session = undefined;
        }
        session = undefined;
      }
      if (browserFlowError) {
        throw browserFlowError;
      }

      const loginResult = await withTimeout(
        completion,
        remaining(),
        timer,
        "Azure CLI browser login did not finish before the timeout.",
        () => running?.terminate(),
      );
      if (loginResult.exitCode !== 0) {
        const status =
          loginResult.exitCode === null
            ? "was terminated"
            : `exited with code ${loginResult.exitCode}`;
        throw new Error(
          `Azure CLI browser login ${status}; process output was suppressed.`,
        );
      }

      result = await this.statusInternal({
        target: "wsl",
        wslDistro,
        tenant,
        timeoutMs: Math.min(STATUS_TIMEOUT_MS, remaining()),
      }, operation);
      logger.info(`Azure CLI tenant verified: ${result.tenantId}.`);
    } catch (error) {
      operationError = error;
      if (running && !cliFinished) {
        try {
          await running.terminate();
        } catch (terminationError) {
          operationError = new Error(
            "Browser login failed and launched-process exit could not be confirmed.",
            { cause: terminationError },
          );
        }
      }
      if (operation.cancelled) {
        operationError = new AuthOperationCancelledError();
      }
    } finally {
      if (session) {
        await session.close().catch(() => undefined);
        if (operation.session === session) {
          operation.session = undefined;
        }
      }
      try {
        await fileSystem.removeDirectory(runtimeDirectory);
      } catch (cleanupError) {
        operationError = new Error(
          operationError
            ? "Browser login failed and temporary authorization files could not be removed."
            : "Temporary browser-login authorization files could not be removed.",
          { cause: cleanupError },
        );
      }
    }

    if (operationError) {
      throw operationError;
    }
    if (!result) {
      throw new Error("Browser login did not produce a verified Azure account.");
    }
    return result;
  }

  async status(options: StatusOptions): Promise<AzureAccount> {
    const operation = this.beginOperation();
    try {
      return await this.statusInternal(options, operation);
    } catch (error) {
      operation.failure = error;
      throw error;
    } finally {
      this.finishOperation(operation);
    }
  }

  private async statusInternal(
    options: StatusOptions,
    operation: ActiveOperation,
  ): Promise<AzureAccount> {
    this.throwIfCancelled(operation);
    const { environment, logger } = this.dependencies;
    const tenant = validateTenant(options.tenant ?? DEFAULT_TENANT);
    const timeoutMs = positiveTimeout(options.timeoutMs, STATUS_TIMEOUT_MS);
    const command = buildAccountShowCommand(options, environment);
    const result = await this.runTrackedProcess(
      command,
      timeoutMs,
      operation,
    );
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
