import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";
import {
  buildAccountShowCommand,
  buildLoginCommand,
  DEFAULT_TENANT,
  WSL_PID_MARKER,
} from "../src/azure-cli.js";
import { AuthAutomationService } from "../src/auth-service.js";
import { decideBrowserAction } from "../src/browser-policy.js";
import type {
  BrowserAdapter,
  BrowserLaunchRequest,
  BrowserSession,
  CommandSpec,
  FileSystemAdapter,
  HostEnvironment,
  PageSnapshot,
  ProcessAdapter,
  ProcessOutputHandlers,
  ProcessResult,
  RunningProcess,
  SafeControl,
  TimerAdapter,
} from "../src/contracts.js";
import { DeviceCodeParser } from "../src/device-code.js";
import { NodeProcessAdapter } from "../src/process-adapter.js";
import {
  collectProcessTree,
  parseProcessTable,
} from "../src/process-tree.js";
import { resolveBrowserProfile } from "../src/profile.js";
import {
  assertSafeVerificationUrl,
  redactSensitiveText,
  SecretSafeLogger,
  type LogSink,
} from "../src/security.js";
import { verifyAzureAccount } from "../src/tenant.js";
import { SystemTimer, withTimeout } from "../src/timing.js";

function environment(
  platform: NodeJS.Platform,
  variables: Record<string, string | undefined> = {},
): HostEnvironment {
  return {
    platform,
    variables,
    homeDirectory: () =>
      platform === "win32" ? "C:\\Users\\tester" : "/home/tester",
  };
}

function snapshot(
  overrides: Partial<PageSnapshot> = {},
): PageSnapshot {
  return {
    url: "https://login.microsoftonline.com/common/oauth2/deviceauth",
    deviceCodeInputVisible: false,
    usernameInputVisible: false,
    passwordInputVisible: false,
    oneTimeCodeInputVisible: false,
    accountSelectionRequired: false,
    accountCandidates: [],
    controls: [],
    bodyText: "",
    ...overrides,
  };
}

class ManualTimer implements TimerAdapter {
  private currentTime = 0;
  private nextId = 1;
  private readonly callbacks = new Map<number, () => void>();

  now(): number {
    return this.currentTime;
  }

  setTimeout(callback: () => void, _delayMs: number): unknown {
    const id = this.nextId;
    this.nextId += 1;
    this.callbacks.set(id, callback);
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.callbacks.delete(handle as number);
  }

  async sleep(delayMs: number): Promise<void> {
    this.currentTime += delayMs;
  }

  fireAll(): void {
    const callbacks = [...this.callbacks.values()];
    this.callbacks.clear();
    for (const callback of callbacks) {
      callback();
    }
  }
}

class MemorySink implements LogSink {
  readonly messages: string[] = [];

  info(message: string): void {
    this.messages.push(message);
  }

  warn(message: string): void {
    this.messages.push(message);
  }

  error(message: string): void {
    this.messages.push(message);
  }
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
    resolve: resolvePromise,
  };
}

async function promiseWithin<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = globalThis.setTimeout(
          () => reject(new Error(message)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) {
      globalThis.clearTimeout(timeout);
    }
  }
}

function isProcessRunning(processId: number): boolean {
  try {
    process.kill(processId, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

function stopWindowsPids(processIds: readonly number[]): void {
  const runningIds = processIds.filter(isProcessRunning);
  if (runningIds.length === 0) {
    return;
  }
  const script = `Stop-Process -Id ${runningIds.join(",")} -Force -ErrorAction SilentlyContinue`;
  spawnSync(
    "powershell.exe",
    [
      "-NoLogo",
      "-NoProfile",
      "-NonInteractive",
      "-Command",
      script,
    ],
    { windowsHide: true },
  );
}

class FakeFileSystem implements FileSystemAdapter {
  readonly directories: string[] = [];

  async ensureDirectory(path: string): Promise<void> {
    this.directories.push(path);
  }
}

class ControlledProcessAdapter implements ProcessAdapter {
  readonly loginCompletion = deferred<ProcessResult>();
  readonly startedCommands: CommandSpec[] = [];
  readonly runCommands: CommandSpec[] = [];
  terminated = false;
  loginCompleted = false;
  readonly terminationStarted = deferred<void>();
  private completionResolved = false;

  constructor(
    private readonly prompt:
      | string
      | undefined =
      "To sign in, use a web browser to open https://microsoft.com/devicelogin and enter the code ABCD-EFGH to authenticate.",
    private readonly terminationGate?: Promise<void>,
  ) {}

  start(
    command: CommandSpec,
    handlers: ProcessOutputHandlers = {},
  ): RunningProcess {
    this.startedCommands.push(command);
    if (this.prompt) {
      handlers.onStdout?.(this.prompt);
    }
    return {
      pid: 4242,
      completion: this.loginCompletion.promise,
      terminate: async () => {
        this.terminated = true;
        this.terminationStarted.resolve();
        await this.terminationGate;
        if (!this.completionResolved) {
          this.completeLogin(null, "", "SIGTERM");
        }
      },
    };
  }

  async run(command: CommandSpec, _timeoutMs: number): Promise<ProcessResult> {
    this.runCommands.push(command);
    return {
      exitCode: 0,
      signal: null,
      stdout: JSON.stringify({
        tenantId: DEFAULT_TENANT,
        id: "subscription-id",
        name: "Test subscription",
        state: "Enabled",
      }),
      stderr: "",
    };
  }

  completeLogin(
    exitCode: number | null = 0,
    stderr = "",
    signal: NodeJS.Signals | null = null,
  ): void {
    if (this.completionResolved) {
      return;
    }
    this.completionResolved = true;
    this.loginCompleted = true;
    this.loginCompletion.resolve({
      exitCode,
      signal,
      stdout: "",
      stderr,
    });
  }
}

class SequenceBrowserSession implements BrowserSession {
  readonly closed = deferred<void>();
  readonly controlsClicked: SafeControl[] = [];
  readonly accountsClicked: string[] = [];
  navigatedUrl: string | undefined;
  filledCode: string | undefined;
  closeCount = 0;
  private index = 0;

  constructor(
    private readonly snapshots: readonly PageSnapshot[],
    private readonly process?: ControlledProcessAdapter,
  ) {}

  async navigate(url: string): Promise<void> {
    this.navigatedUrl = url;
  }

  async observe(): Promise<PageSnapshot> {
    const selected =
      this.snapshots[Math.min(this.index, this.snapshots.length - 1)];
    this.index += 1;
    return selected;
  }

  async fillDeviceCode(code: string): Promise<void> {
    assert.equal(
      this.process?.loginCompleted ?? false,
      false,
      "device code must be consumed before Azure CLI exits",
    );
    this.filledCode = code;
  }

  async clickControl(control: SafeControl): Promise<void> {
    this.controlsClicked.push(control);
  }

  async clickAccount(accountName: string): Promise<void> {
    this.accountsClicked.push(accountName);
  }

  async close(): Promise<void> {
    this.closeCount += 1;
    this.closed.resolve();
  }
}

class FakeBrowser implements BrowserAdapter {
  request: BrowserLaunchRequest | undefined;

  constructor(readonly session: BrowserSession) {}

  async launch(request: BrowserLaunchRequest): Promise<BrowserSession> {
    this.request = request;
    return this.session;
  }
}

test("builds current-host and Windows Azure CLI commands", () => {
  const linuxLogin = buildLoginCommand(
    DEFAULT_TENANT,
    { target: "current" },
    environment("linux"),
  );
  assert.equal(linuxLogin.command, "az");
  assert.deepEqual(linuxLogin.args, [
    "login",
    "--use-device-code",
    "--tenant",
    DEFAULT_TENANT,
    "--output",
    "none",
  ]);

  const windowsStatus = buildAccountShowCommand(
    { target: "windows" },
    environment("win32"),
  );
  assert.equal(windowsStatus.command, "cmd.exe");
  assert.deepEqual(windowsStatus.args.slice(0, 5), [
    "/d",
    "/s",
    "/c",
    "az",
    "account",
  ]);

  assert.throws(
    () =>
      buildAccountShowCommand(
        { target: "windows" },
        environment("linux", { WSL_INTEROP: "/run/WSL/1_interop" }),
      ),
    /native Windows.*--target current/i,
  );
});

test("rejects WSL-to-Windows targeting before spawning a process", async () => {
  const processes = new ControlledProcessAdapter();
  const service = new AuthAutomationService({
    environment: environment("linux", {
      WSL_DISTRO_NAME: "Ubuntu-24.04",
      WSL_INTEROP: "/run/WSL/1_interop",
    }),
    processes,
    browser: new FakeBrowser(
      new SequenceBrowserSession([snapshot({ bodyText: "unused" })]),
    ),
    fileSystem: new FakeFileSystem(),
    timer: new ManualTimer(),
    logger: new SecretSafeLogger(new MemorySink()),
  });

  await assert.rejects(
    service.status({ target: "windows" }),
    /native Windows.*--target current/i,
  );
  assert.deepEqual(processes.startedCommands, []);
  assert.deepEqual(processes.runCommands, []);
});

test("dispatches a named WSL target through wsl.exe", () => {
  const command = buildLoginCommand(
    DEFAULT_TENANT,
    { target: "wsl", wslDistro: "Ubuntu-24.04" },
    environment("win32"),
  );
  assert.equal(command.command, "wsl.exe");
  assert.deepEqual(command.args.slice(0, 5), [
    "--distribution",
    "Ubuntu-24.04",
    "--exec",
    "sh",
    "-c",
  ]);
  assert.match(command.args[5], /exec "\$@"/);
  assert.equal(command.args[6], "auth-automation");
  assert.equal(command.args[7], "az");
  assert.equal(command.args[8], "login");
  assert.deepEqual(command.termination, {
    kind: "wsl",
    distro: "Ubuntu-24.04",
    pidMarker: WSL_PID_MARKER,
  });
  assert.throws(
    () =>
      buildLoginCommand(
        DEFAULT_TENANT,
        { target: "wsl", wslDistro: "Ubuntu-24.04" },
        environment("linux", { WSL_DISTRO_NAME: "Ubuntu-24.04" }),
      ),
    /launched from Windows/,
  );
});

test("parses streaming Azure device-code prompt variants", () => {
  const splitParser = new DeviceCodeParser();
  assert.equal(
    splitParser.push(
      "To sign in, use a web browser to open https://microsoft.com/device",
    ),
    undefined,
  );
  assert.deepEqual(
    splitParser.push(
      "login and enter the code ABCD-EFGH to authenticate.",
    ),
    {
      verificationUrl: "https://microsoft.com/devicelogin",
      userCode: "ABCD-EFGH",
    },
  );

  const reorderedParser = new DeviceCodeParser();
  assert.deepEqual(
    reorderedParser.push(
      "Device code: ZXCV-1234\nAuthenticate at https://aka.ms/devicelogin",
    ),
    {
      verificationUrl: "https://aka.ms/devicelogin",
      userCode: "ZXCV-1234",
    },
  );
});

test("collects wrapper descendants leaf-first using specific PIDs", () => {
  const records = parseProcessTable(`
  100 1
  200 100
  250 100
  300 200
  900 1
`);
  assert.deepEqual(collectProcessTree(records, 100), [300, 250, 200, 100]);
});

test("allows only safe selector decisions", () => {
  assert.deepEqual(
    decideBrowserAction(snapshot({ deviceCodeInputVisible: true }), {
      codeEntered: false,
      codeSubmitted: false,
    }),
    { kind: "fill-device-code" },
  );
  assert.deepEqual(
    decideBrowserAction(snapshot({ controls: ["Next"] }), {
      codeEntered: true,
      codeSubmitted: false,
    }),
    { kind: "click-control", control: "Next" },
  );
  assert.equal(
    decideBrowserAction(snapshot({ controls: ["Next"] }), {
      codeEntered: false,
      codeSubmitted: false,
    }).kind,
    "wait",
  );
  assert.equal(
    decideBrowserAction(
      snapshot({ deviceCodeInputVisible: true, controls: ["Next"] }),
      {
        codeEntered: true,
        codeSubmitted: true,
      },
    ).kind,
    "wait",
  );
  assert.deepEqual(
    decideBrowserAction(
      snapshot({
        accountSelectionRequired: true,
        accountCandidates: ["user@example.com", "other@example.com"],
      }),
      {
        codeEntered: true,
        codeSubmitted: true,
        accountHint: "user@example.com",
      },
    ),
    { kind: "click-account", accountName: "user@example.com" },
  );
  assert.match(
    decideBrowserAction(
      snapshot({ passwordInputVisible: true }),
      { codeEntered: true, codeSubmitted: true },
    ).kind,
    /fail/,
  );
  assert.match(
    (
      decideBrowserAction(
        snapshot({ bodyText: "Approve the sign-in request in your app." }),
        { codeEntered: true, codeSubmitted: true },
      ) as { reason: string }
    ).reason,
    /MFA/,
  );
  assert.equal(
    decideBrowserAction(
      snapshot({ bodyText: "Use your security key", controls: ["Next"] }),
      { codeEntered: true, codeSubmitted: true },
    ).kind,
    "fail",
  );
  assert.match(
    (
      decideBrowserAction(
        snapshot({ usernameInputVisible: true }),
        { codeEntered: true, codeSubmitted: true },
      ) as { reason: string }
    ).reason,
    /username-entry/,
  );
  assert.equal(
    decideBrowserAction(
      snapshot({
        oneTimeCodeInputVisible: true,
        deviceCodeInputVisible: false,
      }),
      { codeEntered: true, codeSubmitted: true },
    ).kind,
    "fail",
  );
  assert.equal(
    decideBrowserAction(snapshot({ bodyText: "You have signed in." }), {
      codeEntered: true,
      codeSubmitted: true,
    }).kind,
    "complete",
  );
  assert.equal(
    decideBrowserAction(
      snapshot({ url: "https://example.com/phishing", controls: ["Next"] }),
      { codeEntered: true, codeSubmitted: true },
    ).kind,
    "fail",
  );
});

test("verifies the Azure account tenant", () => {
  const account = verifyAzureAccount(
    JSON.stringify({
      tenantId: DEFAULT_TENANT.toUpperCase(),
      id: "sub",
      name: "Subscription",
    }),
    DEFAULT_TENANT,
  );
  assert.equal(account.subscriptionId, "sub");
  assert.throws(
    () =>
      verifyAzureAccount(
        JSON.stringify({ tenantId: "11111111-1111-1111-1111-111111111111" }),
        DEFAULT_TENANT,
      ),
    /not the requested tenant/,
  );
  assert.throws(() => verifyAzureAccount("not-json", DEFAULT_TENANT), /JSON/);
});

test("uses dedicated OS cache profiles", () => {
  assert.equal(
    resolveBrowserProfile(
      environment("win32", { LOCALAPPDATA: "D:\\LocalAppData" }),
    ).profilePath,
    "D:\\LocalAppData\\rust-playground\\auth-automation\\msedge-profile",
  );
  assert.equal(
    resolveBrowserProfile(
      environment("linux", { XDG_CACHE_HOME: "/cache" }),
    ).profilePath,
    "/cache/rust-playground/auth-automation/chromium-profile",
  );
});

test("redacts device codes, tokens, passwords, and registered secrets", () => {
  const sink = new MemorySink();
  const logger = new SecretSafeLogger(sink);
  logger.addSecret("TOP-SECRET");
  logger.error(
    'enter the code ABCD-EFGH; access_token="token-value"; password=hunter2; Bearer abc.def; TOP-SECRET',
  );
  const output = sink.messages.join("\n");
  assert.doesNotMatch(output, /ABCD-EFGH|token-value|hunter2|abc\.def|TOP-SECRET/);
  assert.match(output, /\[REDACTED\]/);
  assert.doesNotMatch(
    redactSensitiveText("Use the device code ZXCV-1234 to authenticate."),
    /ZXCV-1234/,
  );
});

test("rejects unsafe verification URLs", () => {
  assert.equal(
    assertSafeVerificationUrl("https://microsoft.com/devicelogin"),
    "https://microsoft.com/devicelogin",
  );
  assert.throws(
    () => assertSafeVerificationUrl("https://example.com/devicelogin"),
    /Refusing/,
  );
  assert.throws(
    () => assertSafeVerificationUrl("http://microsoft.com/devicelogin"),
    /HTTPS/,
  );
});

test("withTimeout fails deterministically", async () => {
  const timer = new ManualTimer();
  let timedOut = false;
  const operation = withTimeout(
    new Promise<void>(() => undefined),
    100,
    timer,
    "expected timeout",
    () => {
      timedOut = true;
    },
  );
  timer.fireAll();
  await assert.rejects(operation, /expected timeout/);
  assert.equal(timedOut, true);
});

test("withTimeout waits for asynchronous process cleanup before rejecting", async () => {
  const timer = new ManualTimer();
  const cleanupGate = deferred<void>();
  let cleanupStarted = false;
  let rejected = false;
  const operation = withTimeout(
    new Promise<void>(() => undefined),
    100,
    timer,
    "expected timeout",
    async () => {
      cleanupStarted = true;
      await cleanupGate.promise;
    },
  );
  const observed = operation.catch((error: unknown) => {
    rejected = true;
    throw error;
  });

  timer.fireAll();
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(cleanupStarted, true);
  assert.equal(rejected, false);

  cleanupGate.resolve();
  await assert.rejects(observed, /expected timeout/);
  assert.equal(rejected, true);
});

test("login parses before process exit, uses Windows Edge for WSL, and verifies tenant", async () => {
  const processes = new ControlledProcessAdapter();
  const session = new SequenceBrowserSession(
    [
      snapshot({ deviceCodeInputVisible: true }),
      snapshot({
        deviceCodeInputVisible: true,
        controls: ["Next"],
      }),
      snapshot({ bodyText: "You have signed in." }),
    ],
    processes,
  );
  const browser = new FakeBrowser(session);
  const fileSystem = new FakeFileSystem();
  const timer = new ManualTimer();
  const service = new AuthAutomationService({
    environment: environment("win32", {
      LOCALAPPDATA: "C:\\Users\\tester\\AppData\\Local",
    }),
    processes,
    browser,
    fileSystem,
    timer,
    logger: new SecretSafeLogger(new MemorySink()),
  });

  const resultPromise = service.login({
    target: "wsl",
    wslDistro: "Ubuntu-24.04",
  });
  await session.closed.promise;
  assert.equal(session.filledCode, "ABCD-EFGH");
  assert.deepEqual(session.controlsClicked, ["Next"]);
  assert.equal(processes.loginCompleted, false);
  assert.equal(processes.startedCommands[0].command, "wsl.exe");
  assert.equal(browser.request?.browserKind, "edge");
  assert.match(
    browser.request?.profilePath ?? "",
    /auth-automation\\msedge-profile$/,
  );
  assert.deepEqual(fileSystem.directories, [browser.request?.profilePath]);

  processes.completeLogin();
  const account = await resultPromise;
  assert.equal(account.tenantId, DEFAULT_TENANT);
  assert.equal(processes.runCommands[0].command, "wsl.exe");
  assert.equal(session.closeCount, 1);
});

test("login timeout terminates the launched Azure CLI process tree", async () => {
  const processes = new ControlledProcessAdapter("");
  const timer = new ManualTimer();
  const browser = new FakeBrowser(
    new SequenceBrowserSession([snapshot({ bodyText: "unused" })]),
  );
  const service = new AuthAutomationService({
    environment: environment("linux"),
    processes,
    browser,
    fileSystem: new FakeFileSystem(),
    timer,
    logger: new SecretSafeLogger(new MemorySink()),
  });

  const login = service.login({
    target: "current",
    promptTimeoutMs: 100,
  });
  timer.fireAll();
  await assert.rejects(login, /Timed out waiting/);
  assert.equal(processes.terminated, true);
  assert.equal(browser.request, undefined);
});

test("login reports a secret-safe failure when Azure CLI exits before the prompt", async () => {
  const processes = new ControlledProcessAdapter("");
  processes.completeLogin(
    1,
    "Login failed after asking to enter the code LEAK-1234.",
  );
  const browser = new FakeBrowser(
    new SequenceBrowserSession([snapshot({ bodyText: "unused" })]),
  );
  const service = new AuthAutomationService({
    environment: environment("linux"),
    processes,
    browser,
    fileSystem: new FakeFileSystem(),
    timer: new ManualTimer(),
    logger: new SecretSafeLogger(new MemorySink()),
  });

  await assert.rejects(service.login({ target: "current" }), (error) => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /before emitting a device code/);
    assert.doesNotMatch(error.message, /LEAK-1234/);
    assert.match(error.message, /\[REDACTED\]/);
    return true;
  });
  assert.equal(browser.request, undefined);
});

test("password challenge closes the launched context and stops Azure CLI", async () => {
  const processes = new ControlledProcessAdapter();
  const session = new SequenceBrowserSession([
    snapshot({ passwordInputVisible: true }),
  ]);
  const browser = new FakeBrowser(session);
  const service = new AuthAutomationService({
    environment: environment("linux"),
    processes,
    browser,
    fileSystem: new FakeFileSystem(),
    timer: new ManualTimer(),
    logger: new SecretSafeLogger(new MemorySink()),
  });

  await assert.rejects(
    service.login({ target: "current" }),
    /password page/i,
  );
  assert.equal(processes.terminated, true);
  assert.equal(session.closeCount, 1);
});

test("safety failures wait for confirmed launched-process exit", async () => {
  const terminationGate = deferred<void>();
  const processes = new ControlledProcessAdapter(
    "To sign in, use a web browser to open https://microsoft.com/devicelogin and enter the code ABCD-EFGH to authenticate.",
    terminationGate.promise,
  );
  const session = new SequenceBrowserSession([
    snapshot({ passwordInputVisible: true }),
  ]);
  const service = new AuthAutomationService({
    environment: environment("linux"),
    processes,
    browser: new FakeBrowser(session),
    fileSystem: new FakeFileSystem(),
    timer: new ManualTimer(),
    logger: new SecretSafeLogger(new MemorySink()),
  });

  let rejected = false;
  const login = service.login({ target: "current" }).catch((error: unknown) => {
    rejected = true;
    throw error;
  });
  await processes.terminationStarted.promise;
  await Promise.resolve();
  assert.equal(rejected, false);

  terminationGate.resolve();
  await assert.rejects(login, /password page/i);
  assert.equal(processes.loginCompleted, true);
  assert.equal(rejected, true);
});

test(
  "NodeProcessAdapter terminates a Windows cmd wrapper and all descendants",
  { skip: process.platform !== "win32", timeout: 20_000 },
  async (context) => {
    const childSource =
      'console.log("grandchild:" + process.pid); setInterval(() => {}, 1000);';
    const childBootstrap = `eval(Buffer.from('${Buffer.from(childSource).toString("base64")}','base64').toString('utf8'))`;
    const outerSource = [
      'const { spawn } = require("node:child_process");',
      `const child = spawn(process.execPath, ["-e", ${JSON.stringify(childBootstrap)}], { stdio: ["ignore", "pipe", "inherit"] });`,
      'console.log("child:" + process.pid);',
      "child.stdout.pipe(process.stdout);",
      "setInterval(() => {}, 1000);",
    ].join(" ");
    const outerBootstrap = `eval(Buffer.from('${Buffer.from(outerSource).toString("base64")}','base64').toString('utf8'))`;
    const adapter = new NodeProcessAdapter(new SystemTimer());
    const descendantIds = deferred<{
      readonly child: number;
      readonly grandchild: number;
    }>();
    let childProcessId: number | undefined;
    let grandchildProcessId: number | undefined;
    let output = "";
    const running = adapter.start(
      {
        command: "cmd.exe",
        args: ["/d", "/s", "/c", "node", "-e", outerBootstrap],
        label: "Windows process-tree regression fixture",
      },
      {
        onStdout: (chunk) => {
          output += chunk;
          for (const line of output.split(/\r?\n/)) {
            const childMatch = line.match(/^child:(\d+)$/);
            const grandchildMatch = line.match(/^grandchild:(\d+)$/);
            if (childMatch) {
              childProcessId = Number(childMatch[1]);
            }
            if (grandchildMatch) {
              grandchildProcessId = Number(grandchildMatch[1]);
            }
          }
          if (childProcessId && grandchildProcessId) {
            descendantIds.resolve({
              child: childProcessId,
              grandchild: grandchildProcessId,
            });
          }
        },
      },
    );
    context.after(async () => {
      if (running.pid && isProcessRunning(running.pid)) {
        await running.terminate().catch(() => undefined);
      }
      stopWindowsPids(
        [running.pid, childProcessId, grandchildProcessId].filter(
          (processId): processId is number => processId !== undefined,
        ),
      );
    });

    assert.ok(running.pid);
    const ids = await promiseWithin(
      descendantIds.promise,
      5_000,
      "Windows regression fixture did not report descendant PIDs.",
    );
    assert.equal(isProcessRunning(running.pid), true);
    assert.equal(isProcessRunning(ids.child), true);
    assert.equal(isProcessRunning(ids.grandchild), true);

    await running.terminate();
    await running.completion;

    assert.equal(isProcessRunning(running.pid), false);
    assert.equal(isProcessRunning(ids.child), false);
    assert.equal(isProcessRunning(ids.grandchild), false);
  },
);

test(
  "NodeProcessAdapter terminates the WSL Linux PID tree before its Windows wrapper",
  { skip: process.platform !== "win32", timeout: 30_000 },
  async (context) => {
    const distro = "Ubuntu-24.04";
    const availability = spawnSync(
      "wsl.exe",
      ["--distribution", distro, "--exec", "true"],
      { windowsHide: true },
    );
    if (availability.status !== 0) {
      context.skip(`${distro} is not available for the WSL regression test.`);
      return;
    }

    const fixtureScript =
      'printf "linux-root:%s\\n" "$$"; sleep 300 & child=$!; printf "linux-child:%s\\n" "$child"; wait "$child"';
    const wrapperScript =
      `printf '${WSL_PID_MARKER}%s\\n' "$$" >&2; exec "$@"`;
    const adapter = new NodeProcessAdapter(new SystemTimer());
    const linuxIds = deferred<{
      readonly root: number;
      readonly child: number;
    }>();
    let linuxRootPid: number | undefined;
    let linuxChildPid: number | undefined;
    let stdout = "";
    let stderr = "";
    const running = adapter.start(
      {
        command: "wsl.exe",
        args: [
          "--distribution",
          distro,
          "--exec",
          "sh",
          "-c",
          wrapperScript,
          "auth-automation",
          "sh",
          "-c",
          fixtureScript,
        ],
        label: "WSL process-tree regression fixture",
        termination: {
          kind: "wsl",
          distro,
          pidMarker: WSL_PID_MARKER,
        },
      },
      {
        onStdout: (chunk) => {
          stdout += chunk;
          for (const line of stdout.split(/\r?\n/)) {
            const rootMatch = line.match(/^linux-root:(\d+)$/);
            const childMatch = line.match(/^linux-child:(\d+)$/);
            if (rootMatch) {
              linuxRootPid = Number(rootMatch[1]);
            }
            if (childMatch) {
              linuxChildPid = Number(childMatch[1]);
            }
          }
          if (linuxRootPid && linuxChildPid) {
            linuxIds.resolve({
              root: linuxRootPid,
              child: linuxChildPid,
            });
          }
        },
        onStderr: (chunk) => {
          stderr += chunk;
        },
      },
    );
    context.after(async () => {
      if (linuxRootPid || linuxChildPid) {
        spawnSync(
          "wsl.exe",
          [
            "--distribution",
            distro,
            "--exec",
            "kill",
            "-KILL",
            ...[linuxChildPid, linuxRootPid]
              .filter(
                (processId): processId is number =>
                  processId !== undefined,
              )
              .map(String),
          ],
          { windowsHide: true },
        );
      }
      if (running.pid && isProcessRunning(running.pid)) {
        await running.terminate().catch(() => undefined);
      }
      if (running.pid) {
        stopWindowsPids([running.pid]);
      }
    });

    assert.ok(running.pid);
    const ids = await promiseWithin(
      linuxIds.promise,
      10_000,
      "WSL regression fixture did not report Linux PIDs.",
    );
    assert.doesNotMatch(stderr, new RegExp(WSL_PID_MARKER));

    await running.terminate();
    await running.completion;

    const remaining = spawnSync(
      "wsl.exe",
      [
        "--distribution",
        distro,
        "--exec",
        "ps",
        "-p",
        `${ids.root},${ids.child}`,
        "-o",
        "pid=",
      ],
      { encoding: "utf8", windowsHide: true },
    );
    assert.equal(remaining.stdout.trim(), "");
    assert.equal(isProcessRunning(running.pid), false);
  },
);
