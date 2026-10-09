export type CliTarget = "current" | "windows" | "wsl";

export interface TargetOptions {
  readonly target: CliTarget;
  readonly wslDistro?: string;
}

export interface HostEnvironment {
  readonly platform: NodeJS.Platform;
  readonly variables: Readonly<Record<string, string | undefined>>;
  homeDirectory(): string;
}

export interface CommandSpec {
  readonly command: string;
  readonly args: readonly string[];
  readonly label: string;
  readonly termination?: WslTerminationMetadata;
}

export interface WslTerminationMetadata {
  readonly kind: "wsl";
  readonly distro: string;
  readonly pidMarker: string;
}

export interface ProcessResult {
  readonly exitCode: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stdout: string;
  readonly stderr: string;
}

export interface ProcessOutputHandlers {
  readonly onStdout?: (chunk: string) => void;
  readonly onStderr?: (chunk: string) => void;
}

export interface RunningProcess {
  readonly pid: number | undefined;
  readonly completion: Promise<ProcessResult>;
  terminate(): Promise<void>;
}

export interface ProcessAdapter {
  start(
    command: CommandSpec,
    handlers?: ProcessOutputHandlers,
  ): RunningProcess;
  run(command: CommandSpec, timeoutMs: number): Promise<ProcessResult>;
}

export interface TimerAdapter {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
  sleep(delayMs: number): Promise<void>;
}

export interface FileSystemAdapter {
  ensureDirectory(path: string): Promise<void>;
  createUniqueDirectory(parentPath: string, prefix: string): Promise<string>;
  writeTextFile(path: string, content: string): Promise<void>;
  readTextFile(path: string): Promise<string | undefined>;
  removeDirectory(path: string): Promise<void>;
}

export interface SafeLogger {
  addSecret(secret: string): void;
  sanitize(message: string): string;
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export type BrowserKind = "edge" | "chromium";

export interface BrowserLaunchRequest {
  readonly browserKind: BrowserKind;
  readonly profilePath: string;
  readonly actionTimeoutMs: number;
}

export type SafeControl = "Next" | "Continue" | "Accept" | "Consent";

export interface PageSnapshot {
  readonly url: string;
  readonly navigationUrls: readonly string[];
  readonly deviceCodeInputVisible: boolean;
  readonly usernameInputVisible: boolean;
  readonly passwordInputVisible: boolean;
  readonly oneTimeCodeInputVisible: boolean;
  readonly accountSelectionRequired: boolean;
  readonly accountCandidates: readonly string[];
  readonly controls: readonly SafeControl[];
  readonly bodyText: string;
}

export interface BrowserSession {
  navigate(url: string): Promise<void>;
  observe(): Promise<PageSnapshot>;
  fillDeviceCode(code: string): Promise<void>;
  clickControl(control: SafeControl): Promise<void>;
  clickAccount(accountName: string): Promise<void>;
  close(): Promise<void>;
}

export interface BrowserAdapter {
  launch(request: BrowserLaunchRequest): Promise<BrowserSession>;
}
