import { createHash, randomUUID } from 'node:crypto';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { lstat, mkdir, open, readFile, realpath, readdir, rename, rm, stat, unlink, writeFile } from 'node:fs/promises';
import { createServer as createHttpServer } from 'node:http';
import { createServer as createTcpServer, isIP } from 'node:net';
import { dirname, extname, isAbsolute, relative, resolve, sep } from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { formatPreflightReport, loadAksEnvironment, runAksPreflight } from './aks-preflight.js';
import { run_tree } from '../../infra/azure/bootstrap.js';
import { collectDataFlow, parseDataFlowVerdictPolicy, record, reportDataFlowRun, type NetworkObservation } from './dataflow-verdict.js';

export interface OatRunStatus {
  id: string;
  config_sha256: string | null;
  state: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  progress: string;
  result_count: number;
  error: string | null;
  started_at_unix_ms?: number;
  finished_at_unix_ms?: number | null;
  aks_cleanup_confirmed?: true;
  local_teardown?: {
    cell_id: string;
    schedule_unix_ms: number;
    measurement_end_unix_ms: number;
    started_at_unix_ms: number;
  } | null;
}

type FetchLike = typeof fetch;
type ProgressHandler = (status: OatRunStatus) => void;

export interface OatOwnedRunIdentity {
  readonly invocationId: string;
  readonly runId: string;
  readonly serverUrl: string;
  readonly serverPid: number;
  readonly serverStartTicks: string;
  readonly serverExecutable: string;
}

export type OatOwnedRunObserver = (identity: Readonly<OatOwnedRunIdentity>, receiptPath: string) => void | Promise<void>;

export interface LocalViewer {
  url: string;
  close(): Promise<void>;
}

interface LocalViewerConfig {
  resultsPath: string;
  siteRoot: string;
}

const workspaceRoot = resolve(fileURLToPath(new URL('../..', import.meta.url)));
const launcherDeadlineReceiptRoot = resolve(workspaceRoot, 'out/arrowcol-oat/deadline-receipts');
const launcherDeadlineMaxSeconds = 600;
const localCancellationCleanupMs = 10 * 60 * 1000;
const cliValuedOptions = new Set(['--config', '--mode', '--server', '--poll-ms', '--cleanup-local', '--stamp-config', '--deadline-seconds']);

export class OatCliError extends Error {
  readonly exitCode = 2;
}

export function cliExitCode(error: unknown): number {
  return error instanceof OatCliError ? error.exitCode : 1;
}

function validateArguments(args: string[]): void {
  const flags = new Set(['--viewer-only', '--no-viewer', '--preflight-only']);
  let positionalConfig = false;
  for (let index = 0; index < args.length; index++) {
    const argument = args[index]!;
    if (argument === '-h' || argument === '--help') continue;
    if (cliValuedOptions.has(argument)) {
      const value = args[index + 1];
      if (!value || value.startsWith('-')) throw new OatCliError(`Expected a value after ${argument}`);
      index++;
      continue;
    }
    if (flags.has(argument)) continue;
    if (argument.startsWith('-')) throw new OatCliError(`Unknown option: ${argument}`);
    if (positionalConfig) throw new OatCliError(`Unexpected argument: ${argument}`);
    positionalConfig = true;
  }
}

const cliUsage = `Usage: arrowcol-oat [options] [config]

Options:
  --mode <local|aks>   Select the execution mode
  --config <path>      Use a configuration file
  --stamp-config <path> Use the private stamp YAML for AKS validation
  --server <URL>       Use an existing OAT API server
  --poll-ms <number>   Set the status polling interval
  --deadline-seconds <1..600> Fail closed after this launcher-start deadline (local only)
  --viewer-only        Serve existing local results without running a benchmark
  --no-viewer          Exit after a successful local run without serving results
  --preflight-only     Run the read-only AKS preflight
  --cleanup-local <id> Remove one abandoned local Compose cell by its run UUID
  -h, --help           Show this help`;

type DeadlineCreationState = 'not_started' | 'pending' | 'accepted' | 'rejected' | 'unknown';
type DeadlineCleanupState = 'not_started' | 'pending' | 'confirmed' | 'unsettled';

export interface SubmittedOatConfig {
  submitted_config_body: string;
  submitted_config_sha256: string;
  source_config_sha256: string;
}

interface LauncherDeadlineReceipt {
  version: 1;
  invocation_id: string;
  started_at_unix_ms: number;
  deadline_at_unix_ms: number | null;
  started_at_monotonic_ms: number;
  deadline_seconds: number | null;
  deadline_at_monotonic_ms: number | null;
  submitted_config_body: string | null;
  submitted_config_sha256: string | null;
  source_config_sha256: string | null;
  cleanup_deadline_at_unix_ms: number | null;
  config_binding_error: string | null;
  overrun: boolean;
  overrun_at_unix_ms: number | null;
  creation_state: DeadlineCreationState;
  server_url: string | null;
  server_pid?: number | null;
  server_start_ticks?: string | null;
  server_executable?: string | null;
  run_id: string | null;
  run_state: OatRunStatus['state'] | 'not_started' | 'unknown';
  terminal_observation?: LauncherTerminalObservation | null;
  cancellation_attempts: number;
  last_cancel_result: string | null;
  cleanup_state: DeadlineCleanupState;
  error: string | null;
}

interface LauncherTerminalObservation {
  observed_at_unix_ms: number;
  status: OatRunStatus;
}

async function writeAtomicJson(path: string, value: unknown): Promise<void> {
  const directory = dirname(path);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = `${path}.${randomUUID()}.tmp`;
  const file = await open(temporary, 'wx', 0o600);
  try {
    await file.writeFile(`${JSON.stringify(value, null, 2)}\n`);
    await file.sync();
  } finally {
    await file.close();
  }
  await rename(temporary, path);
  const directoryHandle = await open(directory, 'r');
  try {
    await directoryHandle.sync();
  } finally {
    await directoryHandle.close();
  }
}

export class OatLauncherDeadline {
  readonly signal: AbortSignal;
  readonly receiptPath: string;
  readonly deadlineAtMonotonicMs: number | null;
  private readonly controller = new AbortController();
  private readonly receipt: LauncherDeadlineReceipt;
  private timer: NodeJS.Timeout | undefined;
  private writeTail: Promise<void> = Promise.resolve();
  private backgroundWriteFailure: unknown;
  private readonly writeReceipt: typeof writeAtomicJson;

  private constructor(receiptPath: string, receipt: LauncherDeadlineReceipt, writeReceipt = writeAtomicJson) {
    this.writeReceipt = writeReceipt;
    this.receiptPath = receiptPath;
    this.receipt = receipt;
    this.deadlineAtMonotonicMs = receipt.deadline_at_monotonic_ms;
    this.signal = this.controller.signal;
  }

  static async create(seconds: number | undefined, startedAtMonotonicMs = performance.now(), receiptRoot = launcherDeadlineReceiptRoot, startedAtUnixMs = Date.now()): Promise<OatLauncherDeadline> {
    if (seconds !== undefined && (!Number.isSafeInteger(seconds) || seconds < 1 || seconds > launcherDeadlineMaxSeconds)) {
      throw new OatCliError(`--deadline-seconds must be an integer from 1 to ${launcherDeadlineMaxSeconds}`);
    }
    const invocationId = randomUUID();
    const receipt: LauncherDeadlineReceipt = {
      version: 1,
      invocation_id: invocationId,
      started_at_unix_ms: startedAtUnixMs,
      deadline_at_unix_ms: seconds === undefined ? null : startedAtUnixMs + seconds * 1000,
      started_at_monotonic_ms: startedAtMonotonicMs,
      deadline_seconds: seconds ?? null,
      deadline_at_monotonic_ms: seconds === undefined ? null : startedAtMonotonicMs + seconds * 1000,
      submitted_config_body: null,
      submitted_config_sha256: null,
      source_config_sha256: null,
      cleanup_deadline_at_unix_ms: null,
      config_binding_error: null,
      overrun: false,
      overrun_at_unix_ms: null,
      creation_state: 'not_started',
      server_url: null,
      server_pid: null,
      server_start_ticks: null,
      server_executable: null,
      run_id: null,
      run_state: 'not_started',
      terminal_observation: null,
      cancellation_attempts: 0,
      last_cancel_result: null,
      cleanup_state: 'not_started',
      error: null,
    };
    const deadline = new OatLauncherDeadline(resolve(receiptRoot, `${invocationId}.json`), receipt);
    await deadline.persist();
    if (seconds !== undefined) {
      deadline.timer = setTimeout(() => {
        void deadline.markOverrun().catch((error: unknown) => { deadline.backgroundWriteFailure = error; });
        deadline.controller.abort(new Error('launcher-start deadline expired'));
      }, Math.max(0, deadline.deadlineAtMonotonicMs! - performance.now()));
    }
    return deadline;
  }

  static async restore(path: string, writeReceipt = writeAtomicJson): Promise<OatLauncherDeadline> {
    const parsed: unknown = JSON.parse(await readFile(path, 'utf8'));
    if (!isLauncherDeadlineReceipt(parsed)) throw new Error(`Invalid launcher deadline receipt ${path}`);
    const restored: LauncherDeadlineReceipt = {
      ...parsed,
      server_pid: parsed.server_pid ?? null,
      server_start_ticks: parsed.server_start_ticks ?? null,
      server_executable: parsed.server_executable ?? null,
      terminal_observation: parsed.terminal_observation ?? null,
    };
    const deadline = new OatLauncherDeadline(path, restored, writeReceipt);
    if (!restored.overrun && restored.deadline_at_unix_ms !== null && Date.now() >= restored.deadline_at_unix_ms) {
      restored.overrun = true;
      restored.overrun_at_unix_ms = Date.now();
      await deadline.persist();
    }
    return deadline;
  }

  get creationState(): DeadlineCreationState { return this.receipt.creation_state; }
  get submittedPlatform(): 'local' | 'aks' | undefined { return submittedPlatform(this.receipt.submitted_config_body); }
  get invocationId(): string { return this.receipt.invocation_id; }
  get cleanupState(): DeadlineCleanupState { return this.receipt.cleanup_state; }
  get runId(): string | null { return this.receipt.run_id; }
  get serverUrl(): string | null { return this.receipt.server_url; }
  get cancellationAttempts(): number { return this.receipt.cancellation_attempts; }
  get submittedConfigSha256(): string | null { return this.receipt.submitted_config_sha256; }
  isExpired(): boolean { return this.deadlineAtMonotonicMs !== null && performance.now() >= this.deadlineAtMonotonicMs; }

  canObserveOwnedRun(status: OatRunStatus, serverUrl: string, submittedConfigSha256: string): boolean {
    const receipt = this.receipt;
    return receipt.creation_state === 'accepted' && receipt.run_id === status.id &&
      receipt.server_url === serverUrl && receipt.run_state === status.state &&
      (status.state === 'queued' || status.state === 'running') && status.error === null &&
      receipt.cleanup_state === 'pending' && receipt.error === null && receipt.config_binding_error === null &&
      receipt.submitted_config_sha256 === submittedConfigSha256 && status.config_sha256 === submittedConfigSha256 &&
      receipt.cancellation_attempts === 0 && receipt.last_cancel_result === null &&
      !receipt.overrun && !this.signal.aborted && !this.isExpired() && this.backgroundWriteFailure === undefined;
  }

  private async persist(): Promise<void> {
    const snapshot = structuredClone(this.receipt);
    const nextWrite = this.writeTail.catch(() => undefined).then(() => this.writeReceipt(this.receiptPath, snapshot));
    this.writeTail = nextWrite;
    try {
      await nextWrite;
      this.backgroundWriteFailure = undefined;
    } catch (error) {
      this.backgroundWriteFailure = error;
      throw error;
    }
  }

  async setSubmissionBinding(binding: SubmittedOatConfig): Promise<void> {
    if (!/^[0-9a-f]{64}$/.test(binding.submitted_config_sha256) ||
        !/^[0-9a-f]{64}$/.test(binding.source_config_sha256) ||
        createHash('sha256').update(binding.submitted_config_body, 'utf8').digest('hex') !== binding.submitted_config_sha256) {
      throw new Error('Submitted OAT config binding is invalid');
    }
    this.receipt.submitted_config_body = binding.submitted_config_body;
    this.receipt.submitted_config_sha256 = binding.submitted_config_sha256;
    this.receipt.source_config_sha256 = binding.source_config_sha256;
    await this.persist();
  }

  async ensureCleanupDeadline(now = Date.now()): Promise<number> {
    if (this.receipt.cleanup_deadline_at_unix_ms === null) {
      if (this.receipt.cancellation_attempts !== 0) {
        throw new Error('Existing OAT cleanup receipt has cancellation attempts but no persisted cleanup deadline');
      }
      this.receipt.cleanup_deadline_at_unix_ms = now + localCancellationCleanupMs;
      await this.persist();
    }
    return this.receipt.cleanup_deadline_at_unix_ms;
  }

  async markCreationPending(serverUrl: string): Promise<void> {
    this.receipt.server_url = serverUrl;
    this.receipt.creation_state = 'pending';
    this.receipt.run_state = 'unknown';
    this.receipt.cleanup_state = 'pending';
    await this.persist();
  }

  async recordRunStarted(status: OatRunStatus): Promise<void> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(status.id)) {
      throw new Error('OAT server returned a run ID that cannot be safely persisted');
    }
    this.receipt.creation_state = 'accepted';
    this.receipt.run_id = status.id;
    this.receipt.run_state = status.state;
    this.receipt.cleanup_state = cleanupStateFor(status);
    await this.persist();
  }

  async recordOwnedServerIdentity(identity: Readonly<OatOwnedRunIdentity>): Promise<void> {
    const receipt = this.receipt;
    if (identity.invocationId !== receipt.invocation_id || receipt.creation_state !== 'accepted' ||
        receipt.run_id !== identity.runId || receipt.server_url !== identity.serverUrl ||
        !Number.isSafeInteger(identity.serverPid) || identity.serverPid <= 0 ||
        !identity.serverStartTicks || !identity.serverExecutable) {
      throw new Error('Owned OAT server identity does not match the accepted invocation receipt');
    }
    const alreadyBound = receipt.server_pid != null || receipt.server_start_ticks != null || receipt.server_executable != null;
    if (alreadyBound && (receipt.server_pid !== identity.serverPid || receipt.server_start_ticks !== identity.serverStartTicks ||
        receipt.server_executable !== identity.serverExecutable)) {
      throw new Error('Owned OAT server identity changed for the accepted invocation receipt');
    }
    receipt.server_pid = identity.serverPid;
    receipt.server_start_ticks = identity.serverStartTicks;
    receipt.server_executable = identity.serverExecutable;
    await this.persist();
  }

  async markCreationRejected(error: unknown): Promise<void> {
    this.receipt.creation_state = 'rejected';
    this.receipt.run_state = 'not_started';
    this.receipt.cleanup_state = 'confirmed';
    this.receipt.error = error instanceof Error ? error.message : String(error);
    await this.persist();
  }

  async markCreationUnknown(error: unknown): Promise<void> {
    this.receipt.creation_state = 'unknown';
    this.receipt.run_state = 'unknown';
    this.receipt.cleanup_state = 'unsettled';
    this.receipt.error = error instanceof Error ? error.message : String(error);
    await this.persist();
  }

  async markCancelAttempt(): Promise<void> {
    if (this.receipt.cancellation_attempts !== 0) throw new Error('Cancellation intent was already recorded for this OAT run');
    this.receipt.cancellation_attempts++;
    this.receipt.last_cancel_result = 'pending';
    await this.persist();
  }

  async markCancelResult(result: string): Promise<void> {
    this.receipt.last_cancel_result = result;
    await this.persist();
  }

  async recordTerminal(status: OatRunStatus): Promise<void> {
    this.receipt.run_state = status.state;
    this.receipt.cleanup_state = this.receipt.config_binding_error ? 'unsettled' : cleanupStateFor(status);
    this.receipt.error = this.receipt.config_binding_error ?? status.error;
    const identityBound = Number.isSafeInteger(this.receipt.server_pid) && (this.receipt.server_pid ?? 0) > 0 &&
      typeof this.receipt.server_start_ticks === 'string' && this.receipt.server_start_ticks.length > 0 &&
      typeof this.receipt.server_executable === 'string' && this.receipt.server_executable.length > 0;
    const terminal = status.state === 'succeeded' || status.state === 'failed' || status.state === 'cancelled';
    const matchesOwnedRun = terminal && identityBound &&
        this.receipt.creation_state === 'accepted' && this.receipt.run_id === status.id &&
        this.receipt.server_url !== null && this.receipt.submitted_config_sha256 !== null &&
        this.receipt.config_binding_error === null && status.config_sha256 === this.receipt.submitted_config_sha256;
    if (matchesOwnedRun && this.receipt.terminal_observation == null) {
      this.receipt.terminal_observation = { observed_at_unix_ms: Date.now(), status: structuredClone(status) };
    }
    if (matchesOwnedRun && status.state === 'failed' && status.aks_cleanup_confirmed === true &&
        this.submittedPlatform === 'aks' && hasConfirmedAksFailure(this.receipt.terminal_observation?.status)) {
      this.receipt.cleanup_state = 'confirmed';
    }
    await this.persist();
  }

  async markBindingFailure(error: unknown): Promise<void> {
    this.receipt.config_binding_error = error instanceof Error ? error.message : String(error);
    this.receipt.cleanup_state = 'unsettled';
    this.receipt.error = this.receipt.config_binding_error;
    await this.persist();
  }

  async markUnsettled(error: unknown): Promise<void> {
    this.receipt.cleanup_state = 'unsettled';
    this.receipt.error = error instanceof Error ? error.message : String(error);
    await this.persist();
  }

  async markNoRun(error?: unknown): Promise<void> {
    if (this.receipt.creation_state !== 'not_started') return;
    this.receipt.run_state = 'not_started';
    this.receipt.cleanup_state = 'confirmed';
    if (error !== undefined) this.receipt.error = error instanceof Error ? error.message : String(error);
    await this.persist();
  }

  async markOverrun(): Promise<void> {
    if (!this.receipt.overrun) {
      this.receipt.overrun = true;
      this.receipt.overrun_at_unix_ms = Date.now();
    }
    await this.persist();
  }

  dispose(): void { if (this.timer) clearTimeout(this.timer); }
}

function cleanupStateFor(status: OatRunStatus): DeadlineCleanupState {
  if (status.state === 'succeeded' || status.state === 'cancelled') return 'confirmed';
  if (status.state === 'failed') return 'unsettled';
  return 'pending';
}

function submittedPlatform(body: unknown): 'local' | 'aks' | undefined {
  if (typeof body !== 'string') return undefined;
  let config: unknown;
  try {
    config = JSON.parse(body);
  } catch {
    return undefined;
  }
  if (!isRecord(config) || !isRecord(config.platform)) return undefined;
  const kind = config.platform.kind;
  return kind === 'local' || kind === 'aks' ? kind : undefined;
}

function hasConfirmedAksFailure(status: OatRunStatus | undefined): boolean {
  return status?.state === 'failed' && status.aks_cleanup_confirmed === true &&
    typeof status.error === 'string' && typeof status.finished_at_unix_ms === 'number' &&
    Number.isSafeInteger(status.finished_at_unix_ms) && status.finished_at_unix_ms > 0;
}

function isLauncherDeadlineReceipt(value: unknown): value is LauncherDeadlineReceipt {
  if (!isRecord(value)) return false;
  const noBinding = value.submitted_config_body === null && value.submitted_config_sha256 === null && value.source_config_sha256 === null;
  const bindingIsValid = typeof value.submitted_config_body === 'string' &&
    typeof value.submitted_config_sha256 === 'string' && /^[0-9a-f]{64}$/.test(value.submitted_config_sha256) &&
    typeof value.source_config_sha256 === 'string' && /^[0-9a-f]{64}$/.test(value.source_config_sha256) &&
    createHash('sha256').update(value.submitted_config_body, 'utf8').digest('hex') === value.submitted_config_sha256;
  const identityFieldsAbsent = !('server_pid' in value) && !('server_start_ticks' in value) && !('server_executable' in value);
  const identityIsNull = value.server_pid === null && value.server_start_ticks === null && value.server_executable === null;
  const identityIsValid = typeof value.server_pid === 'number' && Number.isSafeInteger(value.server_pid) && value.server_pid > 0 &&
    typeof value.server_start_ticks === 'string' && value.server_start_ticks.length > 0 &&
    typeof value.server_executable === 'string' && value.server_executable.length > 0;
  const observationAbsent = !('terminal_observation' in value);
  const observationIsValid = value.terminal_observation === null || isLauncherTerminalObservation(value.terminal_observation);
  const observationMatches = value.terminal_observation === undefined || value.terminal_observation === null ||
    (identityIsValid && isLauncherTerminalObservation(value.terminal_observation) &&
      value.creation_state === 'accepted' && value.run_id === value.terminal_observation.status.id &&
      value.server_url !== null && typeof value.submitted_config_sha256 === 'string' &&
      value.submitted_config_sha256 === value.terminal_observation.status.config_sha256 &&
      value.run_state === value.terminal_observation.status.state);
  return value.version === 1 && typeof value.invocation_id === 'string' &&
    typeof value.started_at_unix_ms === 'number' && (value.deadline_at_unix_ms === null || typeof value.deadline_at_unix_ms === 'number') &&
    typeof value.started_at_monotonic_ms === 'number' &&
    (value.deadline_seconds === null || typeof value.deadline_seconds === 'number') &&
    (value.deadline_at_monotonic_ms === null || typeof value.deadline_at_monotonic_ms === 'number') &&
    (noBinding || bindingIsValid) &&
    (value.cleanup_deadline_at_unix_ms === null || typeof value.cleanup_deadline_at_unix_ms === 'number') &&
    (value.config_binding_error === null || typeof value.config_binding_error === 'string') &&
    typeof value.overrun === 'boolean' && (value.overrun_at_unix_ms === null || typeof value.overrun_at_unix_ms === 'number') &&
    ['not_started', 'pending', 'accepted', 'rejected', 'unknown'].includes(String(value.creation_state)) &&
    (value.server_url === null || typeof value.server_url === 'string') &&
    (value.run_id === null || typeof value.run_id === 'string') &&
    ['queued', 'running', 'succeeded', 'failed', 'cancelled', 'not_started', 'unknown'].includes(String(value.run_state)) &&
    (value.cleanup_state === 'not_started' || value.cleanup_state === 'pending' ||
      value.cleanup_state === 'confirmed' || value.cleanup_state === 'unsettled') &&
    typeof value.cancellation_attempts === 'number' && Number.isSafeInteger(value.cancellation_attempts) &&
    (value.last_cancel_result === null || typeof value.last_cancel_result === 'string') &&
    (value.error === null || typeof value.error === 'string') &&
    (identityFieldsAbsent || identityIsNull || identityIsValid) &&
    (identityFieldsAbsent || identityIsNull || (value.creation_state === 'accepted' && value.run_id !== null && value.server_url !== null)) &&
    (observationAbsent || observationIsValid) && observationMatches &&
    (value.run_state !== 'failed' || value.cleanup_state !== 'confirmed' ||
      (bindingIsValid && submittedPlatform(value.submitted_config_body) === 'aks' && value.config_binding_error === null &&
        isLauncherTerminalObservation(value.terminal_observation) &&
        hasConfirmedAksFailure(value.terminal_observation.status)));
}

function isLauncherTerminalObservation(value: unknown): value is LauncherTerminalObservation {
  if (!isRecord(value) || typeof value.observed_at_unix_ms !== 'number' || !Number.isSafeInteger(value.observed_at_unix_ms) ||
      !isRecord(value.status)) return false;
  const status = value.status;
  return typeof status.id === 'string' && ['succeeded', 'failed', 'cancelled'].includes(String(status.state)) &&
    typeof status.progress === 'string' && typeof status.result_count === 'number' &&
    (status.error === null || typeof status.error === 'string') &&
    (status.config_sha256 === null || typeof status.config_sha256 === 'string');
}

function isLocalOatServerUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === 'http:' && !url.username && !url.password &&
      ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  } catch {
    return false;
  }
}

export async function assertNoUnsettledDeadlineReceipts(root = launcherDeadlineReceiptRoot, writeReceipt = writeAtomicJson): Promise<void> {
  const names = await readdir(root).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  });
  for (const name of names.filter((entry) => entry.endsWith('.json'))) {
    const path = resolve(root, name);
    const deadline = await OatLauncherDeadline.restore(path, writeReceipt);
    if (deadline.creationState !== 'pending' && deadline.creationState !== 'unknown' &&
        deadline.cleanupState !== 'pending' && deadline.cleanupState !== 'unsettled') continue;
    if (deadline.submittedPlatform === 'aks') {
      throw new Error(`Unsettled AKS launcher receipt ${path}; refusing another run until this exact receipt is reconciled by an operator`);
    }
    if (deadline.submittedPlatform !== 'local') {
      throw new Error(`Unsettled launcher deadline receipt ${path} (platform unverified); refusing another run until this exact receipt is reconciled by an operator`);
    }
    if (!deadline.runId || !deadline.serverUrl ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(deadline.runId) ||
        !isLocalOatServerUrl(deadline.serverUrl)) {
      const identity = deadline.runId ? `run ${deadline.runId}` : 'run identity UNKNOWN';
      throw new Error(`Unsettled launcher deadline receipt ${path} (${identity}); refusing another run until this exact receipt is reconciled`);
    }
    try {
      await cancelAndReconcileLocalRun(new URL(deadline.serverUrl), deadline.runId, fetch, 1000, deadline);
    } catch (error) {
      throw new Error(`Unsettled launcher deadline receipt ${path}; refusing another run`, { cause: error });
    }
    if (deadline.cleanupState !== 'confirmed') {
      throw new Error(`Launcher deadline receipt ${path} remains unsettled; refusing another run`);
    }
  }
}


function bindSubmittedConfig(config: Record<string, unknown>, sourceBytes: Buffer): SubmittedOatConfig {
  const submitted_config_body = JSON.stringify(config);
  return {
    submitted_config_body,
    submitted_config_sha256: createHash('sha256').update(submitted_config_body, 'utf8').digest('hex'),
    source_config_sha256: createHash('sha256').update(sourceBytes).digest('hex'),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function resolveOatConfigPath(mode: 'local' | 'aks', configPath?: string, root = workspaceRoot): string {
  if (configPath) return configPath;
  if (mode === 'aks') {
    const generated = resolve(root, 'out/arrowcol-oat/oat-config.aks.json');
    if (existsSync(generated)) return generated;
  }
  const name = mode === 'local' ? 'oat-config.quick.json' : 'oat-config.aks.quick.json';
  return resolve(root, 'docs/experiments/arrowcol-oat', name);
}

export function resolveCliConfigPath(args: string[], mode: 'local' | 'aks', root = workspaceRoot): string {
  const configIndex = args.indexOf('--config');
  const explicitConfig = configIndex < 0 ? undefined : args[configIndex + 1];
  const positionalConfig = args.find((value, index) =>
    !value.startsWith('--') && !cliValuedOptions.has(args[index - 1] ?? ''),
  );
  return resolveOatConfigPath(mode, explicitConfig ?? positionalConfig, root);
}

export type DockerCommand = (args: string[]) => string;

function runDocker(args: string[]): string {
  return execFileSync('docker', args, { encoding: 'utf8', timeout: 30_000 });
}

function localProjectPrefix(config: Record<string, unknown>, length: number): string {
  const platform = isRecord(config.platform) ? config.platform : {};
  const base = platform.compose_project;
  if (typeof base !== 'string' || !/^[a-z0-9][a-z0-9_-]{0,47}$/.test(base)) throw new Error('Local Compose project is invalid or missing from config');
  return `${base.slice(0, length).replace(/[-_]+$/g, '')}-`;
}

export async function cleanupLocalComposeProjects(
  config: Record<string, unknown>,
  runId: string,
  docker: DockerCommand = runDocker,
): Promise<string[]> {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(runId)) {
    throw new Error('--cleanup-local requires a canonical OAT cell run UUID');
  }
  const compactRunId = runId.replace(/-/g, '').toLowerCase();
  const targetProject = localProjectPrefix(config, 15) + compactRunId;
  const legacyProject = localProjectPrefix(config, 39) + compactRunId.slice(0, 8);
  const projects = new Set<string>();
  let legacyFound = false;
  const resources: Array<{ list: string[]; inspect: (id: string) => string[] }> = [
    {
      list: ['ps', '-aq', '--filter', 'label=com.docker.compose.project'],
      inspect: (id) => ['inspect', '--format', '{{ index .Config.Labels "com.docker.compose.project" }}', id],
    },
    {
      list: ['network', 'ls', '-q', '--filter', 'label=com.docker.compose.project'],
      inspect: (id) => ['network', 'inspect', '--format', '{{ index .Labels "com.docker.compose.project" }}', id],
    },
    {
      list: ['volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project'],
      inspect: (id) => ['volume', 'inspect', '--format', '{{ index .Labels "com.docker.compose.project" }}', id],
    },
  ];
  for (const resource of resources) {
    for (const id of docker(resource.list).split(/\r?\n/).filter(Boolean)) {
      const project = docker(resource.inspect(id)).trim();
      if (project === legacyProject) legacyFound = true;
      if (project === targetProject) projects.add(project);
    }
  }
  if (legacyFound) {
    throw new Error(`Legacy Compose project ${legacyProject} has only an eight-character run prefix; ` +
      'ownership is ambiguous. Inspect and clean it manually; no resources or state were removed.');
  }
  projects.add(targetProject);
  for (const project of projects) {
    const containers = docker(['ps', '-aq', '--filter', `label=com.docker.compose.project=${project}`]).split(/\r?\n/).filter(Boolean);
    if (containers.length) docker(['rm', '-f', ...containers]);
    const networks = docker(['network', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`]).split(/\r?\n/).filter(Boolean);
    if (networks.length) docker(['network', 'rm', ...networks]);
    const volumes = docker(['volume', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`]).split(/\r?\n/).filter(Boolean);
    if (volumes.length) docker(['volume', 'rm', ...volumes]);
  }
  const platform = isRecord(config.platform) ? config.platform : {};
  if (typeof platform.compose_state_root === 'string') {
    const stateRoot = localStateRoot(config);
    for (const project of projects) {
      const matches = (await readdir(stateRoot, { withFileTypes: true }).catch((error: unknown) => {
        if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
        throw error;
      })).filter((entry) => entry.isDirectory() && entry.name.toLowerCase() === runId.toLowerCase());
      if (matches.length > 1) throw new Error(`Multiple local state directories match Compose project ${project}`);
      if (matches.length === 1) {
        const stateDir = resolve(stateRoot, matches[0]!.name);
        if ((await lstat(stateDir)).isSymbolicLink()) throw new Error(`Refusing symlinked local OAT state directory ${stateDir}`);
        await rm(stateDir, { recursive: true });
      }
    }
  }
  return [...projects];
}
interface LocalRunMarker extends SubmittedOatConfig {
  version: 1;
  runId: string;
  ownerPid: number;
  ownerStartTicks: string;
  serverPid: number;
  serverStartTicks: string;
  serverUrl: string;
  serverExecutable: string;
}

interface LocalRunMarkerContext {
  path: string;
  marker: Omit<LocalRunMarker, 'runId' | keyof SubmittedOatConfig>;
}

function localStateRoot(config: Record<string, unknown>): string {
  const platform = isRecord(config.platform) ? config.platform : {};
  if (typeof platform.compose_state_root !== 'string' || !platform.compose_state_root) {
    throw new Error('Local Compose state root is missing from config');
  }
  return resolve(workspaceRoot, platform.compose_state_root);
}

export async function processStartTicks(
  pid: number,
  readProcStat: (path: string) => Promise<string> = (path) => readFile(path, 'utf8'),
): Promise<string | undefined> {
  if (process.platform !== 'linux') return undefined;
  let statText: string;
  try {
    statText = await readProcStat(`/proc/${pid}/stat`);
  } catch (error) {
    if (error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ESRCH')) return undefined;
    throw error;
  }
  const fields = statText.slice(statText.lastIndexOf(')') + 2).trim().split(/\s+/);
  if (fields[0] === 'Z' || fields.length <= 19) return undefined;
  return fields[19];
}

async function prepareLocalRunMarker(config: Record<string, unknown>, server: {
  child: ChildProcess;
  url: string;
  executable: string;
}): Promise<LocalRunMarkerContext> {
  const serverPid = server.child.pid;
  if (!serverPid) throw new Error('Could not identify the owned OAT server process');
  const ownerStartTicks = await processStartTicks(process.pid);
  const serverStartTicks = await processStartTicks(serverPid);
  if (!ownerStartTicks || !serverStartTicks) {
    throw new Error('Cannot safely record local OAT process ownership on this platform');
  }
  const root = localStateRoot(config);
  await mkdir(root, { recursive: true });
  const path = resolve(root, `.arrowcol-oat-owner-${serverPid}.json`);
  return {
    path,
    marker: {
      version: 1,
      ownerPid: process.pid,
      ownerStartTicks,
      serverPid,
      serverStartTicks,
      serverUrl: new URL(server.url).href,
      serverExecutable: await realpath(server.executable),
    },
  };
}

async function persistLocalRunMarker(context: LocalRunMarkerContext, status: OatRunStatus, binding: SubmittedOatConfig): Promise<void> {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(status.id)) {
    throw new Error('OAT server returned a noncanonical run ID; local recovery marker was not written');
  }
  const marker: LocalRunMarker = { ...context.marker, ...binding, runId: status.id };
  const file = await open(context.path, 'wx', 0o600);
  try {
    await file.writeFile(JSON.stringify(marker));
    await file.sync();
  } finally {
    await file.close();
  }
  const directory = await open(dirname(context.path), 'r');
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

function parseLocalRunMarker(text: string, markerPath: string): LocalRunMarker {
  const value: unknown = JSON.parse(text);
  if (!isRecord(value) || value.version !== 1 ||
      typeof value.runId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.runId) ||
      typeof value.ownerPid !== 'number' || !Number.isSafeInteger(value.ownerPid) ||
      typeof value.ownerStartTicks !== 'string' ||
      typeof value.serverPid !== 'number' || !Number.isSafeInteger(value.serverPid) ||
      typeof value.serverStartTicks !== 'string' ||
      typeof value.serverUrl !== 'string' || typeof value.serverExecutable !== 'string' ||
      typeof value.submitted_config_body !== 'string' ||
      typeof value.submitted_config_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(value.submitted_config_sha256) ||
      typeof value.source_config_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(value.source_config_sha256)) {
    throw new Error(`Invalid local OAT ownership marker: ${markerPath}`);
  }
  if (createHash('sha256').update(value.submitted_config_body, 'utf8').digest('hex') !== value.submitted_config_sha256) {
    throw new Error(`Local OAT ownership marker config hash is invalid: ${markerPath}`);
  }
  const server = new URL(value.serverUrl);
  if (server.protocol !== 'http:' || server.hostname !== '127.0.0.1' || !server.port) {
    throw new Error(`Unsafe local OAT ownership marker URL: ${markerPath}`);
  }
  return {
    version: 1,
    runId: value.runId,
    ownerPid: value.ownerPid,
    ownerStartTicks: value.ownerStartTicks,
    serverPid: value.serverPid,
    serverStartTicks: value.serverStartTicks,
    serverUrl: value.serverUrl,
    serverExecutable: value.serverExecutable,
    submitted_config_body: value.submitted_config_body,
    submitted_config_sha256: value.submitted_config_sha256,
    source_config_sha256: value.source_config_sha256,
  };
}

async function cancelMarkedRun(marker: LocalRunMarker): Promise<OatRunStatus> {
  const statusUrl = new URL('/runs/' + encodeURIComponent(marker.runId), marker.serverUrl);
  let response = await fetch(statusUrl, { signal: AbortSignal.timeout(10_000) });
  let status = await readStatus(response, 'inspect interrupted local OAT run', marker.runId);
  let bindingMatches = status.config_sha256 === marker.submitted_config_sha256;
  if (status.state === 'queued' || status.state === 'running') {
    response = await fetch(new URL('/runs/' + encodeURIComponent(marker.runId) + '/cancel', marker.serverUrl), {
      method: 'POST',
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok && response.status !== 409) await readStatus(response, 'cancel interrupted local OAT run');
    const deadline = Date.now() + 10 * 60 * 1000;
    while (status.state === 'queued' || status.state === 'running') {
      if (Date.now() >= deadline) throw new Error('Timed out waiting for local OAT cleanup for run ' + marker.runId);
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
      response = await fetch(statusUrl, { signal: AbortSignal.timeout(10_000) });
      status = await readStatus(response, 'wait for interrupted local OAT cleanup', marker.runId);
      bindingMatches = bindingMatches && status.config_sha256 === marker.submitted_config_sha256;
    }
  }
  if (!bindingMatches) throw new Error(`Local OAT run ${marker.runId} config hash does not match its owner marker`);
  return status;
}
async function stopMarkedServer(marker: LocalRunMarker): Promise<void> {
  if (await processStartTicks(marker.serverPid) !== marker.serverStartTicks) return;
  const executable = await realpath(`/proc/${marker.serverPid}/exe`);
  if (executable !== marker.serverExecutable) throw new Error('Refusing to stop a process that does not match the recorded OAT server');
  process.kill(marker.serverPid, 'SIGTERM');
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline && await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 50));
  }
  if (await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
    process.kill(marker.serverPid, 'SIGKILL');
    const killDeadline = Date.now() + 1_000;
    while (Date.now() < killDeadline && await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 50));
    }
    if (await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      throw new Error(`Could not stop interrupted OAT server process ${marker.serverPid}`);
    }
  }
}

async function removeStaleLocalMarkers(config: Record<string, unknown>): Promise<void> {
  const root = localStateRoot(config);
  const files = await readdir(root).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  });
  const remainingCells = files.filter((entry) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(entry));
  if (remainingCells.length) return;
  for (const name of files.filter((entry) => entry.startsWith('.arrowcol-oat-owner-') && entry.endsWith('.json'))) {
    const markerPath = resolve(root, name);
    const marker = parseLocalRunMarker(await readFile(markerPath, 'utf8'), markerPath);
    if (await processStartTicks(marker.ownerPid) === marker.ownerStartTicks ||
        await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      throw new Error(`Refusing to remove ownership marker for active local run ${marker.runId}`);
    }
    await unlink(markerPath);
  }
}
async function recoverInterruptedLocalRuns(
  config: Record<string, unknown>,
  cleanupRunId?: string,
  cleanup: typeof cleanupLocalComposeProjects = cleanupLocalComposeProjects,
): Promise<string[] | undefined> {
  let explicitlyCleanedProjects: string[] | undefined;
  const root = localStateRoot(config);
  const files = await readdir(root).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  });
  for (const name of files.filter((entry) => entry.startsWith('.arrowcol-oat-owner-') && entry.endsWith('.json'))) {
    const markerPath = resolve(root, name);
    const marker = parseLocalRunMarker(await readFile(markerPath, 'utf8'), markerPath);
    if (await processStartTicks(marker.ownerPid) === marker.ownerStartTicks) {
      throw new Error('Local OAT run ' + marker.runId + ' is still owned by process ' + marker.ownerPid);
    }
    if (await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      const executable = await realpath('/proc/' + marker.serverPid + '/exe');
      if (executable !== marker.serverExecutable) throw new Error('OAT server ownership marker does not match the live process');
      const status = await cancelMarkedRun(marker);
      if (status.state === 'failed') {
        if (cleanupRunId === undefined) {
          throw new Error('Local OAT recovery did not confirm cleanup for run ' + marker.runId + ': ' + (status.error ?? status.state));
        }
        explicitlyCleanedProjects = await cleanup(config, cleanupRunId);
        const remainingFiles = await readdir(root).catch((error: unknown) => {
          if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
          throw error;
        });
        const remainingCells = remainingFiles.filter((entry) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(entry));
        if (remainingCells.length) {
          throw new Error('Local OAT cleanup is still incomplete. Clean each remaining cell with --cleanup-local <cell-run-uuid>: ' + remainingCells.join(', '));
        }
      } else if (status.state !== 'succeeded' && status.state !== 'cancelled') {
        throw new Error('Local OAT recovery did not confirm cleanup for run ' + marker.runId + ': ' + (status.error ?? status.state));
      }
      await stopMarkedServer(marker);
      await unlink(markerPath);
    } else if (cleanupRunId === undefined) {
      const cellRuns = files.filter((entry) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(entry));
      if (cellRuns.length) {
        throw new Error('Interrupted OAT server is gone; clean each local cell with --cleanup-local <cell-run-uuid>. Remaining cells: ' + cellRuns.join(', '));
      }
      await unlink(markerPath);
    }
  }
  return explicitlyCleanedProjects;
}
function getDockerBridgeGateway(): string {
  try {
    const gateway = execFileSync('docker', [
      'network', 'inspect', 'bridge', '--format', '{{(index .IPAM.Config 0).Gateway}}',
    ], { encoding: 'utf8', timeout: 10_000 }).trim();
    if (!gateway || isIP(gateway) === 0) throw new Error('Docker bridge returned no valid gateway address');
    return gateway;
  } catch (error) {
    throw new Error(`Could not detect Docker bridge gateway: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export async function readOatConfigForMode(
  configPath: string,
  mode: 'local' | 'aks',
  bridgeGateway: () => string = getDockerBridgeGateway,
  sourceBytes?: Buffer,
): Promise<Record<string, unknown>> {
  const parsed: unknown = JSON.parse((sourceBytes ?? await readFile(configPath)).toString('utf8'));
  if (!isRecord(parsed)) throw new Error('OAT config must be a JSON object');
  parseDataFlowVerdictPolicy(parsed.dataflow_verdict_policy);
  const platform: Record<string, unknown> = isRecord(parsed.platform) ? { ...parsed.platform, kind: mode } : { kind: mode };
  if (mode === 'local' && isRecord(platform.local)) {
    const local = { ...platform.local };
    if (local.bind_host == null || local.endpoint_host == null) {
      const gateway = bridgeGateway();
      if (local.bind_host == null) local.bind_host = gateway;
      if (local.endpoint_host == null) local.endpoint_host = gateway;
    }
    platform.local = local;
  }
  return { ...parsed, platform };
}

async function cancelAndReconcileLocalRun(
  server: URL,
  runId: string,
  fetcher: FetchLike,
  pollIntervalMs: number,
  deadline: OatLauncherDeadline,
  initialEvidenceError?: unknown,
): Promise<OatRunStatus> {
  if (deadline.runId !== runId) throw new Error('Cleanup run ID differs from the accepted OAT run');
  const cleanupDeadlineAtUnixMs = await deadline.ensureCleanupDeadline();
  const cleanupDeadlineAtMonotonicMs = performance.now() + Math.max(0, cleanupDeadlineAtUnixMs - Date.now());
  let cancellationAttempted = deadline.cancellationAttempts !== 0;
  let firstRead = true;
  let evidenceError = initialEvidenceError;
  let lastError: unknown = new Error('no cancellation response received');
  const note = async (operation: () => Promise<void>): Promise<void> => {
    try {
      await operation();
      evidenceError = undefined;
    } catch (error) {
      evidenceError = error;
    }
  };
  const remainingMs = (): number => Math.min(cleanupDeadlineAtUnixMs - Date.now(), cleanupDeadlineAtMonotonicMs - performance.now());
  const wait = async (): Promise<void> => {
    const remaining = remainingMs();
    if (remaining > 0) await new Promise<void>((resolveWait) => setTimeout(resolveWait, Math.min(pollIntervalMs, remaining)));
  };
  const cancelSignal = (): AbortSignal => {
    const remaining = remainingMs();
    if (remaining <= 0) throw new Error('Persisted OAT cleanup budget expired before cancellation dispatch');
    return AbortSignal.timeout(Math.max(1, Math.min(30_000, Math.floor(remaining))));
  };

  while (firstRead || remainingMs() > 0) {
    firstRead = false;
    let response: Response;
    try {
      response = await fetcher(new URL('/runs/' + encodeURIComponent(runId), server), { signal: AbortSignal.timeout(10_000) });
    } catch (error) {
      lastError = error;
      if (remainingMs() <= 0) break;
      await wait();
      continue;
    }
    if (!response.ok) {
      if (response.status >= 500 || response.status === 408 || response.status === 429) {
        lastError = new Error(`Run status read returned HTTP ${response.status}`);
        if (remainingMs() <= 0) break;
        await wait();
        continue;
      }
      const error = new Error(`Could not reconcile local OAT run ${runId}: HTTP ${response.status}`);
      await note(() => deadline.markUnsettled(error));
      throw error;
    }
    let status: OatRunStatus;
    try {
      status = await readStatus(response, 'reconcile local OAT run', runId);
    } catch (error) {
      await note(() => deadline.markUnsettled(error));
      throw error;
    }
    if (deadline.submittedConfigSha256 !== null && status.config_sha256 !== deadline.submittedConfigSha256) {
      await note(() => deadline.markBindingFailure(new Error(`OAT server config_sha256 mismatch for run ${runId}`)));
    }
    if (status.state !== 'queued' && status.state !== 'running') {
      await note(() => deadline.recordTerminal(status));
      if (evidenceError) {
        const persistError = evidenceError;
        const error = new Error(`OAT run ${runId} reached ${status.state}, but its terminal receipt could not be persisted`);
        await note(() => deadline.markUnsettled(error));
        throw new Error(error.message, { cause: persistError });
      }
      return status;
    }

    let attemptedCancellation = false;
    if (!cancellationAttempted) {
      cancellationAttempted = true;
      attemptedCancellation = true;
      await note(() => deadline.markCancelAttempt());
      if (evidenceError) {
        const persistError = evidenceError;
        const error = new Error('Could not persist cancellation intent before dispatch');
        await note(() => deadline.markUnsettled(error));
        throw new Error(error.message, { cause: persistError });
      }
      let signalForCancel: AbortSignal;
      try {
        signalForCancel = cancelSignal();
      } catch (error) {
        await note(() => deadline.markUnsettled(error));
        throw error;
      }
      let cancelResponse: Response;
      try {
        cancelResponse = await fetcher(new URL('/runs/' + encodeURIComponent(runId) + '/cancel', server), {
          method: 'POST',
          signal: signalForCancel,
        });
      } catch (error) {
        lastError = error;
        await note(() => deadline.markCancelResult('response_unknown'));
        continue;
      }
      if (cancelResponse.ok) {
        await note(() => deadline.markCancelResult('accepted'));
      } else if (cancelResponse.status === 409) {
        await note(() => deadline.markCancelResult('not_active'));
      } else if (cancelResponse.status >= 500 || cancelResponse.status === 408 || cancelResponse.status === 429) {
        lastError = new Error(`Cancel response was inconclusive (HTTP ${cancelResponse.status})`);
        await note(() => deadline.markCancelResult('response_unknown'));
      } else {
        const error = new Error(`Could not cancel local OAT run ${runId}: HTTP ${cancelResponse.status}`);
        await note(() => deadline.markCancelResult(`rejected_http_${cancelResponse.status}`));
        await note(() => deadline.markUnsettled(error));
        throw error;
      }
    }
    if (attemptedCancellation) continue;
    if (remainingMs() <= 0) break;
    await wait();
  }

  const detail = lastError instanceof Error ? lastError.message : String(lastError);
  const error = new Error(`Local OAT run ${runId} cleanup was not confirmed within the persisted ${localCancellationCleanupMs / 1000}s allowance: ${detail}`);
  await note(() => deadline.markUnsettled(error));
  if (evidenceError) throw new Error(`${error.message}; launcher receipt persistence also failed`, { cause: evidenceError });
  throw error;
}

export async function launchOat(
  configPath: string,
  serverUrl = 'http://127.0.0.1:8088',
  pollIntervalMs = 1000,
  fetcher: FetchLike = fetch,
  onProgress: ProgressHandler = (status) => console.log(`${status.state}: ${status.progress}`),
  mode?: 'local' | 'aks',
  bridgeGateway: () => string = getDockerBridgeGateway,
  signal?: AbortSignal,
  onRunStarted?: (status: OatRunStatus) => Promise<void>,
  onRunFinished?: (status: OatRunStatus) => void | Promise<void>,
  deadline?: OatLauncherDeadline,
  submission?: SubmittedOatConfig,
): Promise<OatRunStatus> {
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 1) {
    throw new Error('poll interval must be a positive integer');
  }
  const server = new URL(serverUrl);
  if (!['http:', 'https:'].includes(server.protocol)) {
    throw new Error('server URL must use HTTP or HTTPS');
  }
  const sourceBytes = submission ? undefined : await readFile(configPath);
  const config: unknown = submission
    ? JSON.parse(submission.submitted_config_body)
    : mode
      ? await readOatConfigForMode(configPath, mode, bridgeGateway, sourceBytes)
      : JSON.parse(sourceBytes!.toString('utf8'));
  if (!isRecord(config)) throw new Error('OAT config must be a JSON object');
  const binding = submission ?? bindSubmittedConfig(config, sourceBytes!);
  if (deadline) await deadline.setSubmissionBinding(binding);

  if (deadline) {
    if (deadline.isExpired()) {
      await deadline.markOverrun();
      await deadline.markNoRun(new Error('launcher-start deadline expired before run creation'));
      throw new Error(`Launcher-start deadline expired before OAT run creation; receipt: ${deadline.receiptPath}`);
    }
    await deadline.markCreationPending(server.href);
    if (deadline.isExpired()) {
      await deadline.markOverrun();
      const error = new Error('launcher-start deadline expired before POST /runs');
      await deadline.markCreationRejected(error);
      throw error;
    }
  }

  let startResponse: Response;
  try {
    startResponse = await fetcher(new URL('/runs', server), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: binding.submitted_config_body,
      signal: AbortSignal.timeout(30_000),
    });
  } catch (error) {
    if (deadline) await deadline.markCreationUnknown(error);
    throw error;
  }

  let status: OatRunStatus;
  try {
    status = await readStatus(startResponse, 'start OAT run');
  } catch (error) {
    if (deadline) {
      if (startResponse.status >= 400 && startResponse.status < 500) await deadline.markCreationRejected(error);
      else await deadline.markCreationUnknown(error);
    }
    throw error;
  }

  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(status.id)) {
    const error = new Error('OAT server returned a run ID that cannot be safely pinned');
    if (deadline) await deadline.markCreationUnknown(error);
    throw error;
  }
  const ownedRunId = status.id;
  let bindingFailure: Error | undefined = (deadline || submission) && status.config_sha256 !== binding.submitted_config_sha256
    ? new Error(`OAT server config_sha256 mismatch for run ${ownedRunId}`)
    : undefined;
  let startFailure: { error: unknown } | undefined;
  type CancellationOutcome = { ok: true; status?: OatRunStatus } | { ok: false; error: unknown };
  let cancellationRequest: Promise<CancellationOutcome> = Promise.resolve({ ok: true });
  let cancellationRequested = false;
  let cancellationObserved = false;
  let resolveCancellationStarted!: () => void;
  const cancellationStarted = new Promise<void>((resolveStarted) => { resolveCancellationStarted = resolveStarted; });
  const runSignal = deadline
    ? signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal
    : signal;
  const requestCancellation = (): void => {
    if (mode === 'aks' || submittedPlatform(binding.submitted_config_body) === 'aks' || cancellationRequested) return;
    cancellationRequested = true;
    resolveCancellationStarted();
    cancellationRequest = (async (): Promise<CancellationOutcome> => {
      if (deadline) {
        let evidenceError: unknown;
        if (deadline.isExpired()) {
          try { await deadline.markOverrun(); } catch (error) { evidenceError = error; }
        }
        const terminal = await cancelAndReconcileLocalRun(server, ownedRunId, fetcher, pollIntervalMs, deadline, evidenceError);
        return { ok: true, status: terminal };
      }
      const response = await fetcher(new URL('/runs/' + encodeURIComponent(ownedRunId) + '/cancel', server), {
        method: 'POST',
        signal: AbortSignal.timeout(30_000),
      });
      if (!response.ok && response.status !== 409) await readStatus(response, 'cancel local OAT run');
      return { ok: true };
    })().catch((error: unknown) => ({ ok: false, error }));
  };
  try {
    try {
      await deadline?.recordRunStarted(status);
      if (runSignal?.aborted) requestCancellation();
      else runSignal?.addEventListener('abort', requestCancellation, { once: true });
      await onRunStarted?.(status);
      if (bindingFailure) {
        await deadline?.markBindingFailure(bindingFailure);
        startFailure = { error: bindingFailure };
        if (status.state === 'queued' || status.state === 'running') requestCancellation();
      }
    } catch (error) {
      startFailure = { error };
      if (status.state === 'queued' || status.state === 'running') requestCancellation();
    }
    onProgress(status);
    while (status.state === 'queued' || status.state === 'running') {
      if (!cancellationRequested || cancellationObserved) {
        let pollTimer: NodeJS.Timeout;
        const pollDelay = new Promise<'poll'>((resolvePoll) => {
          pollTimer = setTimeout(() => resolvePoll('poll'), pollIntervalMs);
        });
        if (cancellationRequested) {
          await pollDelay;
        } else {
          const waitResult = await Promise.race([
            pollDelay,
            cancellationStarted.then(() => 'cancel' as const),
          ]);
          if (waitResult === 'cancel') clearTimeout(pollTimer!);
        }
      }
      if (cancellationRequested && !cancellationObserved) {
        const result = await cancellationRequest;
        cancellationObserved = true;
        if (!result.ok) throw result.error instanceof Error ? result.error : new Error(String(result.error));
        if (result.status) {
          const changed = result.status.state !== status.state || result.status.progress !== status.progress ||
            result.status.result_count !== status.result_count || result.status.error !== status.error;
          status = result.status;
          if (changed) onProgress(status);
          break;
        }
      }
      const response = await fetcher(new URL('/runs/' + encodeURIComponent(ownedRunId), server), {
        signal: AbortSignal.timeout(30_000),
      });
      const next = await readStatus(response, 'poll OAT run', ownedRunId);
      if ((deadline || submission) && next.config_sha256 !== binding.submitted_config_sha256) {
        bindingFailure = new Error(`OAT server config_sha256 mismatch for run ${ownedRunId}`);
        await deadline?.markBindingFailure(bindingFailure);
        if (next.state === 'queued' || next.state === 'running') requestCancellation();
      }
      const changed = next.state !== status.state || next.progress !== status.progress ||
        next.result_count !== status.result_count || next.error !== status.error;
      status = next;
      if (changed) onProgress(status);
    }
  } catch (error) {
    if (deadline && deadline.runId && deadline.cleanupState !== 'confirmed') {
      await deadline.markUnsettled(error);
    }
    if (startFailure) {
      const original = startFailure.error instanceof Error ? startFailure.error.message : String(startFailure.error);
      const cleanup = error instanceof Error ? error.message : String(error);
      throw new Error(`OAT run ownership could not be recorded: ${original}; cancellation cleanup also failed: ${cleanup}`, { cause: error });
    }
    throw error;
  } finally {
    runSignal?.removeEventListener('abort', requestCancellation);
  }

  await deadline?.recordTerminal(status);
  await onRunFinished?.(status);
  if (bindingFailure) {
    await deadline?.markBindingFailure(bindingFailure);
    throw bindingFailure;
  }
  if (deadline?.isExpired()) {
    await deadline.markOverrun();
    throw new Error(`Launcher-start deadline expired; run ${status.id} is ${status.state}; receipt: ${deadline.receiptPath}`);
  }
  if (startFailure) {
    if (status.state !== 'succeeded' && status.state !== 'cancelled') {
      const original = startFailure.error instanceof Error ? startFailure.error.message : String(startFailure.error);
      throw new Error(`OAT run ownership could not be recorded: ${original}; cancellation did not confirm cleanup: ${status.error ?? status.state}`);
    }
    throw startFailure.error instanceof Error ? startFailure.error : new Error(String(startFailure.error));
  }
  if (status.state === 'failed' || status.state === 'cancelled') {
    throw new Error(status.error ?? `OAT run ${status.id} ${status.state}`);
  }
  return status;
}

async function readStatus(response: Response, action: string, expectedId?: string): Promise<OatRunStatus> {
  const body: unknown = await response.json().catch(() => undefined);
  if (!response.ok) {
    const error = body !== null && typeof body === 'object' && 'error' in body && typeof body.error === 'string'
      ? body.error
      : `${response.status} ${response.statusText}`;
    throw new Error(`Could not ${action}: ${error}`);
  }
  if (
    body === null || typeof body !== 'object' ||
    !('id' in body) || typeof body.id !== 'string' ||
    !('state' in body) || !['queued', 'running', 'succeeded', 'failed', 'cancelled'].includes(String(body.state)) ||
    !('progress' in body) || typeof body.progress !== 'string' ||
    !('result_count' in body) || typeof body.result_count !== 'number'
  ) {
    throw new Error(`Server returned an invalid status while trying to ${action}`);
  }
  if (expectedId !== undefined && body.id !== expectedId) {
    throw new Error(`Server returned run ID ${body.id} while trying to ${action} for owned run ${expectedId}`);
  }
  return {
    ...(body as Omit<OatRunStatus, 'config_sha256'>),
    config_sha256: 'config_sha256' in body && typeof body.config_sha256 === 'string' ? body.config_sha256 : null,
  };
}

export async function main(
  args: string[],
  runner: typeof run_tree = run_tree,
  cleanupLocal = cleanupLocalComposeProjects,
  receiptRoot = launcherDeadlineReceiptRoot,
  collect: typeof collectDataFlow = collectDataFlow,
  onOwnedRun?: OatOwnedRunObserver,
): Promise<void> {
  const launcherStartedAtUnixMs = Date.now();
  const launcherStartedAtMonotonicMs = performance.now();
  if (args.includes('-h') || args.includes('--help')) {
    console.log(cliUsage);
    return;
  }
  validateArguments(args);
  const option = (name: string): string | undefined => {
    const index = args.indexOf(name);
    if (index < 0) return undefined;
    const value = args[index + 1];
    if (!value || value.startsWith('-')) throw new OatCliError(`Expected a value after ${name}`);
    return value;
  };
  const requestedMode = option('--mode');
  if (requestedMode !== undefined && requestedMode !== 'local' && requestedMode !== 'aks') {
    throw new OatCliError('--mode must be local or aks');
  }
  const requestedServerUrl = option('--server');
  const deadlineOptionCount = args.filter((argument) => argument === '--deadline-seconds').length;
  if (deadlineOptionCount > 1) throw new OatCliError('--deadline-seconds may be specified only once');
  const deadlineValue = option('--deadline-seconds');
  const deadlineSeconds = deadlineValue === undefined ? undefined : Number(deadlineValue);
  if (deadlineValue !== undefined && (!/^[1-9][0-9]{0,2}$/.test(deadlineValue) || !Number.isSafeInteger(deadlineSeconds) || deadlineSeconds! > launcherDeadlineMaxSeconds)) {
    throw new OatCliError(`--deadline-seconds must be an integer from 1 to ${launcherDeadlineMaxSeconds}`);
  }
  const requestedStampConfigPath = option('--stamp-config');
  if (requestedStampConfigPath !== undefined && requestedMode !== 'aks') {
    throw new OatCliError('--stamp-config requires --mode aks');
  }
  const cleanupRunId = option('--cleanup-local');
  const viewerOnly = args.includes('--viewer-only');
  const noViewer = args.includes('--no-viewer');
  const preflightOnly = args.includes('--preflight-only');
  if (deadlineSeconds !== undefined && (requestedMode === 'aks' || viewerOnly || !noViewer)) {
    throw new OatCliError('--deadline-seconds requires a local benchmark run with --no-viewer');
  }
  if (deadlineSeconds !== undefined && requestedServerUrl && !isLocalOatServerUrl(requestedServerUrl)) {
    throw new OatCliError('--deadline-seconds requires a loopback --server URL');
  }
  if (noViewer && (requestedMode === 'aks' || viewerOnly)) {
    throw new OatCliError('--no-viewer requires a local benchmark run and cannot be combined with --viewer-only');
  }
  if (cleanupRunId !== undefined && (requestedMode === 'aks' || requestedServerUrl || args.includes('--poll-ms') || args.includes('--viewer-only') || args.includes('--preflight-only') || deadlineSeconds !== undefined)) {
    throw new OatCliError('--cleanup-local requires local mode and cannot be combined with server, poll, viewer, or preflight options');
  }
  if (requestedMode === 'aks') Object.assign(process.env, await loadAksEnvironment(workspaceRoot));
  if (preflightOnly) {
    if (requestedMode !== 'aks' || viewerOnly || requestedServerUrl || args.includes('--poll-ms')) {
      throw new OatCliError('--preflight-only requires --mode aks and cannot be combined with viewer/server/poll options');
    }
    const configPath = resolveCliConfigPath(args, 'aks');
    const report = await runAksPreflight(configPath, { stampConfigPath: requestedStampConfigPath });
    console.log(formatPreflightReport(report));
    if (!report.ready) process.exitCode = 2;
    return;
  }
  const mode: 'local' | 'aks' = requestedMode === 'aks' ? 'aks' : 'local';
  const configPath = resolveCliConfigPath(args, mode);
  if (viewerOnly) {
    if (mode !== 'local' || requestedServerUrl || args.includes('--poll-ms')) {
      throw new OatCliError('--viewer-only is local-only and supports only --mode local and --config');
    }
    await keepLocalViewerOpen(await startLocalViewer(configPath));
    return;
  }
  const pollValue = Number(option('--poll-ms') ?? 1000);
  if (!Number.isSafeInteger(pollValue) || pollValue < 1) {
    throw new OatCliError('Expected values after --server and --poll-ms');
  }
  let network: NetworkObservation | undefined;
  let warnings: string[] = [];
  if (mode === 'aks') {
    const report = await runAksPreflight(configPath, { stampConfigPath: requestedStampConfigPath });
    console.log(formatPreflightReport(report));
    if (!report.ready) throw new Error('AKS preflight is NOT READY; refusing to start the benchmark.');
    if ('network' in report && record(report.network) &&
        typeof report.network.provider === 'string' && typeof report.network.enforcing === 'boolean') {
      network = { provider: report.network.provider, enforcing: report.network.enforcing };
    }
    if ('warnings' in report && Array.isArray(report.warnings)) {
      warnings = report.warnings.filter((warning): warning is string => typeof warning === 'string');
    }
  }
  const sourceBytes = await readFile(configPath);
  const config = await readOatConfigForMode(configPath, mode, getDockerBridgeGateway, sourceBytes);
  const submission = bindSubmittedConfig(config, sourceBytes);
  if (cleanupRunId !== undefined) {
    if (mode !== 'local') throw new OatCliError('--cleanup-local is local-only');
    const recoveredProjects = await recoverInterruptedLocalRuns(config, cleanupRunId, cleanupLocal);
    const projects = recoveredProjects ?? await cleanupLocal(config, cleanupRunId);
    await removeStaleLocalMarkers(config);
    console.log(projects.length ? `Cleaned local Compose project(s): ${projects.join(', ')}` : `No resources found for local run ${cleanupRunId}`);
    return;
  }
  if (mode === 'aks') {
    const subscriptionId = process.env.AZURE_SUBSCRIPTION_ID;
    if (!subscriptionId || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(subscriptionId)) {
      throw new Error('Validated selected Azure subscription is missing or invalid; rerun AKS setup.');
    }
    config.subscriptionId = subscriptionId;
  }
  await assertNoUnsettledDeadlineReceipts(receiptRoot);
  const deadline = await OatLauncherDeadline.create(deadlineSeconds, launcherStartedAtMonotonicMs, receiptRoot, launcherStartedAtUnixMs);
  if (deadlineSeconds !== undefined) {
    if (deadline.isExpired()) {
      await deadline.markOverrun();
      await deadline.markNoRun(new Error('launcher-start deadline expired during setup'));
      deadline.dispose();
      process.exitCode = 1;
      console.error(`[oat] Launcher deadline expired during setup; receipt: ${deadline.receiptPath}`);
      return;
    }
  }
  if (requestedServerUrl) {
    const interrupt = mode === 'local' ? registerLocalInterrupts() : undefined;
    let runPassed = false;
    try {
      try {
        const code = await reportDataFlowRun(
          (progress) => launchOat(configPath, requestedServerUrl, pollValue, fetch, progress, mode, getDockerBridgeGateway, interrupt?.controller.signal, undefined, undefined, deadline, submission),
          config, mode, undefined, runner, network, warnings, collect,
        );
        if (interrupt?.signal) {
          process.exitCode = interrupt.exitCode;
          return;
        }
        if (deadline.isExpired()) {
          await deadline.markOverrun();
          process.exitCode = 1;
          return;
        }
        runPassed = code === 0;
        if (code) process.exitCode = code;
      } catch (error) {
        if (!interrupt?.signal) throw error;
        process.exitCode = interrupt.exitCode;
        return;
      }
    } finally {
      interrupt?.dispose();
      if (deadline) {
        try {
          await deadline.markNoRun();
          if (deadline.isExpired()) {
            await deadline.markOverrun();
            process.exitCode = 1;
          }
        } finally {
          deadline.dispose();
        }
      }
    }
    if (runPassed && !noViewer) await maybeServeLocalViewer(configPath, mode);
    return;
  }

  if (mode === 'local') {
    try {
      await recoverInterruptedLocalRuns(config);
    } catch (error) {
      if (deadline) {
        if (deadline.isExpired()) await deadline.markOverrun();
        await deadline.markNoRun(error);
        deadline.dispose();
      }
      throw error;
    }
  }
  if (deadline?.isExpired()) {
    await deadline.markOverrun();
    await deadline.markNoRun(new Error('launcher-start deadline expired during local recovery'));
    deadline.dispose();
    process.exitCode = 1;
    console.error(`[oat] Launcher deadline expired during local recovery; receipt: ${deadline.receiptPath}`);
    return;
  }
  const serverEnvironment = mode === 'aks'
    ? { ARROWCOL_AKS_STAMP_CONFIG_FILE: requestedStampConfigPath ?? 'infra/azure/stamp.yaml' }
    : {};
  console.log('[oat] Building benchmark server...');
  let server: Awaited<ReturnType<typeof startLocalServer>>;
  try {
    server = await startLocalServer(runner, serverEnvironment);
  } catch (error) {
    if (deadline?.isExpired()) await deadline.markOverrun();
    await deadline?.markNoRun(error);
    deadline?.dispose();
    throw error;
  }
  if (deadline?.isExpired()) {
    await deadline.markOverrun();
    await deadline.markNoRun(new Error('launcher-start deadline expired before run creation'));
    await stopLocalServer(server.child);
    deadline.dispose();
    process.exitCode = 1;
    console.error(`[oat] Launcher deadline expired before run creation; receipt: ${deadline.receiptPath}`);
    return;
  }
  console.log(`[oat] Benchmark server: ${server.url} (PID ${server.child.pid}; logs in this terminal)`);
  let markerContext: LocalRunMarkerContext | undefined;
  try {
    if (mode === 'local') markerContext = await prepareLocalRunMarker(config, server);
  } catch (error) {
    await stopLocalServer(server.child);
    if (deadline?.isExpired()) await deadline.markOverrun();
    await deadline?.markNoRun(error);
    deadline?.dispose();
    throw error;
  }
  let markerWritten = false;
  let supplementaryEmitted = false;
  let runCleanupComplete = false;
  let runPassed = false;
  const interrupt = mode === 'local' ? registerLocalInterrupts() : undefined;
  try {
    try {
      const localMarker = markerContext;
      const bindAksServerIdentity = async (status: OatRunStatus): Promise<void> => {
        const serverPid = server.child.pid;
        if (!serverPid || server.child.exitCode !== null || server.child.signalCode !== null) return;
        const serverStartTicks = await processStartTicks(serverPid);
        if (!serverStartTicks || server.child.exitCode !== null || server.child.signalCode !== null) return;
        await deadline.recordOwnedServerIdentity({
          invocationId: deadline.invocationId,
          runId: status.id,
          serverUrl: new URL(server.url).href,
          serverPid,
          serverStartTicks,
          serverExecutable: server.executable,
        });
      };
      const onRunStarted = localMarker ? async (status: OatRunStatus): Promise<void> => {
        await persistLocalRunMarker(localMarker, status, submission);
        markerWritten = true;
        const identity: Readonly<OatOwnedRunIdentity> = Object.freeze({
          invocationId: deadline.invocationId,
          runId: status.id,
          serverUrl: localMarker.marker.serverUrl,
          serverPid: localMarker.marker.serverPid,
          serverStartTicks: localMarker.marker.serverStartTicks,
          serverExecutable: localMarker.marker.serverExecutable,
        });
        await deadline.recordOwnedServerIdentity(identity);
        if (onOwnedRun && !supplementaryEmitted) {
          if (interrupt?.controller.signal.aborted ||
              localMarker.marker.serverUrl !== new URL(server.url).href || localMarker.marker.serverPid !== server.child.pid ||
              !deadline.canObserveOwnedRun(status, localMarker.marker.serverUrl, submission.submitted_config_sha256)) {
            console.error('[oat] Supplementary owned-run identity skipped: accepted run is no longer eligible');
            return;
          }
          supplementaryEmitted = true;
          const diagnostic = (error: unknown): void => {
            console.error('[oat] Supplementary owned-run observer failed:', error);
          };
          try {
            void Promise.resolve(onOwnedRun(identity, deadline.receiptPath)).catch(diagnostic);
          } catch (error) {
            diagnostic(error);
          }
        }
      } : mode === 'aks' ? bindAksServerIdentity : undefined;
      const onRunFinished = (status: OatRunStatus): void => {
        runCleanupComplete = (status.state === 'succeeded' || status.state === 'cancelled') && deadline.cleanupState === 'confirmed';
      };
      const code = await reportDataFlowRun(
        (progress) => launchOat(configPath, server.url, pollValue, fetch, progress, mode, getDockerBridgeGateway, interrupt?.controller.signal, onRunStarted, onRunFinished, deadline, submission),
        config, mode, server.gitSha, runner, network, warnings, collect,
      );
      if (interrupt?.signal) {
        process.exitCode = interrupt.exitCode;
        return;
      }
      if (deadline.isExpired()) {
        await deadline.markOverrun();
        process.exitCode = 1;
        return;
      }
      runPassed = code === 0;
      if (code) process.exitCode = code;
    } catch (error) {
      if (!interrupt?.signal) throw error;
      process.exitCode = interrupt.exitCode;
      return;
    }
  } finally {
    try {
      await finishOwnedLocalRun(
        async () => {
          if (deadline && deadline.cleanupState !== 'confirmed') {
            console.error(`[oat] Preserving OAT server for unresolved run receipt ${deadline.receiptPath}`);
            return;
          }
          await stopLocalServer(server.child);
        },
        async () => {
          if (markerWritten && runCleanupComplete && markerContext) await unlink(markerContext.path);
        },
        interrupt,
        async () => { if (runPassed && !noViewer) await maybeServeLocalViewer(configPath, mode); },
      );
    } finally {
      if (deadline) {
        try {
          await deadline.markNoRun();
          if (deadline.isExpired()) {
            await deadline.markOverrun();
            process.exitCode = 1;
          }
        } finally {
          deadline.dispose();
        }
      }
    }
  }
}

export async function startLocalViewer(configPath: string): Promise<LocalViewer> {
  const config = await readLocalViewerConfig(configPath);
  if (!config) {
    throw new Error('--viewer-only requires platform.kind=local and website_url to be a file:// URL');
  }
  try {
    const resultFile = await stat(config.resultsPath);
    if (!resultFile.isFile()) throw new Error(`Local OAT results URL is not a file: ${config.resultsPath}`);
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      throw new Error(`Local OAT results file does not exist: ${config.resultsPath}`);
    }
    throw error;
  }
  return createLocalViewer(config);
}

async function maybeServeLocalViewer(configPath: string, mode?: 'local' | 'aks'): Promise<void> {
  const config = await readLocalViewerConfig(configPath, mode);
  if (!config) return;
  await keepLocalViewerOpen(await createLocalViewer(config));
}

async function readLocalViewerConfig(
  configPath: string,
  mode?: 'local' | 'aks',
): Promise<LocalViewerConfig | undefined> {
  const parsed: unknown = JSON.parse(await readFile(configPath, 'utf8'));
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return;
  const config = parsed as { platform?: { kind?: string }; website_url?: string };
  const platform = mode ?? config.platform?.kind;
  const websiteUrl = config.website_url;
  if (platform !== 'local' || !websiteUrl?.startsWith('file://')) return undefined;
  const resultsPath = fileURLToPath(new URL(websiteUrl));
  const workspace = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  const siteRoot = await realpath(resolve(workspace, 'docs/experiments/arrowcol-oat'));
  return { resultsPath, siteRoot };
}

async function createLocalViewer(config: LocalViewerConfig): Promise<LocalViewer> {
  const { resultsPath, siteRoot } = config;
  const server = createHttpServer(async (request, response) => {
    const port = request.socket.localPort;
    const hostHeaderCount = request.rawHeaders.filter((header, index) =>
      index % 2 === 0 && header.toLowerCase() === 'host').length;
    const host = request.headers.host?.toLowerCase();
    const allowedHosts = port === undefined ? [] : [`127.0.0.1:${port}`, `localhost:${port}`];
    if (hostHeaderCount !== 1 || !host || !allowedHosts.includes(host)) {
      response.writeHead(400, { 'content-type': 'text/plain; charset=utf-8' }).end('Invalid Host header');
      return;
    }
    const allowedOrigins = port === undefined ? [] : [
      `http://127.0.0.1:${port}`,
      `http://localhost:${port}`,
    ];
    const origin = request.headers.origin?.toLowerCase();
    if (origin !== undefined && !allowedOrigins.includes(origin)) {
      response.writeHead(403, { 'content-type': 'text/plain; charset=utf-8' }).end('Origin not allowed');
      return;
    }
    try {
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        response.writeHead(405, { allow: 'GET, HEAD' }).end();
        return;
      }
      const requestUrl = new URL(request.url ?? '/', 'http://127.0.0.1');
      if (requestUrl.pathname === '/results.parquet') {
        await sendViewerFile(request, response, await readFile(resultsPath), 'application/vnd.apache.parquet');
        return;
      }
      let pathname: string;
      try {
        pathname = decodeURIComponent(requestUrl.pathname === '/' ? '/index.html' : requestUrl.pathname);
      } catch {
        response.writeHead(400).end('Invalid path');
        return;
      }
      const file = resolve(siteRoot, `.${pathname}`);
      if (!isPathWithin(siteRoot, file)) {
        response.writeHead(400).end('Invalid path');
        return;
      }
      const actualFile = await realpath(file);
      if (!isPathWithin(siteRoot, actualFile)) {
        response.writeHead(403).end('Forbidden');
        return;
      }
      const contentTypes: Record<string, string> = {
        '.css': 'text/css; charset=utf-8',
        '.html': 'text/html; charset=utf-8',
        '.js': 'text/javascript; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
        '.png': 'image/png',
        '.svg': 'image/svg+xml',
      };
      await sendViewerFile(
        request,
        response,
        await readFile(actualFile),
        contentTypes[extname(actualFile).toLowerCase()] ?? 'application/octet-stream',
      );
    } catch (error) {
      const errorCode = error instanceof Error && 'code' in error ? error.code : undefined;
      const code = errorCode === 'ENOENT' || errorCode === 'ENOTDIR' ? 404
        : errorCode === 'ENAMETOOLONG' ? 414 : 500;
      response.writeHead(code).end(code === 404 ? 'Not found'
        : code === 414 ? 'Request-URI Too Long' : 'Could not serve local OAT viewer');
      if (code === 500) console.error(error);
    }
  });
  await new Promise<void>((resolvePromise, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolvePromise);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Could not allocate a local viewer port');
  return {
    url: `http://127.0.0.1:${address.port}/`,
    close: () => new Promise<void>((resolvePromise, reject) => {
      server.close((error) => error ? reject(error) : resolvePromise());
    }),
  };
}

function registerLocalInterrupts(): {
  controller: AbortController;
  readonly signal: NodeJS.Signals | undefined;
  readonly exitCode: number;
  dispose(): void;
} {
  const controller = new AbortController();
  let received: NodeJS.Signals | undefined;
  let count = 0;
  const exitCodes: Partial<Record<NodeJS.Signals, number>> = { SIGINT: 130, SIGTERM: 143, SIGHUP: 129 };
  const handlers = new Map<NodeJS.Signals, () => void>();
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) {
    const handler = (): void => {
      count++;
      if (count > 1) process.exit(exitCodes[signal] ?? 1);
      received = signal;
      controller.abort();
    };
    handlers.set(signal, handler);
    process.on(signal, handler);
  }
  return {
    controller,
    get signal() { return received; },
    get exitCode() { return received ? exitCodes[received] ?? 1 : 0; },
    dispose() {
      for (const [signal, handler] of handlers) process.off(signal, handler);
    },
  };
}
export async function finishOwnedLocalRun(
  stopServer: () => Promise<void>,
  removeMarker: () => Promise<void>,
  interrupt: { readonly signal: NodeJS.Signals | undefined; readonly exitCode: number; dispose(): void } | undefined,
  serveViewer: () => Promise<void>,
): Promise<void> {
  try {
    await stopServer();
    await removeMarker();
    if (interrupt?.signal) {
      process.exitCode = interrupt.exitCode;
      return;
    }
  } finally {
    interrupt?.dispose();
  }
  await serveViewer();
}

async function keepLocalViewerOpen(viewer: LocalViewer): Promise<void> {
  console.log(`Local results viewer: ${viewer.url}`);
  console.log('Press Ctrl+C to stop the viewer.');
  try {
    await new Promise<void>((resolvePromise) => {
      const stop = (): void => {
        process.off('SIGINT', stop);
        process.off('SIGTERM', stop);
        process.off('SIGHUP', stop);
        resolvePromise();
      };
      process.once('SIGINT', stop);
      process.once('SIGTERM', stop);
      process.once('SIGHUP', stop);
    });
  } finally {
    await viewer.close();
  }
}

function isPathWithin(root: string, target: string): boolean {
  const path = relative(root, target);
  return path !== '..' && !path.startsWith(`..${sep}`) && !isAbsolute(path);
}

export async function sendViewerFile(
  request: import('node:http').IncomingMessage,
  response: import('node:http').ServerResponse,
  data: Buffer,
  contentType: string,
): Promise<void> {
  const range = request.headers.range;
  const headers = {
    'accept-ranges': 'bytes',
    'cache-control': 'no-store',
    'content-type': contentType,
  };
  if (!range) {
    response.writeHead(200, { ...headers, 'content-length': data.length });
    response.end(request.method === 'HEAD' ? undefined : data);
    return;
  }
  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match || (!match[1] && !match[2])) {
    response.writeHead(416, { 'content-range': `bytes */${data.length}` }).end();
    return;
  }
  const suffixLength = match[1] ? undefined : Number(match[2]);
  const start = match[1] ? Number(match[1]) : Math.max(0, data.length - (suffixLength ?? 0));
  const end = match[2] && match[1] ? Math.min(Number(match[2]), data.length - 1) : data.length - 1;
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || end >= data.length) {
    response.writeHead(416, { 'content-range': `bytes */${data.length}` }).end();
    return;
  }
  const body = data.subarray(start, end + 1);
  response.writeHead(206, {
    ...headers,
    'content-length': body.length,
    'content-range': `bytes ${start}-${end}/${data.length}`,
  });
  response.end(request.method === 'HEAD' ? undefined : body);
}

export async function buildOwnedServer(
  workspace: string,
  runner: typeof run_tree = run_tree,
): Promise<{ executable: string; gitSha: string }> {
  const root = await realpath(workspace);
  const run = async (command: string[]): Promise<string> => {
    const result = await runner(command, command[0] === 'cargo' ? 600 : 15, root, undefined,
      command[0] === 'cargo' ? (chunk) => { process.stderr.write(chunk); } : undefined);
    if (result.returncode) throw new Error(`OAT ${command[0]} failed: ${result.stderr.trim()}`);
    return result.stdout;
  };
  const source = async () => ({
    sha: (await run(['git', 'rev-parse', '--verify', 'HEAD'])).trim(),
    status: await run(['git', 'status', '--porcelain', '--untracked-files=all', '--ignore-submodules=none']),
  });
  const before = await source();
  if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(before.sha)) {
    throw new Error('Git did not return a full OAT source commit');
  }
  const output = await run([
    'cargo', 'build', '--locked', '--package', 'arrowcol-oat-rust',
    '--bin', 'arrowcol-oat-rust', '--message-format=json-render-diagnostics',
  ]);
  const after = await source();
  if (before.sha !== after.sha || before.status !== after.status) {
    throw new Error('OAT source changed during the build; refusing to start an ambiguously attributed server');
  }
  const executables: string[] = [];
  for (const line of output.split('\n').filter((line) => line.trim())) {
    const message: unknown = JSON.parse(line);
    if (!message || typeof message !== 'object' ||
        !('reason' in message) || message.reason !== 'compiler-artifact' ||
        !('manifest_path' in message) ||
        message.manifest_path !== resolve(root, 'projects/arrowcol-oat-rust/Cargo.toml') ||
        !('target' in message) || !message.target || typeof message.target !== 'object' ||
        !('name' in message.target) || message.target.name !== 'arrowcol-oat-rust' ||
        !('kind' in message.target) || !Array.isArray(message.target.kind) || !message.target.kind.includes('bin') ||
        !('executable' in message) || typeof message.executable !== 'string' || !isAbsolute(message.executable)) continue;
    executables.push(message.executable);
  }
  if (executables.length !== 1) throw new Error('Cargo did not identify exactly one built OAT server executable');
  const gitSha = before.status.trim() ? 'unknown' : before.sha;
  if (gitSha === 'unknown') console.warn('OAT source has uncommitted changes; result git_sha will remain unknown.');
  return { executable: await realpath(executables[0]!), gitSha };
}

export async function startLocalServer(
  runner: typeof run_tree = run_tree,
  serverEnvironment: NodeJS.ProcessEnv = {},
): Promise<{ child: ChildProcess; url: string; gitSha: string; executable: string }> {
  const workspace = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  const build = await buildOwnedServer(workspace, runner);
  const port = await availablePort();
  const url = `http://127.0.0.1:${port}`;
  const child = spawn(build.executable, ['--listen', `127.0.0.1:${port}`], {
    cwd: workspace, detached: process.platform === 'linux', stdio: 'inherit',
    env: { ...process.env, ...serverEnvironment, GIT_SHA: build.gitSha },
  });
  let exit: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  let spawnError: Error | undefined;
  child.once('exit', (code, signal) => { exit = { code, signal }; });
  child.once('error', (error) => { spawnError = error; });
  const deadline = Date.now() + 10 * 60 * 1000;
  while (Date.now() < deadline) {
    if (spawnError) throw new Error(`Could not start OAT server: ${spawnError.message}`);
    if (exit) throw new Error(`OAT server exited before becoming ready (${exit.signal ?? exit.code})`);
    try {
      const response = await fetch(new URL('/healthz', url), { signal: AbortSignal.timeout(2000) });
      if (response.ok) return { child, url, gitSha: build.gitSha, executable: build.executable };
    } catch {
      // The server may still be compiling or starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  await stopLocalServer(child);
  throw new Error('OAT server did not become ready within ten minutes');
}

async function availablePort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === 'string') {
    server.close();
    throw new Error('Could not allocate a local OAT server port');
  }
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
}

export async function stopLocalServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
  child.kill('SIGTERM');
  let timeout: NodeJS.Timeout | undefined;
  const stopped = await Promise.race([
    exited.then(() => true),
    new Promise<boolean>((resolve) => { timeout = setTimeout(() => resolve(false), 5000); }),
  ]);
  if (timeout) clearTimeout(timeout);
  if (!stopped) throw new Error('OAT server did not stop after SIGTERM; check its process before retrying');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = cliExitCode(error);
  });
}
