import { spawn } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { mkdir, open, type FileHandle } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import {
  redactString,
  renderReportMarkdown,
  renderTable,
  stringifySafeJson,
  toSafeJsonValue,
  writeFileAtomic,
  type Column,
  type JsonValue,
  type ResultsSection,
} from './report.js';

export type ExperimentStatus = 'passed' | 'finding' | 'inconclusive' | 'failed';

export const EXPERIMENT_STATUSES: readonly ExperimentStatus[] = ['passed', 'finding', 'inconclusive', 'failed'];

export interface StageResult<TRow> {
  readonly status: ExperimentStatus;
  readonly summary: string;
  readonly rows?: readonly TRow[];
}

export interface EvidenceRecorder {
  /**
   * Queues one JSONL record. Throws synchronously: JsonSafetyError for non-JSON-safe
   * data, EvidenceClosedError once the run has closed the log.
   */
  record(kind: string, data?: unknown): void;
  /** Resolves once every queued record is written and fsynced. Rejects if closed or on write failure. */
  flush(): Promise<void>;
}

export class EvidenceClosedError extends Error {
  constructor(kind: string) {
    super(`evidence log is closed; "${kind}" was not recorded`);
    this.name = 'EvidenceClosedError';
  }
}

export interface StageContext extends EvidenceRecorder {
  readonly experimentId: string;
  readonly stage: string;
  /** Aborted on caller cancellation, experiment deadline or stage timeout. The stage must stop its own effects. */
  readonly signal: AbortSignal;
  /** Epoch milliseconds of the earliest applicable deadline. */
  readonly deadline: number;
}

export interface Stage<TRow> {
  readonly name: string;
  readonly timeoutMs?: number;
  run(context: StageContext): Promise<StageResult<TRow>>;
}

export interface CleanupContext extends EvidenceRecorder {
  readonly experimentId: string;
  /** Independent of cancellation; bounded by `cleanupTimeoutMs`. */
  readonly signal: AbortSignal;
  readonly status: ExperimentStatus;
}

export interface Experiment<TRow> {
  readonly id: string;
  readonly title: string;
  readonly stages: readonly Stage<TRow>[];
  readonly columns: readonly Column<TRow>[];
  readonly cleanup?: (context: CleanupContext) => Promise<void>;
}

export interface EvidenceOptions {
  /** Default 10000. Records beyond the bound are counted as dropped. */
  readonly maxRecords?: number;
  /** Default 16 MiB. */
  readonly maxBytes?: number;
  /** fsync after this many written lines. Default 1 (every line). */
  readonly syncEvery?: number;
}

export interface RunOptions {
  /** Created if absent; must not already contain `evidence.jsonl`. */
  readonly outDir: string;
  /** Experiment deadline. Required so every run is bounded. */
  readonly timeoutMs: number;
  readonly signal?: AbortSignal;
  /** How long to wait for an aborted stage to settle before declaring it unsettled. Default 5000. */
  readonly settleTimeoutMs?: number;
  readonly cleanupTimeoutMs?: number;
  readonly evidence?: EvidenceOptions;
  readonly secrets?: readonly string[];
  readonly missing?: string;
  /** Receives exactly the Markdown written to `report.md`. Defaults to stdout. */
  readonly output?: (markdown: string) => void;
}

export interface StageReport {
  readonly name: string;
  readonly status: ExperimentStatus | 'not-run';
  readonly summary: string;
  readonly durationMs: number | null;
  readonly rowCount: number | null;
  readonly settled: boolean;
}

export interface ExperimentReport {
  readonly schemaVersion: 1;
  readonly experimentId: string;
  readonly title: string;
  readonly status: ExperimentStatus;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly durationMs: number;
  readonly abortReason: string | null;
  readonly stages: readonly StageReport[];
  readonly rows: readonly JsonValue[];
  readonly cleanup: { readonly status: 'ok' | 'failed' | 'skipped' | 'none'; readonly detail: string | null };
  readonly evidence: {
    readonly file: string;
    readonly records: number;
    readonly dropped: number;
    readonly truncated: boolean;
    readonly syncEvery: number;
    readonly syncs: number;
    readonly writeError: string | null;
  };
}

export interface RunOutcome {
  readonly report: ExperimentReport;
  readonly markdown: string;
  readonly files: { readonly evidence: string; readonly json: string; readonly markdown: string };
}

const SEVERITY: Readonly<Record<ExperimentStatus, number>> = { passed: 0, finding: 1, inconclusive: 2, failed: 3 };

/** failed > inconclusive > finding > passed. */
export function worstStatus(statuses: readonly ExperimentStatus[]): ExperimentStatus {
  return statuses.reduce<ExperimentStatus>((worst, next) => (SEVERITY[next] > SEVERITY[worst] ? next : worst), 'passed');
}

export function describeReason(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  if (typeof reason === 'string') return reason;
  return 'aborted';
}

class DeadlineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DeadlineError';
  }
}

interface Disposable<T> {
  readonly value: T;
  dispose(): void;
}

function deadlineSignal(ms: number, label: string): Disposable<AbortSignal> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new DeadlineError(`${label} exceeded ${ms}ms`)), ms);
  return { value: controller.signal, dispose: () => clearTimeout(timer) };
}

function whenAborted(signal: AbortSignal): Disposable<Promise<'aborted'>> {
  let onAbort = (): void => {};
  const promise = new Promise<'aborted'>((resolve) => {
    onAbort = () => resolve('aborted');
    if (signal.aborted) resolve('aborted');
    else signal.addEventListener('abort', onAbort, { once: true });
  });
  return { value: promise, dispose: () => signal.removeEventListener('abort', onAbort) };
}

function delay(ms: number): Disposable<Promise<'elapsed'>> {
  let timer: NodeJS.Timeout | undefined;
  const promise = new Promise<'elapsed'>((resolve) => {
    timer = setTimeout(() => resolve('elapsed'), ms);
  });
  return { value: promise, dispose: () => clearTimeout(timer) };
}

type Settlement<T> = { readonly kind: 'value'; readonly value: T } | { readonly kind: 'error'; readonly error: unknown };

/**
 * Observes (does not stop) `work`. After `signal` aborts, waits up to
 * `settleTimeoutMs` for the owner to settle; otherwise reports `unsettled`.
 */
async function observe<T>(
  work: () => Promise<T>,
  signal: AbortSignal,
  settleTimeoutMs: number,
): Promise<{ readonly aborted: boolean; readonly result: Settlement<T> | { readonly kind: 'unsettled' } }> {
  let pending: Promise<Settlement<T>>;
  try {
    pending = work().then(
      (value): Settlement<T> => ({ kind: 'value', value }),
      (error: unknown): Settlement<T> => ({ kind: 'error', error }),
    );
  } catch (error) {
    return { aborted: signal.aborted, result: { kind: 'error', error } };
  }
  const abort = whenAborted(signal);
  const first = await Promise.race([pending, abort.value]);
  abort.dispose();
  if (first !== 'aborted') return { aborted: false, result: first };
  const grace = delay(settleTimeoutMs);
  const second = await Promise.race([pending, grace.value]);
  grace.dispose();
  return { aborted: true, result: second === 'elapsed' ? { kind: 'unsettled' } : second };
}

function positiveInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive integer`);
  return value;
}

function nonNegativeMs(name: string, value: number): number {
  if (!Number.isFinite(value) || value < 0) throw new RangeError(`${name} must be a finite non-negative number`);
  return value;
}

class EvidenceLog {
  readonly file: string;
  records = 0;
  dropped = 0;
  syncs = 0;
  writeError: string | null = null;
  readonly syncEvery: number;
  private bytes = 0;
  private unsynced = 0;
  private closed = false;
  private chain: Promise<void> = Promise.resolve();
  private readonly handle: FileHandle;
  private readonly maxRecords: number;
  private readonly maxBytes: number;
  private readonly secrets: readonly string[];

  private constructor(file: string, handle: FileHandle, settings: Required<EvidenceOptions>, secrets: readonly string[]) {
    this.file = file;
    this.handle = handle;
    this.maxRecords = settings.maxRecords;
    this.maxBytes = settings.maxBytes;
    this.syncEvery = settings.syncEvery;
    this.secrets = secrets;
  }

  static settings(options: EvidenceOptions): Required<EvidenceOptions> {
    return {
      maxRecords: positiveInteger('evidence.maxRecords', options.maxRecords ?? 10_000),
      maxBytes: positiveInteger('evidence.maxBytes', options.maxBytes ?? 16 * 1024 * 1024),
      syncEvery: positiveInteger('evidence.syncEvery', options.syncEvery ?? 1),
    };
  }

  static async create(file: string, settings: Required<EvidenceOptions>, secrets: readonly string[]): Promise<EvidenceLog> {
    return new EvidenceLog(file, await open(file, 'ax', 0o600), settings, secrets);
  }

  get truncated(): boolean {
    return this.dropped > 0;
  }

  append(stage: string | null, kind: string, data: unknown): void {
    if (this.closed) throw new EvidenceClosedError(kind);
    const safe = toSafeJsonValue(data === undefined ? null : data, this.secrets, '$.data');
    const line = `${JSON.stringify({
      seq: this.records + 1,
      at: new Date().toISOString(),
      stage,
      kind: redactString(kind, this.secrets),
      data: safe,
    })}\n`;
    const size = Buffer.byteLength(line);
    if (this.records >= this.maxRecords || this.bytes + size > this.maxBytes) {
      this.dropped += 1;
      return;
    }
    this.records += 1;
    this.bytes += size;
    this.enqueue(line);
  }

  flush(): Promise<void> {
    if (this.closed) return Promise.reject(new EvidenceClosedError('flush'));
    this.chain = this.chain.then(() => this.syncPending());
    return this.chain.then(() => {
      if (this.writeError !== null) throw new Error(`evidence write failed: ${this.writeError}`);
    });
  }

  recorder(stage: string | null): EvidenceRecorder {
    return { record: (kind, data) => this.append(stage, kind, data), flush: () => this.flush() };
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.dropped > 0) {
      this.enqueue(`${JSON.stringify({ seq: null, at: new Date().toISOString(), stage: null, kind: 'evidence.truncated', data: { dropped: this.dropped } })}\n`);
    }
    this.chain = this.chain.then(() => this.syncPending());
    await this.chain;
    await this.handle.close();
  }

  private async syncPending(): Promise<void> {
    if (this.writeError !== null || this.unsynced === 0) return;
    try {
      await this.handle.datasync();
      this.unsynced = 0;
      this.syncs += 1;
    } catch (error) {
      this.writeError = describeReason(error);
    }
  }

  private enqueue(line: string): void {
    this.chain = this.chain.then(async () => {
      if (this.writeError !== null) return;
      try {
        await this.handle.appendFile(line, 'utf8');
        this.unsynced += 1;
      } catch (error) {
        this.writeError = describeReason(error);
        return;
      }
      if (this.unsynced >= this.syncEvery) await this.syncPending();
    });
  }
}

function isStageResult(value: unknown): value is StageResult<unknown> {
  if (typeof value !== 'object' || value === null) return false;
  const status: unknown = Reflect.get(value, 'status');
  const summary: unknown = Reflect.get(value, 'summary');
  const rows: unknown = Reflect.get(value, 'rows');
  return (
    typeof status === 'string' &&
    (EXPERIMENT_STATUSES as readonly string[]).includes(status) &&
    typeof summary === 'string' &&
    (rows === undefined || Array.isArray(rows))
  );
}

/**
 * Runs stages sequentially. Never throws for stage, cleanup or rendering
 * failures: the final report is always written to `outDir` and `output`.
 * Throws only if `outDir`/evidence cannot be created or report files cannot be written
 * (the Markdown is still sent to `output` before rethrowing).
 */
export async function runExperiment<TRow>(experiment: Experiment<TRow>, options: RunOptions): Promise<RunOutcome> {
  if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
    throw new RangeError('runExperiment requires a positive finite timeoutMs');
  }
  const secrets = options.secrets ?? [];
  const settleTimeoutMs = nonNegativeMs('settleTimeoutMs', options.settleTimeoutMs ?? 5_000);
  const cleanupTimeoutMs = nonNegativeMs('cleanupTimeoutMs', options.cleanupTimeoutMs ?? 30_000);
  const evidenceSettings = EvidenceLog.settings(options.evidence ?? {});
  const output = options.output ?? ((markdown: string) => void process.stdout.write(markdown));
  const files = {
    evidence: path.join(options.outDir, 'evidence.jsonl'),
    json: path.join(options.outDir, 'report.json'),
    markdown: path.join(options.outDir, 'report.md'),
  };
  await mkdir(options.outDir, { recursive: true });
  const evidence = await EvidenceLog.create(files.evidence, evidenceSettings, secrets);

  const startedAt = new Date();
  const startedMs = performance.now();
  const deadline = Date.now() + options.timeoutMs;
  const runDeadline = deadlineSignal(options.timeoutMs, 'experiment deadline');
  const runSignal = AbortSignal.any(options.signal ? [options.signal, runDeadline.value] : [runDeadline.value]);
  const stages: StageReport[] = [];
  const rows: TRow[] = [];
  const safeRows: JsonValue[] = [];
  let stop = false;
  let unsettled = false;
  const safeLog = (stage: string | null, kind: string, data: JsonValue): void => {
    try {
      evidence.append(stage, kind, data);
    } catch {
      // Lifecycle records are JSON-safe by construction.
    }
  };

  safeLog(null, 'experiment.start', { id: experiment.id, timeoutMs: options.timeoutMs });
  for (const stage of experiment.stages) {
    if (stop || runSignal.aborted) {
      stages.push({ name: stage.name, status: 'not-run', summary: 'not run', durationMs: null, rowCount: null, settled: true });
      continue;
    }
    const stageDeadline = stage.timeoutMs === undefined ? undefined : deadlineSignal(stage.timeoutMs, `stage ${stage.name} timeout`);
    const signal = stageDeadline ? AbortSignal.any([runSignal, stageDeadline.value]) : runSignal;
    const context: StageContext = {
      experimentId: experiment.id,
      stage: stage.name,
      signal,
      deadline: stage.timeoutMs === undefined ? deadline : Math.min(deadline, Date.now() + stage.timeoutMs),
      ...evidence.recorder(stage.name),
    };
    safeLog(stage.name, 'stage.start', null);
    const began = performance.now();
    const observed = await observe(() => stage.run(context), signal, settleTimeoutMs);
    stageDeadline?.dispose();
    const durationMs = Math.round(performance.now() - began);
    const reason = redactString(describeReason(signal.reason), secrets);
    let status: ExperimentStatus;
    let summary: string;
    let rowCount: number | null = null;
    let settled = true;

    if (observed.result.kind === 'unsettled') {
      status = 'failed';
      summary = `aborted (${reason}) but did not settle within ${settleTimeoutMs}ms; effects may still be running`;
      settled = false;
      unsettled = true;
    } else if (observed.result.kind === 'error') {
      const message = redactString(describeReason(observed.result.error), secrets);
      status = observed.aborted ? 'inconclusive' : 'failed';
      summary = observed.aborted ? `aborted: ${reason} (${message})` : `stage threw: ${message}`;
    } else if (!isStageResult(observed.result.value)) {
      status = 'failed';
      summary = 'stage returned an invalid StageResult';
    } else {
      const result = observed.result.value as StageResult<TRow>;
      const stageRows = result.rows ?? [];
      status = observed.aborted ? 'inconclusive' : result.status;
      summary = observed.aborted ? `aborted: ${reason}; partial result: ${result.summary}` : result.summary;
      try {
        const converted = stageRows.map((row, index) => toSafeJsonValue(row, secrets, `$.${stage.name}.rows[${index}]`));
        rows.push(...stageRows);
        safeRows.push(...converted);
        rowCount = stageRows.length;
      } catch (error) {
        status = 'failed';
        summary = `rows rejected: ${describeReason(error)}`;
      }
    }
    summary = redactString(summary, secrets);
    stages.push({ name: stage.name, status, summary, durationMs, rowCount, settled });
    safeLog(stage.name, 'stage.end', { status, summary, durationMs, rowCount, settled });
    if (status === 'failed' || observed.aborted) stop = true;
  }
  runDeadline.dispose();

  const abortReason = runSignal.aborted ? redactString(describeReason(runSignal.reason), secrets) : null;
  const statuses: ExperimentStatus[] = stages.map((stage) => (stage.status === 'not-run' ? 'inconclusive' : stage.status));
  let cleanup: ExperimentReport['cleanup'] = { status: 'none', detail: null };
  if (experiment.cleanup) {
    if (unsettled) {
      cleanup = { status: 'skipped', detail: 'a stage did not settle; cleanup must not race live effects' };
      statuses.push('failed');
    } else {
      const cleanupDeadline = deadlineSignal(cleanupTimeoutMs, 'cleanup timeout');
      const cleanupFn = experiment.cleanup;
      const observed = await observe(
        () =>
          cleanupFn({
            experimentId: experiment.id,
            signal: cleanupDeadline.value,
            status: worstStatus(statuses),
            ...evidence.recorder('cleanup'),
          }),
        cleanupDeadline.value,
        settleTimeoutMs,
      );
      cleanupDeadline.dispose();
      if (observed.result.kind === 'value' && !observed.aborted) {
        cleanup = { status: 'ok', detail: null };
      } else {
        const detail =
          observed.result.kind === 'error'
            ? describeReason(observed.result.error)
            : observed.result.kind === 'unsettled'
              ? `did not settle within ${settleTimeoutMs}ms after timeout`
              : describeReason(cleanupDeadline.value.reason);
        cleanup = { status: 'failed', detail: redactString(detail, secrets) };
        statuses.push('failed');
      }
    }
    safeLog('cleanup', 'cleanup.end', cleanup);
  }

  let results: ResultsSection;
  try {
    results = { table: renderTable(experiment.columns, rows, { missing: options.missing, secrets }) };
  } catch (error) {
    results = { error: describeReason(error) };
    statuses.push('failed');
  }
  safeLog(null, 'experiment.end', { status: worstStatus(statuses) });
  await evidence.close();
  if (evidence.writeError !== null) statuses.push('failed');

  const finishedAt = new Date();
  const report: ExperimentReport = {
    schemaVersion: 1,
    experimentId: redactString(experiment.id, secrets),
    title: redactString(experiment.title, secrets),
    status: worstStatus(statuses),
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: Math.round(performance.now() - startedMs),
    abortReason,
    stages,
    rows: safeRows,
    cleanup,
    evidence: {
      file: path.basename(files.evidence),
      records: evidence.records,
      dropped: evidence.dropped,
      truncated: evidence.truncated,
      syncEvery: evidence.syncEvery,
      syncs: evidence.syncs,
      writeError: evidence.writeError,
    },
  };
  const markdown = renderReportMarkdown(report, results, secrets);
  try {
    await writeFileAtomic(files.json, stringifySafeJson(report, secrets));
    await writeFileAtomic(files.markdown, markdown);
  } finally {
    output(markdown);
  }
  return { report, markdown, files };
}

export const DEFAULT_MAX_OUTPUT_BYTES = 32 * 1024 * 1024;

export interface ProcessOptions {
  readonly signal: AbortSignal;
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
  /** Time between SIGTERM and SIGKILL. Default 5000. */
  readonly killGraceMs?: number;
  /** Per-stream capture bound; excess output is discarded and flagged. Default 32 MiB. */
  readonly maxOutputBytes?: number;
  /** After the process (group) is gone, how long to wait for stdio to close before force-closing it. Default 2000. */
  readonly closeTimeoutMs?: number;
}

export interface ProcessResult {
  readonly exitCode: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly aborted: boolean;
  readonly stdout: string;
  readonly stderr: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
  /** POSIX: the child exited normally but left members in its process group, which were then terminated. */
  readonly strayProcessesKilled: boolean;
  /** False if stdio was still held open after closeTimeoutMs and had to be force-closed. */
  readonly outputClosed: boolean;
  readonly durationMs: number;
}

class BoundedCapture {
  private readonly chunks: Buffer[] = [];
  private size = 0;
  truncated = false;

  constructor(private readonly limit: number) {}

  push(chunk: Buffer): void {
    const room = this.limit - this.size;
    if (chunk.length > room) this.truncated = true;
    if (room <= 0) return;
    const kept = chunk.length > room ? chunk.subarray(0, room) : chunk;
    this.chunks.push(kept);
    this.size += kept.length;
  }

  text(): string {
    return Buffer.concat(this.chunks, this.size).toString('utf8');
  }
}

function procStat(pid: string): { readonly state: string; readonly pgrp: number } | undefined {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    return { state: fields[0] ?? '', pgrp: Number(fields[2]) };
  } catch {
    return undefined;
  }
}

/** Linux: confirms setsid() made the child its own group leader before any group signal is sent. */
function ownedGroup(pid: number | undefined): number | undefined {
  if (pid === undefined || process.platform === 'win32') return undefined;
  if (process.platform !== 'linux') return pid;
  return procStat(String(pid))?.pgrp === pid ? pid : undefined;
}

function groupHasLiveMembers(pgid: number): boolean {
  if (process.platform === 'linux') {
    try {
      for (const entry of readdirSync('/proc')) {
        if (!/^\d+$/.test(entry)) continue;
        const stat = procStat(entry);
        if (stat !== undefined && stat.pgrp === pgid && stat.state !== 'Z') return true;
      }
      return false;
    } catch {
      // Fall through to the signal probe.
    }
  }
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Spawns `command` with argv (never a shell). On POSIX the child runs in its
 * own process group (setsid); on abort the group gets SIGTERM, then SIGKILL
 * after `killGraceMs`, and stray group members left after a normal exit are
 * terminated the same way. Resolves only after the group is gone and stdio has
 * closed (or `closeTimeoutMs` elapsed). On Windows only the direct child is
 * signalled. Non-zero exit resolves; spawn errors reject. Output is not redacted.
 */
export async function runProcess(command: string, args: readonly string[], options: ProcessOptions): Promise<ProcessResult> {
  const { signal } = options;
  const killGraceMs = nonNegativeMs('killGraceMs', options.killGraceMs ?? 5_000);
  const closeTimeoutMs = nonNegativeMs('closeTimeoutMs', options.closeTimeoutMs ?? 2_000);
  const maxOutputBytes = options.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES;
  if (!Number.isSafeInteger(maxOutputBytes) || maxOutputBytes < 0) throw new RangeError('maxOutputBytes must be a non-negative integer');
  if (signal.aborted) throw new Error(`aborted before spawn: ${describeReason(signal.reason)}`);
  const began = performance.now();

  const child = spawn(command, [...args], {
    cwd: options.cwd,
    env: options.env,
    shell: false,
    detached: process.platform !== 'win32',
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const stdout = new BoundedCapture(maxOutputBytes);
  const stderr = new BoundedCapture(maxOutputBytes);
  child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));

  let exited = false;
  let closed = false;
  const closedPromise = new Promise<void>((resolve) => {
    child.once('close', () => {
      closed = true;
      resolve();
    });
  });
  const exitPromise = new Promise<{ readonly code: number | null; readonly sig: NodeJS.Signals | null }>((resolve, reject) => {
    child.on('error', reject);
    child.once('exit', (code, sig) => {
      exited = true;
      resolve({ code, sig });
    });
  });

  const pgid = ownedGroup(child.pid);
  let groupGone = pgid === undefined;
  const alive = (): boolean => {
    if (pgid === undefined) return !exited;
    if (!groupGone && !groupHasLiveMembers(pgid)) groupGone = true;
    return !groupGone;
  };
  const send = (sig: NodeJS.Signals): void => {
    try {
      if (pgid !== undefined) {
        if (alive()) process.kill(-pgid, sig);
      } else if (!exited) {
        child.kill(sig);
      }
    } catch {
      // Already gone.
    }
  };
  const waitUntilDead = async (ms: number): Promise<void> => {
    const until = performance.now() + ms;
    while (alive() && performance.now() < until) {
      const tick = delay(20);
      await tick.value;
    }
  };
  let termination: Promise<void> | undefined;
  const terminate = (): Promise<void> =>
    (termination ??= (async () => {
      send('SIGTERM');
      await waitUntilDead(killGraceMs);
      if (alive()) {
        send('SIGKILL');
        await waitUntilDead(closeTimeoutMs);
      }
    })());
  let aborted = false;
  const onAbort = (): void => {
    aborted = true;
    void terminate();
  };
  signal.addEventListener('abort', onAbort, { once: true });

  let exit: { readonly code: number | null; readonly sig: NodeJS.Signals | null };
  try {
    exit = await exitPromise;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
  let strayProcessesKilled = false;
  if (termination !== undefined) {
    await termination;
  } else if (pgid !== undefined && alive()) {
    strayProcessesKilled = true;
    await terminate();
  }
  if (!closed) {
    const grace = delay(closeTimeoutMs);
    await Promise.race([closedPromise, grace.value]);
    grace.dispose();
  }
  const outputClosed = closed;
  if (!outputClosed) {
    child.stdout.destroy();
    child.stderr.destroy();
  }
  return {
    exitCode: exit.code,
    signal: exit.sig,
    aborted,
    stdout: stdout.text(),
    stderr: stderr.text(),
    stdoutTruncated: stdout.truncated,
    stderrTruncated: stderr.truncated,
    strayProcessesKilled,
    outputClosed,
    durationMs: Math.round(performance.now() - began),
  };
}
