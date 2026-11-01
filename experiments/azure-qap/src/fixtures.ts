import fs from 'node:fs';
import path from 'node:path';
import { HttpFailure, Failure, parse_xml, type Azure } from '../../../infra/azure/bootstrap.ts';
import { runProcess, type ProcessOptions, type ProcessResult } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { groupKey, identifier, object } from './kusto.js';
import type { Operation } from './profile.js';

export interface SourceFile { path: string; bytes: number; partition: string }
export interface ExpectedGroup { partition: string; id: number; value: number; payloadSha256: string; multiplicity: number }
export interface Snapshot {
  version: number;
  rows: number;
  fingerprint: string;
  files: SourceFile[];
  expectedGroups: ExpectedGroup[];
}
export interface SeedResult extends Snapshot { tombstones: number; totalBytes: number }
export interface SourceDelta { added: SourceFile[]; removed: SourceFile[]; unchanged: SourceFile[] }
export interface MaintenanceObservation { filesDeleted: number; filesRemoved: number; interrupted: boolean; outcome: string }
export type Runner = (command: string, args: readonly string[], options: ProcessOptions) => Promise<ProcessResult>;

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

/** Trial names double as Delta table directories and must satisfy `arrowcol-experiment --table`. */
export function trialName(operation: Operation, repetition: number): string {
  if (!Number.isSafeInteger(repetition) || repetition < 1) throw new Error('Repetitions start at 1.');
  return repetition === 1 ? operation : `${operation}_${repetition}`;
}

/** Fixture partitions are minute-granular and relative to this anchor. */
export function anchorMinute(nowMs: number): number {
  return Math.floor(nowMs / 60_000) * 60_000;
}

function integer(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new Error(`Fixture returned an invalid ${name}.`);
  return value;
}

function relative(value: unknown): string {
  if (typeof value !== 'string' || !value || value.startsWith('/') || value.includes('\\') || value.includes('://') ||
      /[?#]/.test(value) || value.split('/').some(part => part === '..' || part === '.' || part === ''))
    throw new Error('Fixture returned an invalid relative file path.');
  return value;
}

export function parseSnapshot(value: unknown, trial: string, action: 'seed-scale' | 'inspect-scale'): Snapshot {
  const response = object(value);
  if (response.ok !== true || response.action !== action || response.table !== trial ||
      !Array.isArray(response.files) || !Array.isArray(response.expectedGroups) || typeof response.fingerprint !== 'string')
    throw new Error(`Fixture ${action} returned a malformed or mismatched result for ${trial}.`);
  const files = response.files.map(raw => {
    const file = object(raw);
    if (typeof file.partition !== 'string' || !file.partition) throw new Error('Fixture file has no partition.');
    return { path: relative(file.path), bytes: integer(file.bytes, 'file size'), partition: file.partition };
  });
  if (new Set(files.map(file => file.path)).size !== files.length) throw new Error('Fixture listed duplicate files.');
  const seen = new Set<string>();
  const expectedGroups = response.expectedGroups.map(raw => {
    const group = object(raw);
    if (typeof group.partition !== 'string' || typeof group.payloadSha256 !== 'string' ||
        !/^[0-9a-f]{64}$/.test(group.payloadSha256)) throw new Error('Fixture oracle group is malformed.');
    const result = { partition: group.partition, id: integer(group.id, 'group id'), value: integer(group.value, 'group value'),
      payloadSha256: group.payloadSha256, multiplicity: integer(group.multiplicity, 'multiplicity') };
    const key = groupKey(result);
    if (result.multiplicity === 0 || seen.has(key)) throw new Error('Fixture oracle group is empty or duplicated.');
    seen.add(key);
    return result;
  });
  const rows = integer(response.rows, 'row count');
  if (expectedGroups.reduce((sum, group) => sum + group.multiplicity, 0) !== rows)
    throw new Error('Fixture oracle groups do not account for every row.');
  return { version: integer(response.version, 'version'), rows, fingerprint: response.fingerprint, files, expectedGroups };
}

export function sourceDelta(before: SourceFile[], after: SourceFile[]): SourceDelta {
  const old = new Map(before.map(file => [file.path, file]));
  const current = new Map(after.map(file => [file.path, file]));
  const unchanged = after.filter(file => old.has(file.path));
  if (unchanged.some(file => old.get(file.path)!.bytes !== file.bytes || old.get(file.path)!.partition !== file.partition))
    throw new Error('A Delta file path was rewritten in place; unchanged-file evidence would be invalid.');
  return { added: after.filter(file => !old.has(file.path)), removed: before.filter(file => !current.has(file.path)), unchanged };
}

/** Each mutation must do what the demo claims, or its re-read numbers mean nothing. */
export function checkMutation(operation: Operation, before: Snapshot, after: Snapshot, filesDeleted: number): SourceDelta {
  const delta = sourceDelta(before.files, after.files);
  const fail = (reason: string) => { throw new Error(`${operation} broke its contract: ${reason}`); };
  if (!delta.unchanged.length) fail('no file stayed unchanged, so there is nothing to observe');
  if (operation === 'optimize') {
    if (!delta.added.length || !delta.removed.length) fail('OPTIMIZE did not rewrite any partition');
    if (after.rows !== before.rows) fail('OPTIMIZE changed the row count');
  } else if (operation === 'purge') {
    if (!delta.removed.length || delta.added.length || after.rows >= before.rows)
      fail('PURGE did not drop the expired partition without adding files');
  } else {
    if (after.version !== before.version || after.rows !== before.rows || delta.added.length || delta.removed.length)
      fail('the Delta snapshot changed');
    if (operation === 'vacuum' && filesDeleted === 0) fail('VACUUM deleted no tombstoned file');
  }
  return delta;
}

export function maintenanceArgs(root: string, trial: string, operation: Exclude<Operation, 'reload'>, anchorMs: number,
  apply: boolean, nowMs: number): string[] {
  identifier(trial);
  if (!Number.isSafeInteger(anchorMs) || anchorMs <= 0) throw new Error('Maintenance requires the fixture anchor.');
  // Partition retention is relative to the fixture anchor; VACUUM tombstone age uses the real clock.
  const args = ['--root', root, '--table', trial, '--operation', operation, '--granularity', 'minute',
    '--anchor-ms', String(operation === 'vacuum' ? nowMs : anchorMs), '--target-file-mib', '128', '--no-metrics-listener'];
  if (operation === 'optimize') args.push('--latest-partitions', '2');
  if (operation === 'purge') args.push('--purge-retention-secs', '86400');
  // Demo only: tombstones are seeded before QAP starts, so zero retention deletes nothing a reader still needs.
  if (operation === 'vacuum') args.push('--vacuum-retention-secs', '0', '--allow-short-vacuum-retention');
  if (apply) args.push('--apply');
  return args;
}

export function maintenanceReport(stdout: string): MaintenanceObservation {
  const candidates = stdout.split(/\r?\n/).filter(line => line.trim().startsWith('{'))
    .map(line => object(JSON.parse(line))).filter(row => row.report !== undefined);
  if (candidates.length !== 1) throw new Error('delta-maintenance must print exactly one JSON report.');
  const report = object(candidates[0]!.report);
  if (typeof report.outcome !== 'string' || typeof report.interrupted !== 'boolean')
    throw new Error('delta-maintenance report has no outcome.');
  return { filesDeleted: integer(report.files_deleted, 'files_deleted'), filesRemoved: integer(report.files_removed, 'files_removed'),
    outcome: report.outcome, interrupted: report.interrupted };
}

/** Environment for both Rust binaries: the run root is their only writable allowlist. */
export function childEnvironment(base: NodeJS.ProcessEnv, root: string, stage: string): NodeJS.ProcessEnv {
  if (!UUID.test(base.ARROWCOL_RESET_GENERATION ?? ''))
    throw new Error('ARROWCOL_RESET_GENERATION is missing; run `npx nx run azure-infra:provision` first.');
  if (!base.AZURE_SUBSCRIPTION_ID) throw new Error('AZURE_SUBSCRIPTION_ID is missing from the env file.');
  const env: NodeJS.ProcessEnv = { ...base, ARROWCOL_DELTA_ROOT: root, ARROWCOL_PARTITION_GRANULARITY: 'minute', TMPDIR: stage };
  delete env.ARROWCOL_LAYOUT_SORT_COLUMNS;
  delete env.ARROWCOL_LAYOUT_BLOOM_COLUMNS;
  return env;
}

export function transientTransport(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /operation timed out|request or response body error|connection (?:reset|closed)|error sending request/i.test(message);
}

export interface FixtureOptions {
  repo: string;
  root: string;
  env: NodeJS.ProcessEnv;
  redact: (text: string) => string;
  run?: Runner;
}

/** Drives `arrowcol-experiment` (seed/inspect) and `delta-maintenance` against `<root>/<trial>/`. */
export class Fixtures {
  readonly run: Runner;
  constructor(readonly options: FixtureOptions) {
    if (!options.root.endsWith('/')) throw new Error('Fixture root must end with a slash.');
    this.run = options.run ?? runProcess;
  }

  get experimentBinary(): string { return path.join(this.options.repo, 'target/release/arrowcol-experiment'); }
  get maintenanceBinary(): string { return path.join(this.options.repo, 'target/release/delta-maintenance'); }

  seedArgs(trial: string, anchorMs: number, fileMib: number, files: number, tombstones: boolean, seconds: number): string[] {
    return [...this.baseArgs(trial, seconds), 'seed-scale', '--anchor-ms', String(anchorMs), '--seed-mib', String(fileMib),
      '--files-per-partition', String(files), ...(tombstones ? ['--tombstones'] : [])];
  }

  inspectArgs(trial: string, seconds: number): string[] {
    return [...this.baseArgs(trial, seconds), 'inspect-scale'];
  }

  async seed(trial: string, anchorMs: number, fileMib: number, files: number, tombstones: boolean,
    signal: AbortSignal, seconds = 900): Promise<SeedResult> {
    const args = this.seedArgs(trial, anchorMs, fileMib, files, tombstones, seconds);
    let value: Record<string, unknown>;
    for (let attempt = 1; ; attempt++) {
      try { value = await this.command(this.experimentBinary, args, signal, seconds); break; }
      catch (error) {
        // seed-scale resumes from its durable manifest, so a dropped connection can be retried safely.
        if (attempt >= 3 || signal.aborted || !transientTransport(error)) throw error;
      }
    }
    if (value.publisherClosed !== true) throw new Error('Fixture seed did not close its Delta publisher cleanly.');
    const snapshot = parseSnapshot(value, trial, 'seed-scale');
    const tombstoneCount = value.tombstones === null || value.tombstones === undefined ? 0 : integer(value.tombstones, 'tombstones');
    if (tombstones && tombstoneCount === 0) throw new Error('VACUUM fixture has no pre-seeded tombstones.');
    return { ...snapshot, tombstones: tombstoneCount, totalBytes: integer(value.totalBytes, 'total bytes') };
  }

  async inspect(trial: string, signal: AbortSignal, seconds = 600): Promise<Snapshot> {
    for (let attempt = 1; ; attempt++) {
      try { return parseSnapshot(await this.command(this.experimentBinary, this.inspectArgs(trial, seconds), signal, seconds),
        trial, 'inspect-scale'); }
      catch (error) {
        if (attempt >= 3 || signal.aborted || !transientTransport(error)) throw error;
      }
    }
  }

  /** Dry run first (prints the plan), then apply. Returns the applied report. */
  async maintain(trial: string, operation: Exclude<Operation, 'reload'>, anchorMs: number, signal: AbortSignal,
    now: () => number = Date.now): Promise<{ plan: MaintenanceObservation; applied: MaintenanceObservation }> {
    const reports: MaintenanceObservation[] = [];
    this.stage();
    for (const apply of [false, true]) {
      const result = await this.run(this.maintenanceBinary, maintenanceArgs(this.options.root, trial, operation, anchorMs, apply, now()),
        { signal: AbortSignal.any([signal, AbortSignal.timeout(900_000)]), cwd: this.options.repo, env: this.options.env });
      if (result.aborted || result.exitCode !== 0 || !result.outputClosed || result.stdoutTruncated || result.stderrTruncated)
        throw new Error(this.options.redact(`delta-maintenance ${operation} ${apply ? 'apply' : 'dry run'} failed: ${tail(result.stderr)}`));
      const report = maintenanceReport(result.stdout);
      if (report.interrupted) throw new Error(`delta-maintenance ${operation} was interrupted.`);
      reports.push(report);
    }
    return { plan: reports[0]!, applied: reports[1]! };
  }

  maintainCommand(trial: string, operation: Exclude<Operation, 'reload'>, anchorMs: number, nowMs: number): string {
    return ['target/release/delta-maintenance', ...maintenanceArgs(this.options.root, trial, operation, anchorMs, true, nowMs)].join(' ');
  }

  private stage(): void {
    if (this.options.env.TMPDIR) fs.mkdirSync(this.options.env.TMPDIR, { recursive: true });
  }

  private baseArgs(trial: string, seconds: number): string[] {
    identifier(trial);
    return ['--root', this.options.root, '--table', trial, '--allow-table', trial, '--timeout-seconds', String(seconds)];
  }

  private async command(binary: string, args: string[], signal: AbortSignal, seconds: number): Promise<Record<string, unknown>> {
    signal.throwIfAborted();
    this.stage();
    const result = await this.run(binary, args, { signal: AbortSignal.any([signal, AbortSignal.timeout((seconds + 120) * 1000)]),
      cwd: this.options.repo, env: this.options.env, killGraceMs: 60_000, closeTimeoutMs: 5000 });
    if (!result.outputClosed || result.stdoutTruncated || result.stderrTruncated)
      throw new Error('Fixture process did not close its output; its Delta state is unknown.');
    const lines = result.stdout.trim().split(/\r?\n/).filter(Boolean);
    if (lines.length !== 1)
      throw new Error(this.options.redact(`Fixture ${args.at(-1)} returned no single JSON result: ${tail(result.stderr)}`));
    const value = object(JSON.parse(lines[0]!));
    if (value.ok !== true || result.exitCode !== 0 || result.aborted)
      throw new Error(this.options.redact(`Fixture command failed: ${lines[0]} ${tail(result.stderr)}`));
    return value;
  }
}

function tail(text: string): string {
  return text.trim().split(/\r?\n/).slice(-8).join(' | ').slice(-1500);
}

export function transientStorage(error: unknown): boolean {
  if (error instanceof HttpFailure) return [408, 429, 500, 502, 503, 504].includes(error.code);
  return error instanceof Failure;
}

export type StorageClient = Pick<Azure, 'request' | 'blob'> & { c: { account: string } };

/** Recursively delete `prefix` (a directory on the HNS account) and confirm no blob remains below it. */
export async function deletePrefix(azure: StorageClient, container: string, prefix: string,
  sleep: (ms: number) => Promise<void> = ms => new Promise(resolve => setTimeout(resolve, ms))): Promise<void> {
  const directory = prefix.replace(/\/+$/, '');
  if (!/^runs\/[a-z0-9_]+$/.test(directory)) throw new Error(`Refusing to delete unexpected prefix ${prefix}.`);
  let continuation = '';
  for (let pages = 0; ; pages++) {
    if (pages > 1000) throw new Error('Recursive delete did not finish after 1000 continuation pages.');
    const query = new URLSearchParams({ recursive: 'true', ...(continuation ? { continuation } : {}) });
    let headers: Record<string, string> = {};
    for (let attempt = 1; ; attempt++) {
      try {
        [, headers] = await azure.request('DELETE', `https://${azure.c.account}.dfs.core.windows.net/${container}/${directory}?${query}`,
          'https://storage.azure.com/', undefined, { 'x-ms-version': '2023-11-03' }, true);
        break;
      } catch (error) {
        if (attempt >= 4 || !transientStorage(error)) throw error;
        await sleep(2000 * attempt);
      }
    }
    continuation = headers['x-ms-continuation'] ?? '';
    if (!continuation) break;
  }
  const [data] = await azure.blob('GET', container, '', { restype: 'container', comp: 'list', prefix: directory + '/', maxresults: '5' });
  const listing = parse_xml(data?.toString() ?? '');
  const blobs = listing.children.find(child => child.name === 'Blobs')?.children.filter(child => child.name === 'Blob') ?? [];
  if (listing.name !== 'EnumerationResults' || blobs.length)
    throw new Error(`Fixture prefix ${directory}/ still has blobs after delete.`);
}
