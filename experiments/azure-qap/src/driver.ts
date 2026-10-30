import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runProcess } from '../../../tools/libs/experiment-kit/src/experiment.js';
import type { Resources } from './resources.js';
import type { Profile } from './profile.js';
import { identifier, object, type Emit } from './kusto.js';
import { fingerprint, type FixtureDriver, type Oracle, type Operation, type Writer } from './scenarios.js';
import { maintain } from './maintenance.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const columns = ['id', 'value', '__arrowcol_ingestion'];

export function physicalTable(table: string): string {
  identifier(table);
  return 'experiment_' + table;
}

function integer(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new Error(`Fixture helper returned an invalid ${name}.`);
  return value;
}

export function parseOracle(value: unknown, table: string): Oracle {
  const response = object(value);
  if (response.ok !== true || response.action !== 'inspect' || response.table !== physicalTable(table) ||
      !Array.isArray(response.records) || !Array.isArray(response.files))
    throw new Error('Fixture helper returned a malformed or mismatched oracle.');
  const records = response.records.map(raw => {
    if (!Array.isArray(raw) || raw.length !== 3 || typeof raw[2] !== 'string')
      throw new Error('Fixture oracle rows do not match the fixed id/value/partition schema.');
    integer(raw[0], 'record id');
    integer(raw[1], 'record value');
    return raw;
  });
  const files = response.files.map(raw => {
    const file = object(raw);
    if (typeof file.path !== 'string' || !file.path || file.path.startsWith('/') || file.path.includes('\\') ||
        file.path.includes('://') || /[?#]/.test(file.path) || file.path.split('/').some(part => part === '..' || part === '.') ||
        typeof file.partition !== 'string' || !file.partition)
      throw new Error('Fixture oracle contains an invalid relative file identity or partition.');
    return { path: `${physicalTable(table)}/${file.path}`, bytes: integer(file.bytes, 'file size'), partition: file.partition };
  });
  const rows = integer(response.rows, 'row count');
  if (rows !== records.length || rows > 90_000 || new Set(files.map(file => file.path)).size !== files.length)
    throw new Error('Fixture oracle has missing/duplicate rows, duplicate files or exceeds the exact-query budget.');
  const digest = fingerprint(records);
  if (response.fingerprint !== digest) throw new Error('Fixture oracle fingerprint does not match its exact row multiset.');
  return { version: integer(response.version, 'version'), rows, records, files, columns: [...columns], fingerprint: digest };
}

export class Driver implements FixtureDriver {
  readonly anchorMs = Math.floor(Date.now() / 60_000) * 60_000;
  private readonly nextIds = new Map<string, number>();
  private readonly controlPartitions = new Map<string, number>();

  constructor(readonly resources: Resources, readonly config: Profile, readonly emit: Emit) {}

  async seed(table: string, signal: AbortSignal): Promise<Oracle> {
    if (this.nextIds.has(table)) throw new Error('Fixture seed cannot adopt or overwrite an initialized table.');
    const result = await this.command(table, ['seed', '--anchor-ms', String(this.anchorMs), '--start-id', '0',
      '--rows-per-file', String(this.config.rowsPerFile),
      '--files-per-partition', String(this.config.filesPerPartition)], signal);
    this.rememberMutation(table, result);
    if (!Array.isArray(result.partitions)) throw new Error('Seed did not identify its deterministic Hive partition roles.');
    const control = result.partitions.map(object).find(partition => partition.role === 'control');
    const timestamp = integer(control?.timestamp_ms, 'control partition timestamp');
    this.controlPartitions.set(table, timestamp);
    return this.inspect(table, signal);
  }

  async seedBroad(table: string, signal: AbortSignal): Promise<Oracle> {
    await this.seed(table, signal);
    const controlMs = this.controlPartitions.get(table);
    if (controlMs === undefined) throw new Error('Broad fixture control partition is absent.');
    await this.appendAt(table, 2, controlMs, 0, signal);
    return this.inspect(table, signal);
  }

  async inspect(table: string, signal: AbortSignal): Promise<Oracle> {
    return parseOracle(await this.command(table, ['inspect'], signal), table);
  }

  async append(table: string, batches: number, signal: AbortSignal): Promise<void> {
    await this.appendAt(table, batches, this.anchorMs, 0, signal);
  }

  async writer(table: string, signal: AbortSignal): Promise<Writer> {
    const controller = new AbortController();
    const combined = AbortSignal.any([signal, controller.signal]);
    const finished = this.appendAt(table, this.config.appendBatches, this.anchorMs,
      this.config.appendIntervalMs, combined, true);
    return { finished, stop: async () => { controller.abort(); await finished; } };
  }

  async maintain(table: string, operation: Operation, signal: AbortSignal) {
    return maintain(this.resources, this.config, table, this.anchorMs, operation, signal, this.emit);
  }

  private async appendAt(table: string, batches: number, partitionMs: number, intervalMs: number,
    signal: AbortSignal, allowCancelled = false): Promise<void> {
    const startId = this.nextIds.get(table);
    if (startId === undefined) throw new Error('Append requires a positively initialized fixture.');
    const result = await this.command(table, ['append', '--start-id', String(startId),
      '--batches', String(batches), '--rows-per-batch', String(this.config.rowsPerFile),
      '--interval-ms', String(intervalMs), '--partition-ms', String(partitionMs)], signal, allowCancelled);
    this.rememberMutation(table, result);
    if (result.cancelled !== true && result.batches !== batches)
      throw new Error('Fixture writer did not publish every requested batch.');
  }

  private rememberMutation(table: string, result: Record<string, unknown>): void {
    if (result.publisherClosed !== true) throw new Error('Fixture publisher did not establish definite closure.');
    this.nextIds.set(table, integer(result.nextId, 'next publication id'));
    this.emit('arrowcol-publication', { at: new Date().toISOString(), table, action: result.action,
      batches: result.batches, startId: result.startId, nextId: result.nextId,
      cancelled: result.cancelled, publisherClosed: true, version: result.version });
  }

  private async command(table: string, args: string[], signal: AbortSignal,
    allowCancelled = false): Promise<Record<string, unknown>> {
    signal.throwIfAborted();
    const name = physicalTable(table);
    this.emit('fixture-request', { at: new Date().toISOString(), table, action: args[0] });
    const result = await runProcess(path.join(root, 'target/release/arrowcol-experiment'),
      ['--root', this.resources.config.table_root, '--table', name, '--allow-table', name,
        '--timeout-seconds', '180', ...args],
      { signal: AbortSignal.any([signal, AbortSignal.timeout(240_000)]),
        cwd: root, env: this.resources.environment(), killGraceMs: 60_000, closeTimeoutMs: 5000 });
    if (!result.outputClosed || result.stdoutTruncated || result.stderrTruncated)
      throw new Error('Fixture process did not close its bounded output; publisher closure is unknown.');
    const lines = result.stdout.trim().split(/\r?\n/).filter(Boolean);
    if (lines.length !== 1)
      throw new Error(this.resources.config.redact(`Fixture helper returned no unique structured result: ${result.stderr}`));
    const parsed: unknown = JSON.parse(lines[0]!);
    const value = object(parsed);
    const cancelled = allowCancelled && result.aborted && result.exitCode === 130 &&
      value.cancelled === true && value.publisherClosed === true;
    if (value.ok !== true || value.table !== name || value.action !== args[0] ||
        (!cancelled && (result.exitCode !== 0 || result.aborted)))
      throw new Error(this.resources.config.redact(`Fixture helper failed: ${result.stderr} ${JSON.stringify(value)}`));
    return value;
  }
}
