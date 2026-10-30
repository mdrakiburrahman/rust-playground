import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { runProcess } from '../../../tools/libs/experiment-kit/src/experiment.js';
import type { Resources } from './resources.js';
import type { Profile } from './profile.js';
import type { Emit } from './kusto.js';
import { identifier, object } from './kusto.js';
import type { MaintenanceResult, Operation } from './scenarios.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');

export function maintenanceArgs(parent: string, table: string, anchorMs: number, operation: Operation,
  config: Profile, apply: boolean, nowMs = Date.now()): string[] {
  identifier(table);
  if (!Number.isSafeInteger(anchorMs) || anchorMs <= 0) throw new Error('Maintenance requires the fixed fixture anchor.');
  const kind = operation.endsWith('optimize') ? 'optimize' : operation;
  // Tombstone retention uses the real clock, not the fixture's fixed partition anchor.
  const clock = operation === 'vacuum' ? nowMs : anchorMs;
  const args = ['--root', parent, '--table', table, '--granularity', 'minute',
    '--anchor-ms', String(clock), '--operation', kind];
  if (operation === 'partial-optimize') args.push('--latest-partitions', '2');
  if (operation === 'purge') args.push('--purge-retention-secs', '86400');
  if (operation === 'vacuum') {
    args.push('--vacuum-retention-secs', String(config.vacuumRetentionSeconds));
    if (config.vacuumRetentionSeconds < 604_800) args.push('--allow-short-vacuum-retention');
  }
  if (apply) args.push('--apply');
  return args;
}

export interface MaintenanceObservation {
  filesDeleted: number;
  filesRemoved: number;
  interrupted: boolean;
  outcome: string;
}

export function maintenanceReport(stdout: string): MaintenanceObservation {
  const candidates = stdout.split(/\r?\n/).filter(Boolean).map(line => object(JSON.parse(line)))
    .filter(row => row.report !== undefined);
  if (candidates.length !== 1) throw new Error('Maintenance must return exactly one structured report.');
  const report = object(candidates[0]!.report);
  const integer = (name: string): number => {
    const value = report[name];
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
      throw new Error(`Maintenance report is missing a valid ${name}.`);
    return value;
  };
  if (typeof report.outcome !== 'string' || typeof report.interrupted !== 'boolean')
    throw new Error('Maintenance outcome or interruption state is absent.');
  return { filesDeleted: integer('files_deleted'), filesRemoved: integer('files_removed'),
    outcome: report.outcome, interrupted: report.interrupted };
}

export async function maintain(resources: Resources, config: Profile, table: string, anchorMs: number,
  operation: Operation, signal: AbortSignal, emit: Emit): Promise<MaintenanceResult> {
  const command = path.join(root, 'target/release/delta-maintenance');
  let applied: MaintenanceObservation | undefined;
  for (const apply of [false, true]) {
    const result = await runProcess(command,
      maintenanceArgs(resources.config.table_root + 'experiment/fixtures/', 'experiment_' + table,
        anchorMs, operation, config, apply),
      { signal: AbortSignal.any([signal, AbortSignal.timeout(180_000)]), cwd: root, env: resources.environment() });
    if (result.aborted || result.exitCode !== 0 || !result.outputClosed ||
        result.stdoutTruncated || result.stderrTruncated)
      throw new Error(resources.config.redact(`Maintenance ${operation} ${apply ? 'apply' : 'dry run'} failed: ${result.stderr}`));
    const report = maintenanceReport(result.stdout);
    emit('maintenance', { at: new Date().toISOString(), table, operation, apply, ...report,
      vacuumRetentionSeconds: operation === 'vacuum' ? config.vacuumRetentionSeconds : null });
    if (report.interrupted) throw new Error('Maintenance was interrupted; its mutation is not reported as complete.');
    if (apply) applied = report;
  }
  if (!applied) throw new Error('Maintenance apply report is absent.');
  return { changed: operation === 'vacuum' ? applied.filesDeleted > 0 : applied.filesRemoved > 0,
    deletedFiles: applied.filesDeleted };
}
