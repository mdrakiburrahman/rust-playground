import fs from 'node:fs';
import path from 'node:path';
import { redactString, renderTable, toSafeJsonValue, writeFileAtomic, type Column } from '../../../tools/libs/experiment-kit/src/report.js';
import { formatDuration, MIB, type Profile } from './profile.js';
import type { TrialResult } from './scenario.js';

/** Append-only raw evidence (QAP samples, extents, Delta snapshots, log groups), one redacted JSON object per line. */
export class Evidence {
  constructor(readonly file: string, readonly secrets: readonly string[] = []) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
  }

  record(trial: string, kind: string, data: unknown): void {
    // JSON round trip drops undefined optionals, which the strict JSON-safety check would reject.
    const plain: unknown = JSON.parse(JSON.stringify({ at: new Date().toISOString(), trial, kind, data }));
    fs.appendFileSync(this.file, JSON.stringify(toSafeJsonValue(plain, this.secrets)) + '\n', { mode: 0o600 });
  }
}

export type ControlStatus = 'detected' | 'not-detected' | 'not-run';
export interface Control { status: ControlStatus; detail: string }

function reloadSeen(trial: TrialResult): boolean {
  return Boolean(trial.reads && trial.files && trial.files.unchanged > 0 && trial.reads.unchanged.files >= trial.files.unchanged);
}

function whyNotSeen(trial: TrialResult): string {
  const counted = trial.reads && trial.files ? `${trial.reads.unchanged.files}/${trial.files.unchanged} files re-read` : 'no reads counted';
  if (trial.status === 'failed') return `${trial.trial} failed (${counted})`;
  if (trial.status === 'budget-exceeded') return `${trial.trial} ran out of budget (${counted})`;
  if (trial.telemetry === 'pending') return `${trial.trial} logs still pending (${counted}); re-check with the printed query`;
  if (trial.telemetry === 'complete') return `${trial.trial} logs complete but only ${counted}`;
  return `${trial.trial} telemetry ${trial.telemetry} (${counted})`;
}

/**
 * The reload trial proves the log pipeline can see QAP reads at all: every file of a recreated table must show up as
 * read again. Without that, a zero re-read count for OPTIMIZE/PURGE/VACUUM would mean nothing, so any included reload
 * that is not seen (pending logs and budget overruns included) fails the control.
 */
export function positiveControl(trials: TrialResult[]): Control {
  const reloads = trials.filter(trial => trial.operation === 'reload');
  if (!reloads.length) return { status: 'not-run', detail: 'reload was not selected; zero re-reads cannot be trusted on their own.' };
  const unseen = reloads.filter(trial => !reloadSeen(trial));
  if (unseen.length) return { status: 'not-detected', detail: unseen.map(whyNotSeen).join('; ') + '.' };
  return { status: 'detected', detail: reloads.map(trial => `${trial.trial} re-read ${trial.reads!.unchanged.files}/` +
    `${trial.files!.unchanged} files`).join('; ') + ': StorageBlobLogs capture QAP reads.' };
}

export type CleanupStatus = 'done' | 'failed' | 'nothing';
export interface Cleanup { status: CleanupStatus; errors: string[] }

export interface RunReport {
  runId: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  profile: Record<string, unknown>;
  location: { account: string; container: string; database: string; workspace: string; workspaceCustomerId?: string; principal?: string };
  trials: TrialResult[];
  control: Control;
  cleanup: Cleanup;
  interrupted: boolean;
  error?: string;
  exitCode: number;
}

/** 0 when a report was produced; 1 on errors, failed trials, an included reload not detected or failed cleanup; 130 on Ctrl+C. */
export function exitCode(report: Pick<RunReport, 'trials' | 'control' | 'cleanup' | 'interrupted' | 'error'>): number {
  if (report.interrupted) return 130;
  if (report.error || report.trials.some(trial => trial.status === 'failed') || report.control.status === 'not-detected' ||
      report.cleanup.status === 'failed') return 1;
  return 0;
}

export function profileSummary(profile: Profile): Record<string, unknown> {
  return { operations: profile.operations, files: profile.files, fileMib: profile.fileMib, repetitions: profile.repetitions,
    parallel: profile.parallel, tail: formatDuration(profile.tailMs), logWait: formatDuration(profile.logWaitMs),
    budget: formatDuration(profile.budgetMs), settle: formatDuration(profile.settleMs), poll: formatDuration(profile.pollMs),
    logPoll: formatDuration(profile.logPollMs) };
}

const mib = (bytes: number) => Math.round(bytes / MIB * 10) / 10;
const time = (ms: number | undefined) => ms === undefined ? undefined : formatDuration(ms);

export const COLUMNS: Column<TrialResult>[] = [
  { header: 'Trial', value: trial => trial.trial },
  { header: 'Status', value: trial => trial.status },
  { header: 'Delta', value: trial => trial.versionBefore === undefined ? undefined
    : `v${trial.versionBefore}->${trial.versionAfter === undefined ? '?' : `v${trial.versionAfter}`}` },
  { header: 'Files unchanged', value: trial => trial.files?.unchanged, align: 'right' },
  { header: 'Unchanged re-read', value: trial => trial.reads && trial.files ? `${trial.reads.unchanged.files}/${trial.files.unchanged}`
    : undefined, align: 'right' },
  { header: 'Re-read', unit: 'MiB', value: trial => trial.reads ? mib(trial.reads.unchanged.bytes) : undefined, align: 'right' },
  { header: 'Warm-up reads', value: trial => trial.reads?.warmUp.files, align: 'right' },
  { header: 'Extent rows kept', value: trial => trial.extents ? `${trial.extents.keptRows}/${trial.extents.untouchedRows}` : undefined,
    align: 'right' },
  { header: 'Regressions', value: trial => trial.regressions, align: 'right' },
  { header: 'Catch-up', value: trial => time(trial.catchUpMs), align: 'right' },
  { header: 'Recovery', value: trial => time(trial.recoveryMs), align: 'right' },
  { header: 'Telemetry', value: trial => trial.telemetry },
  { header: 'Duration', value: trial => time(trial.durationMs), align: 'right' },
];

export function resultsTable(trials: TrialResult[], secrets: readonly string[] = []): string {
  return renderTable(COLUMNS, trials, { missing: '-', secrets });
}

export function queryFile(out: string, trial: TrialResult, raw = false): string {
  return path.join(out, 'queries', `${trial.trial}${raw ? '.raw' : ''}.kql`);
}

/** How to re-run the StorageBlobLogs queries once late logs have arrived. */
export function lateLogHelp(report: RunReport, out: string): string {
  const trials = report.trials.filter(trial => trial.readsQuery);
  if (!trials.length) return '';
  const workspace = report.location.workspaceCustomerId ?? '<workspace customer id>';
  return [
    `StorageBlobLogs often land 2-10 minutes late. Re-check any trial later (workspace ${report.location.workspace}):`,
    ...trials.map(trial => `  az monitor log-analytics query -w ${workspace} --analytics-query @${
      path.relative(process.cwd(), queryFile(out, trial))} -o table`),
    `Or paste a query file from ${path.relative(process.cwd(), path.join(out, 'queries'))}/ into the workspace's Logs blade.`,
    'Rows with Phase == "after-mutation" and OperationName GetBlob/ReadFile on files the mutation did not touch are QAP re-reads;',
    'the *.raw.kql files list individual requests.',
  ].join('\n');
}

function finding(trial: TrialResult): string | undefined {
  if (!trial.reads || !trial.files) return trial.error ? `- **${trial.trial}**: ${trial.status}: ${trial.error}` : undefined;
  const reread = trial.reads.unchanged;
  const verdict = trial.operation === 'reload' ? 'positive control'
    : reread.files ? 'QAP re-read files the mutation did not change' : 'no unchanged file was re-read';
  return `- **${trial.trial}** (${trial.telemetry === 'complete' ? 'logs complete' : 'logs still arriving'}): ` +
    `${reread.files}/${trial.files.unchanged} unchanged files (${mib(reread.bytes)} MiB) re-read after ${trial.operation}; ` +
    `${verdict}. Extents kept ${trial.extents?.keptRows ?? '?'}/${trial.extents?.untouchedRows ?? '?'} rows; ` +
    `${trial.regressions} QAP regression(s).`;
}

export function renderMarkdown(report: RunReport, out: string): string {
  const lines = [
    `# Azure QAP demo run ${report.runId}`,
    '',
    `Started ${report.startedAt}, took ${formatDuration(report.durationMs)}. Exit code ${report.exitCode}.`,
    '',
    `Storage \`${report.location.account}/${report.location.container}/runs/${report.runId}/\`, Kusto database ` +
      `\`${report.location.database}\`, Log Analytics workspace \`${report.location.workspace}\`.`,
    '',
    `Profile: \`${JSON.stringify(report.profile)}\``,
    '',
    '## Results',
    '',
    resultsTable(report.trials),
    '',
    `**Positive control:** ${report.control.status}: ${report.control.detail}`,
    '',
    ...(report.error ? [`**Run error:** ${report.error}`, ''] : []),
    '## Findings',
    '',
    ...report.trials.map(finding).filter((line): line is string => Boolean(line)),
    '',
    '## Columns',
    '',
    '- *Files unchanged*: Parquet files present before and after the mutation with the same path and size.',
    '- *Unchanged re-read*: of those, how many Kusto\'s managed identity read again (GetBlob/ReadFile, 2xx, bytes > 0) ' +
      'between the mutation and the end of the tail window.',
    '- *Warm-up reads*: Parquet files read while QAP accelerated the fresh table (expected: all of them).',
    '- *Extent rows kept*: rows of partitions the mutation did not touch that still sit in their original accelerated extents.',
    '- *Regressions*: times QAP looked caught up and then fell behind again.',
    '',
    `## Cleanup: ${report.cleanup.status}`,
    '',
    ...report.cleanup.errors.map(error => `- ${error}`),
    '',
  ];
  const help = lateLogHelp(report, out);
  if (help) lines.push('## Late logs', '', '```text', help, '```', '');
  return lines.join('\n');
}

export async function writeReport(report: RunReport, out: string, secrets: readonly string[] = []): Promise<void> {
  fs.mkdirSync(path.join(out, 'queries'), { recursive: true });
  for (const trial of report.trials) {
    if (trial.readsQuery) await writeFileAtomic(queryFile(out, trial), trial.readsQuery + '\n');
    if (trial.rawQuery) await writeFileAtomic(queryFile(out, trial, true), trial.rawQuery + '\n');
  }
  const plain: unknown = JSON.parse(JSON.stringify(report));
  await writeFileAtomic(path.join(out, 'report.json'), JSON.stringify(toSafeJsonValue(plain, secrets), null, 2) + '\n');
  await writeFileAtomic(path.join(out, 'report.md'), redactString(renderMarkdown(report, out), secrets));
}
