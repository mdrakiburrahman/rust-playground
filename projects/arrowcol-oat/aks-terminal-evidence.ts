import { createHash } from 'node:crypto';

export const COLLECTOR_TERMINAL_LOG_MAX_BYTES = 64 * 1024 * 1024;

export interface TerminalCommit {
  table_id: string;
  version: number;
  rows: number;
  files: number;
  parquet_bytes: number;
}
export interface TerminalSummary {
  schema_version: 2;
  admitted: number;
  committed: number;
  failed: number;
  cancelled_ambiguous: number;
  pending: number;
  committed_rows: number;
  incomplete: 0 | 1;
  commits: TerminalCommit[];
  publication_evidence: TerminalPublicationCall[];
}
export interface LegacyTerminalSummary {
  schema_version: 1;
  admitted: number;
  committed: number;
  failed: number;
  cancelled_ambiguous: number;
  pending: number;
  committed_rows: number;
  incomplete: 0 | 1;
  commits: TerminalCommit[];
}
export interface TerminalPublicationOutcome {
  app_id: string;
  app_version: number;
  status: 'committed_now' | 'already_committed' | 'unknown';
  delta_version: number | null;
  reason: string | null;
}
export interface TerminalPublicationCall {
  clean: boolean;
  reasons: string[];
  outcomes: TerminalPublicationOutcome[];
}
export type TerminalSummaryRecord = LegacyTerminalSummary | TerminalSummary;
export interface BoundTerminalSummary {
  pod_name: string;
  pod_uid: string;
  container: string;
  restart_count: 0;
  exit_code: 0;
  summary: TerminalSummary;
}
const exact = (value: unknown, keys: string[]): value is Record<string, unknown> =>
  !!value && typeof value === 'object' && !Array.isArray(value) &&
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const count = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
const terminalPublicationReasons = new Set(['not_submitted', 'no_commit_provenance', 'deduplicated', 'partial_group',
  'unidentifiable', 'fenced', 'conflict', 'retry', 'retryable_io', 'reconciled', 'sync_failed', 'error']);
const terminalSummaryKeys = ['schema_version', 'admitted', 'committed', 'failed', 'cancelled_ambiguous', 'pending',
  'committed_rows', 'incomplete', 'commits'];

function validatePublicationEvidence(value: unknown, commits: TerminalCommit[]): void {
  if (!Array.isArray(value) || value.length > 100_000)
    throw new Error('Terminal publication evidence is unavailable.');
  const identities = new Set<string>();
  const publicationVersions: number[] = [];
  let unknownOutcome = false;
  for (const call of value) {
    if (!exact(call, ['clean', 'reasons', 'outcomes']) || typeof call.clean !== 'boolean' ||
        !Array.isArray(call.reasons) || call.reasons.length > terminalPublicationReasons.size ||
        call.reasons.some(reason => typeof reason !== 'string' || !terminalPublicationReasons.has(reason)) ||
        new Set(call.reasons).size !== call.reasons.length || !Array.isArray(call.outcomes) || call.outcomes.length > 100_000)
      throw new Error('Terminal publication call evidence is malformed.');
    for (const outcome of call.outcomes) {
      if (!exact(outcome, ['app_id', 'app_version', 'status', 'delta_version', 'reason']) ||
          typeof outcome.app_id !== 'string' || outcome.app_id.length === 0 || outcome.app_id.length > 253 ||
          /[\x00-\x1f\x7f]/.test(outcome.app_id) || !count(outcome.app_version))
        throw new Error('Terminal publication identity is malformed.');
      const identity = JSON.stringify([outcome.app_id, outcome.app_version]);
      if (identities.has(identity)) throw new Error('Duplicate terminal publication identity and version.');
      identities.add(identity);
      if (outcome.status === 'committed_now' || outcome.status === 'already_committed') {
        if (!count(outcome.delta_version) || outcome.reason !== null)
          throw new Error('Terminal publication outcome has no exact Delta version.');
        publicationVersions.push(outcome.delta_version);
      } else if (outcome.status === 'unknown') {
        if (outcome.delta_version !== null || typeof outcome.reason !== 'string' ||
            !terminalPublicationReasons.has(outcome.reason))
          throw new Error('Unknown terminal publication outcome is malformed.');
        unknownOutcome = true;
      } else {
        throw new Error('Terminal publication status is unavailable.');
      }
    }
  }
  const commitVersions = commits.map(commit => commit.version);
  const commitVersionSet = new Set(commitVersions);
  if (publicationVersions.some(version => !commitVersionSet.has(version)) ||
      new Set(publicationVersions).size !== publicationVersions.length ||
      !unknownOutcome && (publicationVersions.length !== commits.length ||
        commitVersionSet.size !== commits.length || publicationVersions.length !== commitVersionSet.size))
    throw new Error('Terminal publication versions differ from physical Delta commits.');
}

export function parseTerminalSummary(value: unknown): TerminalSummaryRecord {
  const legacy = exact(value, terminalSummaryKeys) && value.schema_version === 1;
  const currentKeys = [...terminalSummaryKeys, 'publication_evidence'];
  const current = exact(value, currentKeys) && value.schema_version === 2;
  if ((!legacy && !current) ||
      !value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('Terminal summary schema/counts are unavailable.');
  const record = value as Record<string, unknown>;
  if (!terminalSummaryKeys.slice(1, 7).every(key => count(record[key])) ||
      ![0, 1].includes(Number(record.incomplete)) || typeof record.incomplete !== 'number' ||
      !Array.isArray(record.commits) || record.commits.length > 20_000)
    throw new Error('Terminal summary schema/counts are unavailable.');
  for (const commit of record.commits) {
    if (!exact(commit, ['table_id', 'version', 'rows', 'files', 'parquet_bytes']) ||
        typeof commit.table_id !== 'string' || !/^[a-f0-9]{64}$/.test(commit.table_id) ||
        !['version', 'rows', 'files', 'parquet_bytes'].every(key => count(commit[key])))
      throw new Error('Terminal summary commit tuple is unavailable.');
  }
  const summary = JSON.parse(JSON.stringify(value)) as TerminalSummaryRecord;
  if (summary.schema_version === 2) validatePublicationEvidence(summary.publication_evidence, summary.commits);
  return summary;
}
export function terminalSummaryClear(summary: TerminalSummaryRecord): summary is TerminalSummary {
  if (summary.schema_version !== 2 || summary.publication_evidence.some(call =>
    !call.clean || call.reasons.length !== 0 || call.outcomes.length === 0 ||
    call.outcomes.some(outcome => (outcome.status !== 'committed_now' && outcome.status !== 'already_committed') ||
      outcome.delta_version === null || outcome.reason !== null))) return false;
  const accounted = summary.committed + summary.failed + summary.cancelled_ambiguous + summary.pending;
  const rows = summary.commits.reduce((sum, commit) => sum + commit.rows, 0);
  const publicationVersions = summary.publication_evidence.flatMap(call =>
    call.outcomes.flatMap(outcome => outcome.delta_version === null ? [] : [outcome.delta_version]));
  const commitVersions = summary.commits.map(commit => commit.version);
  const publicationVersionSet = new Set(publicationVersions);
  const commitVersionSet = new Set(commitVersions);
  return summary.incomplete === 0 && summary.failed === 0 && summary.cancelled_ambiguous === 0 && summary.pending === 0 &&
    Number.isSafeInteger(accounted) && accounted === summary.admitted &&
    Number.isSafeInteger(rows) && rows === summary.committed_rows &&
    new Set(summary.commits.map(commit => `${commit.table_id}:${commit.version}`)).size === summary.commits.length &&
    publicationVersions.length === commitVersions.length && publicationVersionSet.size === publicationVersions.length &&
    commitVersionSet.size === commitVersions.length &&
    publicationVersions.every(version => commitVersionSet.has(version));
}
export function extractTerminalSummary(logs: string): { summary: TerminalSummaryRecord | null; status: 0 | 1 | 2 | 3 } {
  const lines = logs.split('\n').filter(line => line.includes('arrowcol_terminal_evidence'));
  if (lines.length === 0) return { summary: null, status: 1 };
  if (lines.length !== 1) return { summary: null, status: 2 };
  const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z arrowcol_terminal_evidence (\{.*\})$/.exec(lines[0]!);
  try {
    if (!match) throw new Error('Terminal summary log envelope is unavailable.');
    return { summary: parseTerminalSummary(JSON.parse(match[1]!)), status: 0 };
  } catch { return { summary: null, status: 3 }; }
}
export function collectorStartupObserved(logs: string): boolean {
  const lines = logs.split('\n'), terminal = lines.findIndex(line => line.includes('arrowcol_terminal_evidence'));
  if (terminal < 0 || !logs.endsWith('\n') || logs.includes('\uFFFD')) return false;
  return lines.slice(0, terminal).some(line => {
    const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z arrowcol_effective_config (\{.*\})$/.exec(line);
    try {
      const value: unknown = match ? JSON.parse(match[1]!) : null;
      return !!value && typeof value === 'object' && !Array.isArray(value) &&
        (value as Record<string, unknown>).component === 'receiver';
    } catch { return false; }
  });
}
export function terminalTableDigest(account: string, container: string, root: string): string {
  if (!/^[a-z0-9]+$/.test(account) || !/^[a-z0-9-]+$/.test(container) ||
      !/^[A-Za-z0-9_./-]+$/.test(root) || root.split('/').some(part => !part || part === '.' || part === '..'))
    throw new Error('Canonical configured terminal table URL is unavailable.');
  // Only the configured ABFSS binding is supported; other schemes are not equivalent hashes.
  return createHash('sha256').update(`abfss://${container}@${account}.dfs.core.windows.net/${root}/`).digest('hex');
}
