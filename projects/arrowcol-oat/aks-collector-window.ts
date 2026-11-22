import type { DeltaCommitSummary } from './aks-delta-preservation.js';

export function reconcileCollectorWindow(logs: string, tableId: string | undefined,
  commits: DeltaCommitSummary[] | undefined, start: number, end: number, podUid: string) {
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start >= end || !podUid) {
    throw new Error('Collector window lacks exact interval or pod identity.');
  }
  const unavailable = (reason: string) => ({ schema_version: 1 as const, pod_uid: podUid,
    measurement_start_unix_ms: start, measurement_end_unix_ms: end,
    receipt_clock: 'finished_at_unix_ms', delta_clock: 'commitInfo.timestamp',
    completed_receipts: null, joined_rows: null, joined_files: null, joined_bytes: null,
    receipt_window_versions: null, delta_window_versions: null, receipt_only_versions: null,
    delta_only_versions: null, joined_receipts: null, unavailable_reason: reason });
  if (!tableId || !commits) return unavailable('Replay did not retain table identity and per-version Add summaries.');
  const number = (value: unknown, label: string): number => {
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Invalid collector ' + label);
    return value;
  };
  const byVersion = new Map(commits.map(commit => [commit.version, commit]));
  if (byVersion.size !== commits.length) throw new Error('Duplicate Delta commit summaries.');
  const receipts = new Map<number, { version: number; finished_at_unix_ms: number; rows: number; files: number; bytes: number }>();
  let uncertain = 0;
  for (const line of logs.split(/\r?\n/)) {
    const marker = 'arrowcol_data_commit ';
    if (!line.trimStart().startsWith(marker)) continue;
    const parsed: unknown = JSON.parse(line.trimStart().slice(marker.length));
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('Invalid collector receipt.');
    const value = parsed as Record<string, unknown>;
    if (value.table_id !== tableId) throw new Error('Collector emitted a receipt for an unexpected table.');
    if (value.outcome !== 'committed') { uncertain++; continue; }
    if (value.schema_version !== 1) throw new Error('Unsupported collector receipt schema.');
    const receipt = { version: number(value.delta_version, 'version'), finished_at_unix_ms: number(value.finished_at_unix_ms, 'completion clock'),
      rows: number(value.rows, 'rows'), files: number(value.files, 'files'), bytes: number(value.parquet_bytes, 'bytes') };
    const previous = receipts.get(receipt.version);
    if (previous && JSON.stringify(previous) !== JSON.stringify(receipt)) throw new Error('Conflicting completed receipts for one table/version.');
    receipts.set(receipt.version, receipt);
  }
  if (!receipts.size) return unavailable(uncertain ? 'Only attribution-uncertain receipts were captured.' : 'No completed collector receipts were captured.');
  const selected = [...receipts.values()].filter(receipt => receipt.finished_at_unix_ms >= start && receipt.finished_at_unix_ms < end);
  const joined = selected.map(receipt => {
    const commit = byVersion.get(receipt.version);
    if (!commit || commit.rows !== receipt.rows || commit.files !== receipt.files || commit.bytes !== receipt.bytes) {
      throw new Error('Completed collector receipt does not join exactly to replayed Delta Adds at version ' + receipt.version);
    }
    return { ...receipt, commit_info_timestamp_unix_ms: commit.commit_info_timestamp_unix_ms };
  }).sort((a, b) => a.version - b.version);
  const deltaVersions = commits.filter(commit => commit.commit_info_timestamp_unix_ms >= start && commit.commit_info_timestamp_unix_ms < end)
    .map(commit => commit.version).sort((a, b) => a - b);
  const receiptVersions = joined.map(receipt => receipt.version);
  const sum = (key: 'rows' | 'files' | 'bytes') => {
    const total = joined.reduce((value, receipt) => value + receipt[key], 0);
    if (!Number.isSafeInteger(total)) throw new Error('Collector window totals overflow.');
    return total;
  };
  return { schema_version: 1 as const, pod_uid: podUid, measurement_start_unix_ms: start, measurement_end_unix_ms: end,
    receipt_clock: 'finished_at_unix_ms', delta_clock: 'commitInfo.timestamp', completed_receipts: joined.length,
    joined_rows: sum('rows'), joined_files: sum('files'), joined_bytes: sum('bytes'),
    receipt_window_versions: receiptVersions, delta_window_versions: deltaVersions,
    receipt_only_versions: receiptVersions.filter(version => !deltaVersions.includes(version)),
    delta_only_versions: deltaVersions.filter(version => !receiptVersions.includes(version)),
    missing_delta_window_receipts: deltaVersions.filter(version => !receipts.has(version)),
    boundary_delta_only_versions: deltaVersions.filter(version => receipts.has(version) && !receiptVersions.includes(version)),
    count_scope: 'captured_completed_receipts_joined_to_replayed_adds',
    joined_receipts: joined, attribution_uncertain_receipts: uncertain, unavailable_reason: null };
}
