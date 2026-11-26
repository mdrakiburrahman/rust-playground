import assert from 'node:assert/strict';
import test from 'node:test';
import { canonicalDeltaWindow, collectWriterCommitRecords, reconcileDeltaAdds, type WriterCommitRecord } from './aks-delta-preservation.js';

function commit(timestamp: number, rows: number, dataChange = true, size = rows * 10): Record<string, unknown>[] {
  return [
    { commitInfo: { timestamp } },
    { add: { path: `part-${timestamp}.parquet`, size, dataChange, stats: JSON.stringify({ numRecords: rows }), partitionValues: {} } },
  ];
}

test('Delta Add reconciliation uses commit timestamps and excludes non-data-changing files', () => {
  const result = reconcileDeltaAdds(new Map([
    [1, commit(999, 5)],
    [2, commit(1000, 7)],
    [3, commit(1999, 9)],
    [4, commit(2000, 11)],
    [5, commit(1500, 7, false)],
  ]), 1000, 2000);
  const { commits, ...window } = result;
  assert.deepEqual(window, {
    total_rows: 32, total_files: 4, total_bytes: 320, total_commits: 4,
    window_rows: 16, window_files: 2, window_bytes: 160, window_commits: 2,
    first_version: 2, last_version: 3,
  });
  assert.deepEqual(commits.map(commit => commit.version), [1, 2, 3, 4]);
});

test('canonical rows join writer finish times to Delta Adds and fail closed on gaps or ambiguity', () => {
  const adds = [1, 2, 3, 4, 5].map(version => ({ version, timestamp: 0, rows: version * 10, files: 1, bytes: version * 100 }));
  const log = (version: number, finished: number, outcome = 'committed') => 'arrowcol_data_commit ' + JSON.stringify({
    schema_version: 1, outcome, table_id: 't', delta_version: version, rows: version * 10, files: 1, finished_at_unix_ms: finished });
  const records = new Map<number, WriterCommitRecord>();
  collectWriterCommitRecords([log(2, 900), log(3, 1000), 'INFO unrelated', log(4, 1999)].join('\n'), records);
  collectWriterCommitRecords([log(4, 1999), log(5, 2000)].join('\n'), records);
  assert.deepEqual(canonicalDeltaWindow(adds, records, 1000, 2000),
    { rows: 70, files: 2, bytes: 700, commits: 2, first_version: 3, last_version: 4 });

  const gap = new Map(records);
  gap.delete(2);
  assert.throws(() => canonicalDeltaWindow(adds, gap, 1000, 2000), /cover the measurement start/);
  gap.set(2, records.get(2)!).delete(4);
  assert.throws(() => canonicalDeltaWindow(adds, gap, 1000, 2000), /cover the measurement window/);
  assert.throws(() => collectWriterCommitRecords(log(4, 1998), new Map(records)), /conflicting/);
  assert.throws(() => collectWriterCommitRecords(log(6, 2100, 'reconciled'), new Map()), /ambiguous/);
  assert.throws(() => collectWriterCommitRecords('arrowcol_data_commit {"outcome":', new Map()), /not valid JSON/);
});

test('Delta Add reconciliation rejects missing row statistics and data-changing removals', () => {
  assert.throws(() => reconcileDeltaAdds(new Map([[1, [
    { commitInfo: { timestamp: 1000 } },
    { add: { path: 'part.parquet', size: 10, dataChange: true, partitionValues: {} } },
  ]]]), 1000, 2000), /numRecords/);
  assert.throws(() => reconcileDeltaAdds(new Map([[1, [
    { commitInfo: { timestamp: 1000 } },
    { remove: { path: 'part.parquet', dataChange: true } },
  ]]]), 1000, 2000), /append-only/);
});