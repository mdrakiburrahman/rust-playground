import assert from 'node:assert/strict';
import test from 'node:test';
import { reconcileDeltaAdds } from './aks-delta-preservation.js';

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
  assert.deepEqual(result, {
    total_rows: 32, total_files: 4, total_bytes: 320, total_commits: 4,
    window_rows: 16, window_files: 2, window_bytes: 160, window_commits: 2,
    first_version: 2, last_version: 3,
  });
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