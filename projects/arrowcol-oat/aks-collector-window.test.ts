import assert from 'node:assert/strict';
import test from 'node:test';
import { reconcileCollectorWindow } from './aks-collector-window.js';

const commits = [
  { version: 1, commit_info_timestamp_unix_ms: 999, rows: 10, files: 1, bytes: 100 },
  { version: 2, commit_info_timestamp_unix_ms: 1000, rows: 20, files: 2, bytes: 200 },
  { version: 3, commit_info_timestamp_unix_ms: 1999, rows: 30, files: 3, bytes: 300 },
  { version: 4, commit_info_timestamp_unix_ms: 2000, rows: 40, files: 4, bytes: 400 },
];
function receipt(version: number, finished: number) {
  const c = commits.find(commit => commit.version === version)!;
  return 'arrowcol_data_commit ' + JSON.stringify({ schema_version: 1, outcome: 'committed', table_id: 'table',
    delta_version: version, finished_at_unix_ms: finished, rows: c.rows, files: c.files, parquet_bytes: c.bytes });
}

test('completion clock independently joins Adds and preserves both half-open boundary version sets', () => {
  const logs = [receipt(1, 1000), receipt(2, 1500), receipt(3, 2000), receipt(4, 2100), receipt(2, 1500)].join('\n');
  const result = reconcileCollectorWindow(logs, 'table', commits, 1000, 2000, 'pod');
  assert.equal(result.completed_receipts, 2);
  assert.equal(result.joined_rows, 30);
  assert.deepEqual(result.receipt_window_versions, [1, 2]);
  assert.deepEqual(result.delta_window_versions, [2, 3]);
  assert.deepEqual(result.receipt_only_versions, [1]);
  assert.deepEqual(result.delta_only_versions, [3]);
  assert.equal(result.joined_receipts?.[0]?.commit_info_timestamp_unix_ms, 999);
});

test('receipt join rejects missing versions, wrong table and conflicting duplicates; absence is not zero', () => {
  assert.throws(() => reconcileCollectorWindow(receipt(1, 1500), 'other', commits, 1000, 2000, 'pod'), /unexpected table/);
  assert.throws(() => reconcileCollectorWindow(receipt(1, 1500), 'table', [], 1000, 2000, 'pod'), /join exactly/);
  assert.throws(() => reconcileCollectorWindow(receipt(1, 1500) + '\n' + receipt(1, 1600), 'table', commits, 1000, 2000, 'pod'), /Conflicting/);
  assert.equal(reconcileCollectorWindow('', 'table', commits, 1000, 2000, 'pod').joined_rows, null);
  assert.equal(reconcileCollectorWindow('', undefined, undefined, 1000, 2000, 'pod').completed_receipts, null);
});
