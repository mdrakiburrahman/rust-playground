import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { clientMetrics, difference, parseProcStat, snapshot } from './measure.js';

test('counts newly visible commits even when their commitInfo timestamp precedes the window', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bwa-stats-'));
  try {
    const log = path.join(root, 'table', '_delta_log');
    fs.mkdirSync(log, { recursive: true });
    fs.writeFileSync(path.join(log, '00000000000000000000.json'), '{"commitInfo":{"timestamp":1}}\n');
    fs.writeFileSync(path.join(log, '00000000000000000000.checkpoint.parquet'), '');
    const start = snapshot(root);
    start.observedAt = 1000;
    assert.equal(difference(start, { ...start, observedAt: 2000 }).checkpoint_files_added, 0);
    fs.writeFileSync(path.join(log, '00000000000000000001.json'),
      '{"commitInfo":{"timestamp":1}}\n{"add":{"size":400,"stats":"{\\"numRecords\\":20}"}}\n');
    fs.writeFileSync(path.join(log, '00000000000000000001.checkpoint.parquet'), '');
    fs.mkdirSync(path.join(log, '_sidecars'));
    fs.writeFileSync(path.join(log, '_sidecars', 'test.parquet'), 'sidecar');
    const end = snapshot(root);
    end.observedAt = 3000;
    const stats = difference(start, end);
    assert.equal(stats.rows, 20);
    assert.equal(stats.committed_rps, 10);
    assert.equal(stats.bytes_per_row, 20);
    assert.equal(stats.commits, 1);
    assert.equal(stats.checkpoints, 2);
    assert.equal(stats.checkpoint_files_added, 1);
    assert.deepEqual(stats.table_logs_end.table, { version: 1, bytes: 7 +
      fs.statSync(path.join(log, '00000000000000000000.json')).size +
      fs.statSync(path.join(log, '00000000000000000001.json')).size });
    fs.writeFileSync(path.join(log, '00000000000000000002.json'), '{"add":{"size":20}}\n');
    assert.throws(() => snapshot(root), /statistics/);
  } finally { fs.rmSync(root, { recursive: true }); }
});

test('client error fields do not count as error log lines', () => {
  const metrics = { measurement_window: { completed_rows: 10, completed_flight_data_bytes_uncompressed: 100,
    start_unix_ms: 1000, end_unix_ms: 2000, ambiguous_rows: 0, given_up_rows: 0, rejected_rows: 0, retried_rows: 0 }, error_rows: 0 };
  const line = `OAT_CLIENT_METRICS ${JSON.stringify(metrics)}\n`;
  assert.equal(clientMetrics(line).error_logs, 0);
  assert.equal(clientMetrics(`Error: rejected\n${line}`).error_logs, 1);
  assert.equal(clientMetrics(line).offered_rows, null);
  assert.throws(() => clientMetrics(`OAT_CLIENT_METRICS ${JSON.stringify({ measurement_window: { completed_rows: 10 } })}`), /Missing client counter/);
  assert.throws(() => clientMetrics('unexpected EOF'), /did not emit/);
});

test('process stat handles spaces and parentheses in process names', () => {
  const fields = Array.from({ length: 22 }, (_, i) => String(i + 3));
  fields[0] = 'R';
  const stat = parseProcStat(`42 (collector (test)) ${fields.join(' ')}`);
  assert.equal(stat.minorFaults, 10);
  assert.equal(stat.majorFaults, 12);
  assert.equal(stat.userTicks, 14);
  assert.equal(stat.systemTicks, 15);
});
