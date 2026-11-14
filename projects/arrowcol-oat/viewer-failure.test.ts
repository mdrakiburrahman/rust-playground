import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { runInNewContext } from 'node:vm';

test('viewer keeps unknown measurements distinct from zero and unverified pinning distinct from violations', async () => {
  const html = await readFile('docs/experiments/arrowcol-oat/index.html', 'utf8');
  const start = html.indexOf('function number(');
  const end = html.indexOf('const facets =', start);
  assert.ok(start >= 0 && end > start);
  const display = runInNewContext(`${html.slice(start, end)}; ({ show, pinningLabel })`) as {
    show(value: unknown): string;
    pinningLabel(row: { status: string; pinning_ok: boolean }): string;
  };
  assert.equal(display.show(null), '\u2014');
  assert.equal(display.show(0), '0');
  assert.equal(display.pinningLabel({ status: 'failed', pinning_ok: false }), 'Unverified');
  assert.equal(display.pinningLabel({ status: 'pinning_violation', pinning_ok: false }), 'Violation');
  assert.equal(display.pinningLabel({ status: 'succeeded', pinning_ok: true }), 'OK');
  assert.equal(display.pinningLabel({ status: 'failed', pinning_ok: true }), 'OK');
  assert.ok(html.includes('const pinning = pinningLabel(row);'));
});

test('viewer derives commit-window output rather than backfilling legacy admissions or unavailable telemetry', async () => {
  const html = await readFile('docs/experiments/arrowcol-oat/index.html', 'utf8');
  const start = html.indexOf('function number(');
  const end = html.indexOf('const facets =', start);
  const display = runInNewContext(`${html.slice(start, end)}; ({ outputMetrics })`) as {
    outputMetrics(row: Record<string, unknown>): Record<string, unknown>;
  };
  const evidence = {
    version: 1, processes: [{
      component: 'collector', log_complete: true, image_id: `sha256:${'a'.repeat(64)}`,
      effective_config_source: 'consumer_startup_readback',
      effective_config: { commit_telemetry: { schema_version: 1, scope: 'new_ingestion_addfiles_after_sync', clock: 'system_unix_ms' } },
    }],
    commit_window_output_throughput: {
      scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
      rows_per_second: 20, rows: 10, files: 1, parquet_bytes: 50, bytes_per_row: 5, compression_ratio: 2,
      commit_latency_seconds: { p50: 0.1, p95: 0.2, p99: 0.3 },
    },
  };
  const row = () => ({ schema_version: 3, rows_per_second: 999, evidence_json: JSON.stringify(evidence) });
  const metrics = display.outputMetrics(row());
  assert.equal(metrics.committed_rows_per_second, 20);
  assert.equal(metrics.parquet_bytes_per_row, 5);
  assert.equal(metrics.compression_ratio, 2);
  assert.equal(metrics.commit_p95_ms, 200);
  assert.equal(display.outputMetrics({ ...row(), schema_version: 2 }).committed_rows_per_second, undefined);
  assert.equal(display.outputMetrics({ ...row(), evidence_json: '{bad' }).committed_rows_per_second, undefined);
  assert.equal(display.outputMetrics({ ...row(), evidence_json: JSON.stringify({ ...evidence, processes: [] }) }).committed_rows_per_second, undefined);
  assert.equal(display.outputMetrics({ ...row(), evidence_json: JSON.stringify({
    ...evidence, commit_window_output_throughput: { ...evidence.commit_window_output_throughput, unavailable_reason: 'incomplete' },
  }) }).committed_rows_per_second, undefined);
  Object.assign(evidence.commit_window_output_throughput, {
    rows_per_second: 0, rows: 0, files: 0, parquet_bytes: 0, bytes_per_row: null, compression_ratio: null,
    commit_latency_seconds: { p50: null, p95: null, p99: null },
  });
  assert.equal(display.outputMetrics(row()).committed_rows_per_second, 0);
  assert.equal(display.outputMetrics(row()).parquet_bytes_per_row, null);
});
