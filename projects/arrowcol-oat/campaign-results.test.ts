import assert from 'node:assert/strict';
import test from 'node:test';
import { renderCampaignResults, sortCampaignLeaderboard, validateCampaignMeasurements } from './campaign-results.js';

const rows = [
  { cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: 'default', repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 100 },
  { cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: 'default', repeat: 2, expectedRepeats: 3, committedRowsPerSecond: 120 },
  { cohort: 'C1', candidate: 'zstd', factorX: 'codec', valueX: 'zstd', factorY: 'level', valueY: '3', repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 90 },
];

test('keeps observations exploratory until official sustainable evidence is complete', () => {
  const svg = renderCampaignResults(rows, 'C1');
  assert.match(svg, /<svg[^>]+role="img"/);
  assert.match(svg, /codec × level heatmap/);
  assert.match(svg, /Sustainable leaderboard \| official evidence only/);
  assert.match(svg, /No qualified winner: complete official evidence is missing or invalid/);
  assert.match(svg, /Exploratory \/ failed observations \(not ranked\)/);
  assert.match(svg, /snappy: unqualified, 0\/3 protocol-complete repeats; observed mean 110 rows\/s/);
  assert.doesNotMatch(svg, /snappy \(0\/3\)/);
});

test('preserves Delta rate after Kustainer death and nulls only unavailable downstream metrics', () => {
  const result = {
    cohort: 'C1', candidate: 'kustainer-cap', factorX: 'codec', valueX: 'zstd', factorY: 'level', valueY: '6',
    repeat: 1, committedRowsPerSecond: 50, diedFirst: 'Kustainer', comment: 'emulator exited before the window completed',
  };
  const [validated] = validateCampaignMeasurements([result]);
  assert.equal(validated!.committedRowsPerSecond, 50);
  assert.equal(validated!.qapCompletionPercent, null);
  assert.equal(validated!.qapPendingArtifacts, null);
  assert.equal(validated!.qapUnhealthyCatalogs, null);
  assert.equal(validated!.collectorOnly, true);
  const svg = renderCampaignResults([...rows, result], 'C1');
  assert.match(svg, /Kustainer \(collector-only; downstream metrics: null; committed rows\/s: 50\).*emulator exited/);
  assert.throws(() => validateCampaignMeasurements([{ ...result, qapCompletionPercent: 100 }]), /unavailable downstream metrics to be null/);
  assert.throws(() => validateCampaignMeasurements([{ ...result, comment: null }]), /dead Kustainer requires a concrete comment/);
});

test('rejects duplicate candidate-repeat identities', () => {
  assert.throws(() => renderCampaignResults([...rows, rows[0]!], 'C1'), /duplicate candidate\/repeat identity/);
});

test('rejects incompatible frozen source, resource, or load baselines', () => {
  const official = (id: string, baseline: number) => ({
    status: { id, state: 'failed', progress: 'failed', result_count: 0, error: null },
    rows: [], expectedSha: 'same-source', mode: 'local',
    config: { tunables: { codec: id, level: 'default', frozen: baseline } },
  });
  const results = [
    { cohort: 'C1', candidate: 'one', factorX: 'codec', valueX: 'one', factorY: 'level', valueY: 'default', repeat: 1,
      factorConfigPaths: ['codec', 'level'], officialRuns: [official('one', 1)], committedRowsPerSecond: 10 },
    { cohort: 'C1', candidate: 'two', factorX: 'codec', valueX: 'two', factorY: 'level', valueY: 'default', repeat: 1,
      factorConfigPaths: ['codec', 'level'], officialRuns: [official('two', 2)], committedRowsPerSecond: 20 },
  ];
  assert.throws(() => renderCampaignResults(results, 'C1'), /incompatible frozen source\/resource\/load baselines/);
});


function sourceShapedRun(sequence: number, rates: number[], start: number, gapAt?: number) {
  const expectedSha = 'a'.repeat(40);
  const orchestration = '11111111-1111-4111-8111-' + String(sequence).padStart(12, '0');
  const cell = '22222222-2222-4222-8222-' + String(sequence).padStart(12, '0');
  const windows = rates.map((rate, index) => {
    const windowStart = start + index * 30_000 + (gapAt !== undefined && index >= gapAt ? 1_000 : 0);
    const committedRows = Math.round(rate * 30);
    return {
      scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
      clock: 'collector_system_unix_ms_shared_schedule_clock_skew_not_measured',
      warmup_backlog: 'included_when_commit_finishes_in_window',
      rewrite_commits: 'excluded_by_ingestion_only_producer_capability',
      unique_delivered_rows: null,
      complete_delivery_established: false,
      measurement_start_unix_ms: windowStart,
      measurement_end_unix_ms: windowStart + 30_000,
      window_seconds: 30,
      rows: committedRows,
      files: 1,
      input_bytes: committedRows * 10,
      parquet_bytes: committedRows * 5,
      commits: 1,
      rows_per_second: committedRows / 30,
      input_bytes_per_second: committedRows * 10 / 30,
      parquet_bytes_per_second: committedRows * 5 / 30,
      bytes_per_row: 5,
      compression_ratio: 2,
      compression_ratio_numerator: 'collector_reported_input_bytes',
      compression_ratio_denominator: 'committed_parquet_file_bytes_including_metadata',
      commit_latency_seconds: { p50: 0.1, p95: 0.2, p99: 0.3, samples: 1, scope: 'commit_retry_and_sync_not_encoding_or_admission' },
    };
  });
  const outputRows = windows.reduce((sum, window) => sum + window.rows, 0);
  const end = windows[windows.length - 1]!.measurement_end_unix_ms;
  const seconds = (end - start) / 1_000;
  const output = {
    scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
    clock: 'collector_system_unix_ms_shared_schedule_clock_skew_not_measured',
    measurement_start_unix_ms: start,
    measurement_end_unix_ms: end,
    window_seconds: seconds,
    rows: outputRows,
    files: windows.length,
    input_bytes: outputRows * 10,
    parquet_bytes: outputRows * 5,
    rows_per_second: outputRows / seconds,
    input_bytes_per_second: outputRows * 10 / seconds,
    parquet_bytes_per_second: outputRows * 5 / seconds,
    bytes_per_row: 5,
    compression_ratio: 2,
    compression_ratio_numerator: 'collector_reported_input_bytes',
    compression_ratio_denominator: 'committed_parquet_file_bytes_including_metadata',
    commits: windows.length,
    commit_latency_seconds: { p50: 0.1, p95: 0.2, p99: 0.3, samples: windows.length, scope: 'commit_retry_and_sync_not_encoding_or_admission' },
    unique_delivered_rows: null,
    complete_delivery_established: false,
    warmup_backlog: 'included_when_commit_finishes_in_window',
    rewrite_commits: 'excluded_by_ingestion_only_producer_capability',
  };
  const evidence = {
    version: 1,
    processes: [{ component: 'collector', image: 'dataflow:source-pinned', image_id: 'sha256:' + 'b'.repeat(64),
      source_revision: expectedSha, log_complete: true, effective_config_source: 'consumer_startup_readback',
      effective_config: { commit_telemetry: { schema_version: 1, scope: 'new_ingestion_addfiles_after_sync', clock: 'system_unix_ms' } } }],
    durable_commits: {
      output_throughput: output,
      complete_30_second_commit_windows: windows,
      run_cumulative_output: { scope: 'all_new_ingestion_commits_from_this_cell_including_warmup', rows: outputRows,
        commits: windows.length, complete_delivery_established: false },
    },
    reconciliation: {
      status: 'within_bound', settled: true, settle_seconds: 120, settle_timeout_seconds: 120,
      cell_scope: 'source-shaped OAT evidence', delta_rows: outputRows, completed_rows: outputRows - 1, ambiguous_rows: 1,
      gap_rows: 0, minimum_delta_rows: outputRows - 1, maximum_delta_rows: outputRows,
      lower_bound_met: true, upper_bound_met: true, exact_match_required: false, exact_match: false,
      scope: 'process_cumulative_client_rows_vs_this_cells_new_ingestion_commits_including_warmup',
    },
    commit_window_output_throughput: output,
    downstream_status: 'observed_or_not_attempted',
    backlog: null,
  };
  const status = { id: orchestration, state: 'succeeded' as const, progress: 'published', result_count: 1, error: null };
  return {
    status,
    rows: [{ schema_version: 3, run_id: cell, mode: 'local', tunables_json: JSON.stringify({ orchestration_run_id: orchestration }),
      git_sha: expectedSha, status: 'succeeded', pinning_ok: true, table_owner: 'owner-a', rows: BigInt(outputRows),
      duration_ms: BigInt(seconds * 1_000), rows_per_second: output.rows_per_second, replicas: 1n, dfe_worker_concurrency: 8n,
      table: 'sqlserver_availability_replica_states', tables: 1, qap_observation_complete: 1n,
      qap_observed_tables: 1n, qap_completion_percent: 100, qap_pending_artifacts: 0n,
      qap_unhealthy_catalogs: 0n, measure_seconds: seconds, evidence_json: JSON.stringify(evidence) }],
    expectedSha, mode: 'local' as const,
    config: { tables: ['sqlserver_availability_replica_states'], replica_counts: [1], dfe_worker_concurrency: [8],
      tunables: { parquet_writer: { defaults: { compression: 'snappy', compression_level: 3 } } } },
  };
}

test('reads one sustained run of contiguous emitter buckets but keeps missing pressure/debt proof unranked', () => {
  const result = {
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [sourceShapedRun(1, [1, 1, 1, 1, 1], 1_000_000)],
  };
  const svg = renderCampaignResults([result], 'C1');
  assert.match(svg, /snappy: unqualified, 0\/3 protocol-complete repeats; observed mean 1 rows\/s/);
  assert.match(svg, /timestamped pressure and bounded maintenance-debt evidence is unavailable from the current producer/);
  assert.doesNotMatch(svg, /snappy \(0\/3\)/);
});

test('does not combine separate cold runs or accept gapped buckets as a sustained repeat', () => {
  const common = {
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
  };
  const coldRuns = Array.from({ length: 5 }, (_, index) => sourceShapedRun(index + 1, [1], 1_000_000 + index * 60_000));
  const coldSvg = renderCampaignResults([{ ...common, officialRuns: coldRuns }], 'C1');
  assert.match(coldSvg, /each repeat must contain exactly one official sustained run/);
  const gapSvg = renderCampaignResults([{ ...common, officialRuns: [sourceShapedRun(10, [1, 1, 1, 1, 1], 1_000_000, 2)] }], 'C1');
  assert.match(gapSvg, /official commit buckets must be positive, complete, contiguous 30-second windows/);
});

test('requires actual distinct factor leaves and preserves frozen config paths', () => {
  const run = sourceShapedRun(20, [1, 1, 1, 1, 1], 1_000_000);
  const result = {
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [{ ...run, config: { ...run.config, tunables: { ...run.config.tunables,
      parquet_writer: { defaults: { compression: 'snappy', compression_level: 3 } } } } }],
  };
  assert.throws(() => renderCampaignResults([result], 'C1'), /distinct scalar or array config leaves/);
});

test('sorts qualified leaderboard metrics by descending mean then candidate name', () => {
  const items = [
    { candidate: 'zeta', mean: 120 },
    { candidate: 'tie-z', mean: 100 },
    { candidate: 'alpha', mean: 100 },
    { candidate: 'beta', mean: 140 },
  ];
  assert.deepEqual(sortCampaignLeaderboard(items).map(item => item.candidate), ['beta', 'zeta', 'alpha', 'tie-z']);
});
