import assert from 'node:assert/strict';
import test from 'node:test';
import { renderCampaignResults, validateCampaignMeasurements } from './campaign-results.js';

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
      factorConfigPaths: ['codec', 'level'], officialWindows: [official('one', 1)], committedRowsPerSecond: 10 },
    { cohort: 'C1', candidate: 'two', factorX: 'codec', valueX: 'two', factorY: 'level', valueY: 'default', repeat: 1,
      factorConfigPaths: ['codec', 'level'], officialWindows: [official('two', 2)], committedRowsPerSecond: 20 },
  ];
  assert.throws(() => renderCampaignResults(results, 'C1'), /incompatible frozen source\/resource\/load baselines/);
});


function officialCandidate(candidate: string, valueX: string, rates: number[], pending: number[]) {
  const expectedSha = 'a'.repeat(40);
  return [1, 2, 3].map(repeat => ({
    cohort: 'C1', candidate, factorX: 'codec', valueX, factorY: 'level', valueY: '3',
    repeat, expectedRepeats: 3, committedRowsPerSecond: rates.reduce((sum, rate) => sum + rate, 0) / rates.length,
    factorConfigPaths: ['parquet_writer.defaults.compression', 'parquet_writer.defaults.compression_level'] as [string, string],
    officialWindows: rates.map((rate, windowIndex) => {
      const sequence = repeat * 10 + windowIndex;
      const orchestration = '11111111-1111-4111-8111-' + String(sequence).padStart(12, '0');
      const cell = '22222222-2222-4222-8222-' + String(sequence).padStart(12, '0');
      const start = 1_000 + sequence * 30_000;
      const committedRows = rate * 30;
      const evidence = {
        version: 1,
        downstream_status: 'observed_or_not_attempted',
        commit_window_output_throughput: {
          scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
          clock: 'collector_system_unix_ms_shared_schedule_clock_skew_not_measured',
          warmup_backlog: 'included_when_commit_finishes_in_window',
          rewrite_commits: 'excluded_by_ingestion_only_producer_capability',
          unique_delivered_rows: null,
          complete_delivery_established: false,
          measurement_start_unix_ms: start,
          measurement_end_unix_ms: start + 30_000,
          window_seconds: 30,
          rows: committedRows,
          files: 1,
          input_bytes: committedRows * 10,
          parquet_bytes: committedRows * 5,
          commits: 1,
          rows_per_second: rate,
          bytes_per_row: 5,
          compression_ratio: 2,
          compression_ratio_numerator: 'collector_reported_input_bytes',
          compression_ratio_denominator: 'committed_parquet_file_bytes_including_metadata',
          commit_latency_seconds: { p50: 0.1, p95: 0.2, p99: 0.3, samples: 1, scope: 'commit_retry_and_sync_not_encoding_or_admission' },
        },
        reconciliation: { status: 'within_bound', lower_bound_met: true, upper_bound_met: true, exact_match: true, ambiguous_rows: 0 },
        backlog: { pending_rows: pending[windowIndex]!, pending_bytes: pending[windowIndex]! * 10,
          oldest_age_ms: pending[windowIndex]! * 1_000, retry_debt_rows: pending[windowIndex]! },
        processes: [{ component: 'collector', image: 'dataflow:source-pinned', image_id: 'sha256:' + 'b'.repeat(64),
          source_revision: expectedSha, log_complete: true, effective_config_source: 'consumer_startup_readback',
          effective_config: { commit_telemetry: { schema_version: 1, scope: 'new_ingestion_addfiles_after_sync', clock: 'system_unix_ms' } } }],
      };
      return {
        status: { id: orchestration, state: 'succeeded' as const, progress: 'published', result_count: 1, error: null },
        rows: [{ schema_version: 3, run_id: cell, mode: 'local', tunables_json: JSON.stringify({ orchestration_run_id: orchestration }),
          git_sha: expectedSha, status: 'succeeded', pinning_ok: true, table_owner: 'owner-a', rows: BigInt(committedRows),
          duration_ms: 30_000n, rows_per_second: rate, replicas: 1n, dfe_worker_concurrency: 8n,
          table: 'sqlserver_availability_replica_states', tables: 1, qap_observation_complete: 1n,
          qap_observed_tables: 1n, qap_completion_percent: 100, qap_pending_artifacts: 0n,
          qap_unhealthy_catalogs: 0n, measure_seconds: 30, evidence_json: JSON.stringify(evidence) }],
        expectedSha, mode: 'local' as const,
        config: { tables: ['sqlserver_availability_replica_states'], replica_counts: [1], dfe_worker_concurrency: [8],
          tunables: { parquet_writer: { defaults: { compression: valueX, compression_level: 3 } } } },
      };
    }),
  }));
}

test('ranks only stable, reconciled, non-growing official windows', () => {
  const stable = officialCandidate('stable', 'snappy', [100, 101, 100, 99, 100], [0, 0, 0, 0, 0]);
  const unstable = officialCandidate('unstable', 'gzip', [100, 100, 100, 100, 106], [0, 0, 0, 0, 0]);
  const growing = officialCandidate('backlog-growing', 'brotli', [100, 101, 100, 99, 100], [0, 1, 2, 3, 4]);
  const svg = renderCampaignResults([...stable, ...unstable, ...growing], 'C1');
  assert.match(svg, /stable \(3\/3\)/);
  assert.match(svg, /100 \(100–100\)/);
  assert.doesNotMatch(svg, /unstable \(3\/3\)/);
  assert.doesNotMatch(svg, /backlog-growing \(3\/3\)/);
  assert.match(svg, /unstable: unqualified, 0\/3 protocol-complete repeats/);
  assert.match(svg, /backlog-growing: unqualified, 0\/3 protocol-complete repeats/);
});
