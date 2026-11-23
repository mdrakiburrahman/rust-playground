import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { maintenanceDueWorkIssue, publicationBacklogIssue, renderCampaignResults, sortCampaignLeaderboard, validateCampaignMeasurements } from './campaign-results.js';
import { buildThroughputManifests } from './throughput-sweeps.js';

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
  assert.match(svg, /snappy: unqualified, 0\/3 protocol-complete repeats; no committed-rate observation \(varied factor coverage unknown/);
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
  assert.match(svg, /Kustainer \(collector-only; downstream metrics: null; committed rows\/s: null\).*emulator exited/);
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
    inert_fields: [],
    projection_only: [
      { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' },
      { path: 'parquet_writer.defaults.compression_level', reason: 'consumer_startup_proof_unverified' },
    ],
    runner_controls: [],
    consumer_verified: [],
    overload_guard: {
      status: 'complete', run_id: cell, run_start_ms: start - 30_000, measurement_end_ms: end,
      requested_container_memory_limit_bytes: null,
      settings: { enabled: true, cap_fraction: 0.9, zero_progress_windows: 2 },
      coverage_start_ms: start - 30_000, coverage_end_ms: end, max_sampling_gap_ms: 1000,
      sample_count: seconds + 31, peak_cap_fraction: 0.1,
      last_caps: ['container_memory_bytes', 'collector_rss_bytes', 'collector_buffered_bytes']
        .map(metric => ({ metric, observed_value: 100, cap: 1000, confirmed: true })),
      complete_windows: Array.from({ length: Math.floor((end - start + 30_000) / 30_000) }, (_, index) => ({
        index, start_ms: start - 30_000 + index * 30_000, end_ms: start + index * 30_000,
        ingestion_commits: index === 0 ? 0 : 1, outstanding: index !== 0, covered: true,
      })),
    },
    overload_stop: null,
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
    config: { warmup_seconds: 30, tables: ['sqlserver_availability_replica_states'], replica_counts: [1], dfe_worker_concurrency: [8],
      tunables: { parquet_writer: { defaults: { compression: 'snappy', compression_level: 3 } },
        maintenance: { enabled: true, apply: true, operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'] } } },
  };
}

const maintenanceOperations = ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'] as const;

function attachMaintenanceDueWork(
  run: ReturnType<typeof sourceShapedRun>,
  start: number,
  end: number,
  dropped = 0,
): void {
  const row = run.rows[0]!;
  const evidence = JSON.parse(String(row.evidence_json)) as Record<string, unknown>;
  if (!Array.isArray(evidence.processes)) throw new Error('source fixture has no process evidence');
  evidence.processes = [...evidence.processes, {
    component: 'maintenance',
    log_complete: true,
    maintenance_operation_outcomes: {
      status: 'observed', cell_scope: 'stdout_stderr_append_offsets', operation_count: 25,
      operation_failure: false, error_kind_counts: {}, retry_kind_counts: {},
      operations: Object.fromEntries(maintenanceOperations.map(operation => [operation,
        { operation_count: 5, retry_kind_counts: {}, error_kind_counts: {} }])),
    },
    maintenance_table_policies: {
      source: 'validated_consumer_per_table_policy',
      tables: { 'telemetry.sqlserver.sqlserver_availability_replica_states': { active: true, table_policy: {} } },
      operation_reports_dropped: dropped,
      operation_report_timing_scope: 'process_lifetime_including_warmup_and_drain_not_shared_measurement_window',
      operation_outcomes: { scope: 'process_lifetime', operation_count: 25, error_count: 0 },
      operation_reports: Array.from({ length: Math.floor((end - start) / 30_000) }, (_, window) =>
        maintenanceOperations.map(operation => {
          const unit = operation === 'optimize' || operation === 'purge' ? 'parquet_files' :
            operation === 'vacuum' ? 'tombstones' : operation === 'checkpoint' ? 'commits' : 'log_objects';
          const source = operation === 'optimize' || operation === 'purge' ? 'snapshot_active_files' :
            operation === 'vacuum' ? 'bounded_tombstone_replay' :
              operation === 'checkpoint' ? 'checkpoint_log_inventory' : 'verified_prune_proof';
          const hasBytes = operation === 'optimize' || operation === 'purge';
          return {
            table: 'telemetry.sqlserver.sqlserver_availability_replica_states',
            operation,
            operation_failed: false,
            interrupted: false,
            report_outcome: 'noop',
            compaction_debt: operation === 'optimize' ? {
              observed_at_ms: start + window * 30_000 + 1_000, source,
              protected_partition_backlog: { files: 0, bytes: 0, oldest_partition_start_ms: null },
            } : null,
            maintenance_due_work: {
              observed_at_ms: start + window * 30_000 + 1_000,
              source,
              completeness: 'complete',
              unit,
              due: false,
              eligible_count: 0,
              eligible_bytes: hasBytes ? 0 : null,
              backlog_count: 0,
              backlog_bytes: hasBytes ? 0 : null,
              oldest_eligible_timestamp_ms: null,
              oldest_timestamp_basis: null,
              incomplete_reason: null,
            },
          };
        })).flat(),
    },
  }];
  evidence.shutdown_drain = { maintenance: [{
    exit_code: 0, exit_classification: 'no_operation_failures',
    outcomes: {
      status: 'observed', cell_scope: 'stdout_stderr_append_offsets', operation_count: 25,
      operation_failure: false, error_kind_counts: {}, retry_kind_counts: {},
      operations: Object.fromEntries(maintenanceOperations.map(operation => [operation,
        { operation_count: 5, retry_kind_counts: {}, error_kind_counts: {} }])),
    },
  }] };
  row.evidence_json = JSON.stringify(evidence);
}

function mutateMaintenanceReport(
  run: ReturnType<typeof sourceShapedRun>,
  operation: string,
  window: number,
  start: number,
  mutate: (report: Record<string, unknown>, dueWork: Record<string, unknown>) => void,
): void {
  const row = run.rows[0]!;
  const evidence = JSON.parse(String(row.evidence_json)) as {
    processes: Array<{ component: string; maintenance_table_policies?: { operation_reports: Array<Record<string, unknown>> } }>;
  };
  const maintenance = evidence.processes.find(process => process.component === 'maintenance');
  const report = maintenance?.maintenance_table_policies?.operation_reports.find(candidate =>
    candidate.operation === operation && isMaintenanceWindow(candidate, window, start));
  if (!report) throw new Error('source fixture has no maintenance operation report');
  const dueWork = report.maintenance_due_work;
  if (!dueWork || typeof dueWork !== 'object' || Array.isArray(dueWork)) throw new Error('source fixture has no due-work evidence');
  mutate(report, dueWork as Record<string, unknown>);
  row.evidence_json = JSON.stringify(evidence);
}

function isMaintenanceWindow(report: Record<string, unknown>, window: number, start: number): boolean {
  const dueWork = report.maintenance_due_work;
  return dueWork !== null && typeof dueWork === 'object' && !Array.isArray(dueWork) &&
    (dueWork as Record<string, unknown>).observed_at_ms === start + window * 30_000 + 1_000;
}

function maintenanceConfig(run: ReturnType<typeof sourceShapedRun>): Record<string, unknown> {
  return (run.config.tunables as Record<string, unknown>).maintenance as Record<string, unknown>;
}

// Synthetic agreed-contract data, not a serialized producer fixture or benchmark.
function syntheticBacklog() {
  return {
    schema_version: 1,
    source: 'arrowcol_publication_backlog_*',
    scope: 'unique_table_owner_admissions_not_confirmed_durable_commit',
    timestamp_basis: 'UTC request start/end around direct /metrics GET; each is a scrape-time snapshot',
    measurement_window: { start_unix_ms: 1_000_000, end_unix_ms: 1_150_000 },
    sampling_interval_ms: 1_000,
    request_timeout_ms: 3_000,
    sampling_failures: 0,
    samples_dropped: 0,
    last_unavailable_reason: null as string | null,
    samples: Array.from({ length: 150 }, (_, index) => ({
      request_start_unix_ms: 1_000_000 + index * 1_000,
      request_end_unix_ms: 1_000_010 + index * 1_000,
      request_timeout_ms: Math.min(3_000, 150_000 - index * 1_000),
      available: true as boolean | null,
      publications: 0 as number | null,
      rows: 0 as number | null,
      logical_bytes: 0 as number | null,
      oldest_age_seconds: 0 as number | null,
      status: null as string | null,
      unavailable_reason: null as string | null,
    })),
  };
}

test('parses synthetic backlog and producer-shaped maintenance evidence without claiming measured results', () => {
  assert.equal(publicationBacklogIssue(syntheticBacklog(), 1_000_000, 1_150_000), undefined);
  const run = sourceShapedRun(40, [1, 1, 1, 1, 1], 1_000_000);
  attachMaintenanceDueWork(run, 1_000_000, 1_150_000);
  mutateMaintenanceReport(run, 'optimize', 0, 1_000_000, (report, dueWork) => {
    report.negative_probe = true;
    dueWork.source = 'bounded_physical_inventory';
    (report.compaction_debt as Record<string, unknown>).source = dueWork.source;
  });
  assert.equal(maintenanceDueWorkIssue(
    JSON.parse(String(run.rows[0]!.evidence_json)).processes,
    maintenanceConfig(run),
    1_000_000,
    1_150_000,
  ), undefined);
  mutateMaintenanceReport(run, 'optimize', 0, 1_000_000, (_report, dueWork) => {
    dueWork.source = 'checkpoint_log_inventory';
  });
  assert.match(maintenanceDueWorkIssue(
    JSON.parse(String(run.rows[0]!.evidence_json)).processes,
    maintenanceConfig(run),
    1_000_000,
    1_150_000,
  )!, /unavailable or invalid/);

  const row = run.rows[0]!;
  const evidence = JSON.parse(String(row.evidence_json)) as Record<string, unknown>;
  evidence.backlog = syntheticBacklog();
  row.evidence_json = JSON.stringify(evidence);
  const result = {
    cohort: 'C1', candidate: 'synthetic-only', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [run],
  };
  const svg = renderCampaignResults([result], 'C1');
  assert.match(svg, /No qualified winner/);
  assert.match(svg, /five-operation maintenance due-work evidence is unavailable or invalid/);
});

test('consumes actual Rust results through the E1 projection and keeps single-window or failed wrappers UNKNOWN', () => {
  const healthProjection = JSON.parse(readFileSync(
    new URL('./campaign-results-maintenance-health-e1-projection.json', import.meta.url), 'utf8',
  )) as { producer_commit: string; producer_sha256: string; processes: Array<{
    maintenance_table_policies: { operation_reports: Array<{
      operation: string; compaction_debt: Record<string, unknown> | null;
    }> };
  }> };
  const healthProducer = JSON.parse(readFileSync(
    new URL('../arrowcol-oat-rust/tests/fixtures/maintenance-operation-results.json', import.meta.url), 'utf8',
  )) as { operation_results: Array<{ operation: string; report: { compaction_debt: Record<string, unknown> | null } }> };
  assert.equal(healthProjection.producer_commit, 'dbdfbf2b3c0dcaae2db19104b0796933d7fea520');
  const producerDebt = healthProducer.operation_results.find(result => result.operation === 'optimize')!.report.compaction_debt!;
  const projectedDebt = healthProjection.processes[0]!.maintenance_table_policies.operation_reports
    .find(result => result.operation === 'optimize')!.compaction_debt!;
  assert.deepEqual(projectedDebt.protected_partition_backlog,
    { files: 2, bytes: 932, oldest_partition_start_ms: 1_704_067_260_000 });
  assert.deepEqual(projectedDebt.protected_partition_backlog, producerDebt.protected_partition_backlog);
  assert.equal(maintenanceDueWorkIssue(healthProjection.processes, {
    enabled: true, apply: true, operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'],
  }, 1_704_067_380_000, 1_704_067_530_000), 'maintenance due-work is missing from one or more complete 30-second windows');
  const producer = JSON.parse(readFileSync(
    new URL('./campaign-results-maintenance-rust.json', import.meta.url), 'utf8',
  )) as {
    operation_results: Record<string, unknown>[];
  };
  const projection = JSON.parse(readFileSync(
    new URL('./campaign-results-maintenance-e1-projection.json', import.meta.url), 'utf8',
  )) as {
    idle_processes: Array<Record<string, unknown>>;
    qualification_cases: Array<{ case: string; processes: Array<Record<string, unknown>> }>;
  };
  assert.deepEqual(producer.operation_results.map(result => result.operation),
    ['purge', 'optimize', 'vacuum', 'checkpoint', 'prune_log']);
  const idlePolicy = projection.idle_processes[0]!.maintenance_table_policies as Record<string, unknown>;
  const idleReports = idlePolicy.operation_reports as Array<Record<string, unknown>>;
  assert.deepEqual(idleReports.map(report => report.operation), producer.operation_results.map(result => result.operation));
  assert.deepEqual(Object.keys(idlePolicy.tables as Record<string, unknown>),
    ['telemetry.sqlserver.sqlserver_availability_replica_states']);
  const start = 1_704_067_380_000;
  const config = {
    enabled: true,
    apply: true,
    operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'],
  };
  const idleIssue = maintenanceDueWorkIssue(
    projection.idle_processes,
    config,
    start,
    start + 150_000,
  );
  assert.equal(idleIssue, 'maintenance due-work is missing from one or more complete 30-second windows');

  for (const scenario of projection.qualification_cases) {
    const policy = scenario.processes[0]!.maintenance_table_policies as Record<string, unknown>;
    const reports = policy.operation_reports as Array<Record<string, unknown>>;
    const issue = maintenanceDueWorkIssue(scenario.processes, config, start, start + 150_000);
    assert.equal(typeof issue, 'string', scenario.case + ' must not qualify with retained single-pass evidence: ' + issue);
    if (scenario.case === 'complete') {
      assert.notEqual(reports[0]!.maintenance_due_work, null);
      assert.equal(issue, 'maintenance due-work is missing from one or more complete 30-second windows');
    } else if (scenario.case === 'error') {
      assert.equal(policy.operation_outcomes && (policy.operation_outcomes as Record<string, unknown>).error_count, 1);
      assert.equal(reports[0]!.maintenance_due_work, null);
      assert.equal(issue, 'complete five-operation maintenance due-work evidence is unavailable or invalid');
    } else {
      assert.equal(reports[0]!.maintenance_due_work, null);
      if (scenario.case === 'interrupted' || scenario.case === 'missing_report') {
        assert.equal(issue, 'complete five-operation maintenance due-work evidence is unavailable or invalid');
      } else {
        assert.equal(issue, 'maintenance operation failed or its due-work observation is incomplete');
      }
    }
  }
});

test('accepts actual Rust due/backlog samples but keeps the campaign unqualified and incomplete coverage UNKNOWN', () => {
  const fixture = JSON.parse(readFileSync(
    new URL('./campaign-results-maintenance-e1-projection.json', import.meta.url), 'utf8',
  )) as {
    due_backlog_processes: Array<Record<string, unknown>>;
    incomplete_processes: Array<Record<string, unknown>>;
  };
  const producer = JSON.parse(readFileSync(
    new URL('./campaign-results-maintenance-rust-debt.json', import.meta.url), 'utf8',
  )) as {
    due_backlog_results: Array<Record<string, unknown>>;
    incomplete_results: Array<Record<string, unknown>>;
  };
  const start = 1_704_067_380_000;
  const end = start + 150_000;
  const config = {
    enabled: true,
    apply: true,
    operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'],
  };
  const duePolicy = fixture.due_backlog_processes[0]!.maintenance_table_policies as Record<string, unknown>;
  const dueReports = duePolicy.operation_reports as Array<Record<string, unknown>>;
  const dueWork = dueReports[0]!.maintenance_due_work as Record<string, unknown>;
  const producerDueReport = producer.due_backlog_results[0]!.report as Record<string, unknown>;
  const producerDueWork = producerDueReport.maintenance_due_work as Record<string, unknown>;
  assert.equal(dueReports[0]!.operation, 'purge');
  assert.equal(dueWork.observed_at_ms, producerDueWork.observed_at_ms);
  assert.equal(dueWork.completeness, 'complete');
  assert.equal(dueWork.due, true);
  assert.equal(dueWork.eligible_count, 4);
  assert.equal(dueWork.backlog_count, 2);
  assert.equal(maintenanceDueWorkIssue(
    fixture.due_backlog_processes, config, start, end,
  ), 'maintenance due-work is missing from one or more complete 30-second windows');

  const incompletePolicy = fixture.incomplete_processes[0]!.maintenance_table_policies as Record<string, unknown>;
  const incompleteReports = incompletePolicy.operation_reports as Array<Record<string, unknown>>;
  const incompleteWork = incompleteReports[0]!.maintenance_due_work as Record<string, unknown>;
  const producerIncompleteReport = producer.incomplete_results[0]!.report as Record<string, unknown>;
  const producerIncompleteWork = producerIncompleteReport.maintenance_due_work as Record<string, unknown>;
  assert.equal(incompleteWork.completeness, 'incomplete');
  assert.equal(incompleteWork.incomplete_reason, 'malformed_partition');
  assert.equal(incompleteWork.eligible_count, null);
  assert.equal(incompleteWork.observed_at_ms, producerIncompleteWork.observed_at_ms);
  const incompleteIssue = maintenanceDueWorkIssue(
    fixture.incomplete_processes, config, start, end,
  );
  assert.equal(incompleteIssue, 'maintenance operation failed or its due-work observation is incomplete');
});

test('rejects actual projected interrupted and missing-report records mixed with complete windows', () => {
  const projection = JSON.parse(readFileSync(
    new URL('./campaign-results-maintenance-e1-projection.json', import.meta.url), 'utf8',
  )) as {
    qualification_cases: Array<{ case: string; processes: Array<Record<string, unknown>> }>;
  };
  const start = 1_000_000;
  const end = start + 150_000;
  for (const caseName of ['interrupted', 'missing_report']) {
    const scenario = projection.qualification_cases.find(candidate => candidate.case === caseName)!;
    const projectedPolicy = scenario.processes[0]!.maintenance_table_policies as Record<string, unknown>;
    const projectedReports = projectedPolicy.operation_reports as Array<Record<string, unknown>>;
    const projectedReport = projectedReports[0]!;
    assert.equal(projectedReport.operation_failed, false);
    assert.equal(projectedReport.maintenance_due_work, null);

    const run = sourceShapedRun(60 + (caseName === 'interrupted' ? 0 : 1), [1, 1, 1, 1, 1], start);
    attachMaintenanceDueWork(run, start, end);
    const evidence = JSON.parse(String(run.rows[0]!.evidence_json)) as {
      processes: Array<{ component: string; maintenance_table_policies: Record<string, unknown> }>;
    };
    const maintenance = evidence.processes.find(process => process.component === 'maintenance')!;
    const policy = maintenance.maintenance_table_policies;
    const operationReports = policy.operation_reports as Array<Record<string, unknown>>;
    operationReports.push(projectedReport);
    const outcomes = policy.operation_outcomes as Record<string, unknown>;
    outcomes.operation_count = Number(outcomes.operation_count) + 1;
    run.rows[0]!.evidence_json = JSON.stringify(evidence);

    assert.equal(
      maintenanceDueWorkIssue(evidence.processes, maintenanceConfig(run), start, end),
      'complete five-operation maintenance due-work evidence is unavailable or invalid',
      caseName + ' must not be skipped beside otherwise complete five-window evidence',
    );
  }
});

test('rejects unknown, unavailable, dropped, gapped, or clock-reversed backlog contract samples', () => {
  const mutations: Array<(value: ReturnType<typeof syntheticBacklog>) => void> = [
    value => { value.scope = 'entire_client_backlog'; },
    value => { value.measurement_window.start_unix_ms++; },
    value => { value.sampling_failures = 1; },
    value => { value.samples_dropped = 1; },
    value => { value.last_unavailable_reason = 'scrape failed'; },
    value => { value.samples[0]!.available = false; },
    value => { value.samples[0]!.available = null; },
    value => { value.samples[0]!.rows = null; },
    value => { value.samples[0]!.rows = Number.NaN; },
    value => { value.samples[0]!.rows = Number.MAX_SAFE_INTEGER + 1; },
    value => { value.samples[0]!.oldest_age_seconds = Number.POSITIVE_INFINITY; },
    value => { value.samples[0]!.unavailable_reason = 'unknown gauges'; },
    value => { value.samples[0]!.request_end_unix_ms = value.samples[0]!.request_start_unix_ms - 1; },
    value => { value.samples[1]!.request_start_unix_ms = value.samples[0]!.request_start_unix_ms; },
    value => { value.samples.splice(10, 5); },
    value => { value.samples.splice(0, 4); },
    value => { value.samples.splice(-4); },
    value => { value.samples = value.samples.filter((_, index) => index % 30 === 0); },
    value => { value.request_timeout_ms = 30_001; },
    value => { value.samples[0]!.request_timeout_ms = 0; },
    value => { value.samples[0]!.request_timeout_ms = 3_001; },
    value => { value.samples[0]!.request_timeout_ms = 5; },
    value => { value.samples[value.samples.length - 1]!.request_timeout_ms = 3_000; },
    value => { value.samples = Array.from({ length: 10_001 }, () => value.samples[0]!); },
  ];
  mutations.forEach((mutate, index) => {
    const value = syntheticBacklog();
    mutate(value);
    assert.equal(typeof publicationBacklogIssue(value, 1_000_000, 1_150_000), 'string', 'mutation ' + index);
  });
  assert.equal(typeof publicationBacklogIssue(null, 1_000_000, 1_150_000), 'string');
});

test('allows single-flight scrape timeouts longer than cadence with skipped ticks and shrinking end budgets', () => {
  const value = syntheticBacklog();
  value.samples = value.samples.filter((_, index) => index % 3 === 0);
  value.samples.forEach(sample => { sample.request_end_unix_ms = sample.request_start_unix_ms + 1_500; });
  assert.equal(publicationBacklogIssue(value, 1_000_000, 1_150_000), undefined);
  const endBudget = syntheticBacklog();
  assert.equal(endBudget.samples[endBudget.samples.length - 1]!.request_timeout_ms, 1_000);
  assert.equal(publicationBacklogIssue(endBudget, 1_000_000, 1_150_000), undefined);
});

test('rejects sustained positive publication rows, bytes, publications, or age trends without zero filling', () => {
  for (const field of ['publications', 'rows', 'logical_bytes', 'oldest_age_seconds'] as const) {
    const value = syntheticBacklog();
    value.samples.forEach((sample, index) => { sample[field] = index; });
    assert.match(publicationBacklogIssue(value, 1_000_000, 1_150_000)!, /positive pending/);
  }
});

test('requires complete, contiguous five-window due-work for all operations and rejects growth or unknowns', () => {
  const valid = sourceShapedRun(30, [1, 1, 1, 1, 1], 1_000_000);
  attachMaintenanceDueWork(valid, 1_000_000, 1_150_000);
  const validEvidence = JSON.parse(String(valid.rows[0]!.evidence_json));
  assert.equal(maintenanceDueWorkIssue(validEvidence.processes, maintenanceConfig(valid), 1_000_000, 1_150_000), undefined);

  const mutations: Array<(run: ReturnType<typeof sourceShapedRun>) => void> = [
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000, 1); },
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000); mutateMaintenanceReport(run, 'purge', 2, 1_000_000,
      (_report, dueWork) => { dueWork.completeness = 'incomplete'; dueWork.incomplete_reason = 'active_file_limit'; }); },
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000); mutateMaintenanceReport(run, 'vacuum', 1, 1_000_000,
      (_report, dueWork) => { dueWork.observed_at_ms = 1_150_000; }); },
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000); mutateMaintenanceReport(run, 'checkpoint', 3, 1_000_000,
      (_report, dueWork) => { dueWork.backlog_count = 1; dueWork.eligible_count = 0; }); },
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000); mutateMaintenanceReport(run, 'optimize', 0, 1_000_000,
      (_report, dueWork) => { dueWork.backlog_count = 0; dueWork.eligible_count = 0; dueWork.due = true; }); },
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000); mutateMaintenanceReport(run, 'prune_log', 4, 1_000_000,
      (report, dueWork) => { report.operation_failed = true; dueWork.due = false; }); },
    run => { attachMaintenanceDueWork(run, 1_000_000, 1_150_000); mutateMaintenanceReport(run, 'purge', 0, 1_000_000,
      (_report, dueWork) => { dueWork.unit = 'log_objects'; }); },
    run => {
      attachMaintenanceDueWork(run, 1_000_000, 1_150_000);
      const evidence = JSON.parse(String(run.rows[0]!.evidence_json));
      const maintenance = evidence.processes.find((process: Record<string, unknown>) => process.component === 'maintenance');
      maintenance.maintenance_table_policies.operation_outcomes.error_count = 1;
      run.rows[0]!.evidence_json = JSON.stringify(evidence);
    },
    run => {
      attachMaintenanceDueWork(run, 1_000_000, 1_150_000);
      const evidence = JSON.parse(String(run.rows[0]!.evidence_json));
      const maintenance = evidence.processes.find((process: Record<string, unknown>) => process.component === 'maintenance');
      maintenance.maintenance_table_policies.operation_reports =
        maintenance.maintenance_table_policies.operation_reports.filter((report: Record<string, unknown>) => report.operation !== 'purge');
      run.rows[0]!.evidence_json = JSON.stringify(evidence);
    },
  ];
  mutations.forEach((mutate, index) => {
    const run = sourceShapedRun(31 + index, [1, 1, 1, 1, 1], 1_000_000);
    mutate(run);
    const evidence = JSON.parse(String(run.rows[0]!.evidence_json));
    assert.equal(typeof maintenanceDueWorkIssue(evidence.processes, maintenanceConfig(run), 1_000_000, 1_150_000), 'string',
      'invalid evidence mutation ' + index);
  });

  const growing = sourceShapedRun(50, [1, 1, 1, 1, 1], 1_000_000);
  attachMaintenanceDueWork(growing, 1_000_000, 1_150_000);
  for (let window = 0; window < 5; window++) {
    mutateMaintenanceReport(growing, 'optimize', window, 1_000_000, (_report, dueWork) => {
      dueWork.eligible_count = 10;
      dueWork.backlog_count = window + 1;
      dueWork.eligible_bytes = 1_000;
      dueWork.backlog_bytes = (window + 1) * 100;
      dueWork.due = true;
    });
  }
  const growingEvidence = JSON.parse(String(growing.rows[0]!.evidence_json));
  assert.match(maintenanceDueWorkIssue(growingEvidence.processes, maintenanceConfig(growing), 1_000_000, 1_150_000)!,
    /optimize maintenance due-work grows/);

  const eligibleGrowing = sourceShapedRun(51, [1, 1, 1, 1, 1], 1_000_000);
  attachMaintenanceDueWork(eligibleGrowing, 1_000_000, 1_150_000);
  for (let window = 0; window < 5; window++) {
    mutateMaintenanceReport(eligibleGrowing, 'optimize', window, 1_000_000, (_report, dueWork) => {
      dueWork.eligible_count = window + 1;
      dueWork.backlog_count = 0;
      dueWork.eligible_bytes = (window + 1) * 100;
      dueWork.backlog_bytes = 0;
      dueWork.due = true;
    });
  }
  const eligibleEvidence = JSON.parse(String(eligibleGrowing.rows[0]!.evidence_json));
  assert.match(maintenanceDueWorkIssue(eligibleEvidence.processes, maintenanceConfig(eligibleGrowing), 1_000_000, 1_150_000)!,
    /optimize maintenance due-work grows/);
});

test('preserves retries without failures but refuses missing records or growing protected debt', () => {
  const run = sourceShapedRun(91, [1, 1, 1, 1, 1], 1_000_000);
  attachMaintenanceDueWork(run, 1_000_000, 1_150_000);
  const evidence = JSON.parse(String(run.rows[0]!.evidence_json));
  const process = evidence.processes.find((item: Record<string, unknown>) => item.component === 'maintenance');
  const policy = process.maintenance_table_policies;
  policy.operation_outcomes.operation_count = 26;
  policy.operation_outcomes.retry_count = 1;
  policy.operation_outcomes.retry_kind_counts = { stale_plan: 1 };
  policy.operation_reports.push({
    table: 'telemetry.sqlserver.sqlserver_availability_replica_states', operation: 'vacuum',
    operation_failed: false, error_kind: null, operation_retry: true, retry_kind: 'stale_plan',
    maintenance_due_work: null, compaction_debt: null, report_outcome: null,
  });
  process.maintenance_operation_outcomes.operation_count = 26;
  process.maintenance_operation_outcomes.retry_kind_counts = { stale_plan: 1 };
  process.maintenance_operation_outcomes.operations.vacuum.operation_count = 6;
  process.maintenance_operation_outcomes.operations.vacuum.retry_kind_counts = { stale_plan: 1 };
  assert.equal(maintenanceDueWorkIssue(evidence.processes, maintenanceConfig(run), 1_000_000, 1_150_000), undefined);
  evidence.backlog = syntheticBacklog();
  evidence.shutdown_drain.maintenance[0] = {
    exit_code: 0, exit_classification: 'no_operation_failures', outcomes: process.maintenance_operation_outcomes,
  };
  const result = () => ({
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [{ ...run, rows: [{ ...run.rows[0]!, evidence_json: JSON.stringify(evidence) }] }],
  });
  assert.match(renderCampaignResults([result()], 'C1'), /1\/3 protocol-complete repeats/);
  for (const classification of ['classified_retries_only', 'nonzero_health_exit', 'unhealthy', 'no_operation_failures']) {
    evidence.shutdown_drain.maintenance[0].exit_code = 1;
    evidence.shutdown_drain.maintenance[0].exit_classification = classification;
    assert.match(renderCampaignResults([result()], 'C1'), /maintenance shutdown exit has no proved classified cause/);
  }
  process.maintenance_operation_outcomes.operation_failure = null;
  assert.match(maintenanceDueWorkIssue(evidence.processes, maintenanceConfig(run), 1_000_000, 1_150_000)!, /records are unavailable/);
  process.maintenance_operation_outcomes.operation_failure = false;
  for (const [index, report] of policy.operation_reports.filter((item: Record<string, unknown>) => item.operation === 'optimize').entries()) {
    report.compaction_debt.protected_partition_backlog = { files: index + 1, bytes: 100 * (index + 1),
      oldest_partition_start_ms: 999_000 };
  }
  assert.match(maintenanceDueWorkIssue(evidence.processes, maintenanceConfig(run), 1_000_000, 1_150_000)!, /optimize maintenance due-work grows/);
  policy.operation_reports.find((item: Record<string, unknown>) => item.operation === 'optimize').compaction_debt = null;
  assert.match(maintenanceDueWorkIssue(evidence.processes, maintenanceConfig(run), 1_000_000, 1_150_000)!, /unavailable or invalid/);
});

test('requires live guard coverage and confirmed caps and renders actual overload stops above the ceiling', () => {
  const run = sourceShapedRun(92, [1, 1, 1, 1, 1], 1_000_000);
  attachMaintenanceDueWork(run, 1_000_000, 1_150_000);
  const original = JSON.parse(String(run.rows[0]!.evidence_json));
  original.backlog = syntheticBacklog();
  const result = () => ({
    cohort: 'C1', candidate: 'guard', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [run],
  });
  run.rows[0]!.evidence_json = JSON.stringify(original);
  assert.match(renderCampaignResults([result()], 'C1'), /1\/3 protocol-complete repeats/);
  for (const change of [
    (evidence: typeof original) => { evidence.overload_guard = null; },
    (evidence: typeof original) => { evidence.overload_guard.status = 'unavailable'; },
    (evidence: typeof original) => { evidence.overload_guard.last_caps[0].confirmed = false; },
    (evidence: typeof original) => { evidence.overload_guard.requested_container_memory_limit_bytes = 8388608; },
    (evidence: typeof original) => { evidence.overload_guard.max_sampling_gap_ms = 5001; },
    (evidence: typeof original) => { evidence.overload_guard.coverage_start_ms = 1_000_001; },
  ]) {
    const changed = structuredClone(original);
    change(changed);
    run.rows[0]!.evidence_json = JSON.stringify(changed);
    assert.match(renderCampaignResults([result()], 'C1'), /overload_guard protection unavailable or invalid/);
  }
  Object.assign(run.config, { overload_guard: { memory_signal: 'cgroup_memory' } });
  Object.assign(run.config.tunables, { collector: { pressure: { rss_limit_bytes: 1000 } } });
  const cgroup = structuredClone(original);
  cgroup.overload_guard.settings.memory_signal = 'cgroup_memory';
  cgroup.overload_guard.last_caps[1].metric = 'collector_cgroup_memory_bytes';
  run.rows[0]!.evidence_json = JSON.stringify(cgroup);
  assert.match(renderCampaignResults([result()], 'C1'), /1\/3 protocol-complete repeats/);
  for (const change of [
    (evidence: typeof cgroup) => { evidence.overload_guard.settings.memory_signal = 'process_rss'; },
    (evidence: typeof cgroup) => { evidence.overload_guard.last_caps[1].metric = 'collector_rss_bytes'; },
    (evidence: typeof cgroup) => { evidence.overload_guard.last_caps[1].observed_value = 0; },
    (evidence: typeof cgroup) => { evidence.overload_guard.last_caps[1].cap = 2000; },
    (evidence: typeof cgroup) => { evidence.overload_guard.last_caps[1].confirmed = false; },
  ]) {
    const changed = structuredClone(cgroup);
    change(changed);
    run.rows[0]!.evidence_json = JSON.stringify(changed);
    assert.match(renderCampaignResults([result()], 'C1'), /overload_guard protection unavailable or invalid/);
  }
  run.rows[0]!.status = 'failed';
  for (const reason of ['hard_cap_fraction', 'zero_ingestion_commit_progress']) {
    const stopped = structuredClone(original);
    stopped.overload_stop = { reason, metric: reason === 'hard_cap_fraction' ? 'container_memory_bytes' : 'new_ingestion_commits',
      observed_value: reason === 'hard_cap_fraction' ? 900 : 0,
      cap: reason === 'hard_cap_fraction' ? 1000 : null, fraction: reason === 'hard_cap_fraction' ? 0.9 : null,
      window_index: 1, timestamp_ms: 1_060_000 };
    run.rows[0]!.evidence_json = JSON.stringify(stopped);
    assert.match(renderCampaignResults([result()], 'C1'), new RegExp('above_ceiling: overload_stop ' + reason));
    assert.doesNotMatch(renderCampaignResults([result()], 'C1'), /1\/3 protocol-complete repeats/);
  }
});

test('requires measured OPTIMIZE and PURGE debt vectors while preserving legitimate nulls', () => {
  const start = 1_000_000;
  const end = start + 150_000;
  const configFor = (run: ReturnType<typeof sourceShapedRun>) => maintenanceConfig(run);
  const evidenceFor = (run: ReturnType<typeof sourceShapedRun>) =>
    JSON.parse(String(run.rows[0]!.evidence_json)).processes;

  const checkpointNulls = sourceShapedRun(70, [1, 1, 1, 1, 1], start);
  attachMaintenanceDueWork(checkpointNulls, start, end);
  for (let window = 0; window < 5; window++) {
    mutateMaintenanceReport(checkpointNulls, 'checkpoint', window, start, (_report, dueWork) => {
      dueWork.backlog_count = null;
    });
  }
  assert.equal(maintenanceDueWorkIssue(
    evidenceFor(checkpointNulls), configFor(checkpointNulls), start, end,
  ), undefined);

  const mutations: Array<[string, (dueWork: Record<string, unknown>) => void]> = [
    ['optimize', dueWork => { dueWork.backlog_count = null; }],
    ['purge', dueWork => { dueWork.backlog_count = null; }],
    ['optimize', dueWork => { dueWork.eligible_bytes = null; dueWork.backlog_bytes = null; }],
    ['purge', dueWork => { dueWork.eligible_bytes = null; dueWork.backlog_bytes = null; }],
  ];
  mutations.forEach(([operation, mutate], index) => {
    const run = sourceShapedRun(71 + index, [1, 1, 1, 1, 1], start);
    attachMaintenanceDueWork(run, start, end);
    mutateMaintenanceReport(run, operation, 2, start, (_report, dueWork) => mutate(dueWork));
    assert.equal(maintenanceDueWorkIssue(
      evidenceFor(run), configFor(run), start, end,
    ), 'complete five-operation maintenance due-work evidence is unavailable or invalid',
      operation + ' must not qualify with a missing measurable debt vector');
  });
});

test('rejects exact-window commit buckets that do not sum to the run output', () => {
  const run = sourceShapedRun(33, [1, 1, 1, 1, 1], 1_000_000);
  const row = run.rows[0]!;
  const evidence = JSON.parse(String(row.evidence_json)) as {
    durable_commits: { complete_30_second_commit_windows: Array<{ rows: number; rows_per_second: number }> };
  };
  const first = evidence.durable_commits.complete_30_second_commit_windows[0]!;
  first.rows--;
  first.rows_per_second = first.rows / 30;
  row.evidence_json = JSON.stringify(evidence);
  const result = {
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [run],
  };
  const svg = renderCampaignResults([result], 'C1');
  assert.match(svg, /complete commit buckets do not reconcile to the exact-window run output total/);
});

test('reads one sustained run of contiguous emitter buckets but keeps missing debt proof unranked', () => {
  const result = {
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [sourceShapedRun(1, [1, 1, 1, 1, 1], 1_000_000)],
  };
  const svg = renderCampaignResults([result], 'C1');
  assert.match(svg, /snappy: unqualified, 0\/3 protocol-complete repeats; no committed-rate observation/);
  assert.match(svg, /complete five-operation maintenance due-work evidence is unavailable or invalid/);
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

test('requires factor leaves to exist in generated C1 cohort configs', () => {
  type TunedConfig = {
    tables: string[];
    tunables: { parquet_writer: { defaults: Record<string, unknown> }; [key: string]: unknown };
    [key: string]: unknown;
  };
  const base = JSON.parse(readFileSync(new URL('../../docs/experiments/arrowcol-oat/oat-config.tuned.json', import.meta.url), 'utf8')) as TunedConfig;
  const manifest = buildThroughputManifests(base)[0] as {
    campaign_profile: string;
    baseline_tunables: TunedConfig['tunables'];
    invariants: { table: string; replicas: number[] };
    comparisons: Array<{ candidates: Array<{ overrides: { tunables: { parquet_writer: { defaults: Record<string, unknown> } } } }> }>;
  };
  const configs = manifest.comparisons[0]!.candidates.map(candidate => ({
    ...base,
    campaign_profile: manifest.campaign_profile,
    tables: [manifest.invariants.table],
    replica_counts: manifest.invariants.replicas,
    tunables: { ...manifest.baseline_tunables, parquet_writer: {
      ...manifest.baseline_tunables.parquet_writer,
      defaults: { ...manifest.baseline_tunables.parquet_writer.defaults, ...candidate.overrides.tunables.parquet_writer.defaults },
    } },
  }));
  const config = configs[0]!;
  const run = sourceShapedRun(34, [1, 1, 1, 1, 1], 1_000_000);
  const official = { ...run, config };
  const result = {
    cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: '3',
    repeat: 1, expectedRepeats: 3, committedRowsPerSecond: 1,
    factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'tunables.parquet_writer.defaults.compression_level'],
    officialRuns: [official],
  };
  assert.doesNotThrow(() => renderCampaignResults([result], 'C1'));
  for (const generated of configs) {
    assert.doesNotThrow(() => renderCampaignResults([{ ...result, officialRuns: [{ ...run, config: generated }] }], 'C1'));
  }
  const missing = structuredClone(config);
  delete missing.tunables.parquet_writer.defaults.compression_level;
  assert.throws(() => renderCampaignResults([{ ...result, officialRuns: [{ ...run, config: missing }] }], 'C1'),
    /factorConfigPaths must identify distinct scalar or array config leaves/);
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

function coveredCandidate(candidate: string, rate: number, sequence: number) {
  return Array.from({ length: 3 }, (_, index) => {
    const run = sourceShapedRun(sequence + index, [rate, rate, rate, rate, rate], 1_000_000);
    attachMaintenanceDueWork(run, 1_000_000, 1_150_000);
    const evidence = JSON.parse(run.rows[0]!.evidence_json);
    evidence.backlog = syntheticBacklog();
    run.rows[0]!.evidence_json = JSON.stringify(evidence);
    run.config.tunables.parquet_writer.defaults.compression = candidate;
    return {
      cohort: 'C1', candidate, factorX: 'codec', valueX: candidate, factorY: 'level', valueY: '3',
      repeat: index + 1, expectedRepeats: 3, committedRowsPerSecond: rate,
      factorConfigPaths: ['tunables.parquet_writer.defaults.compression', 'parquet_writer.defaults.compression_level'],
      officialRuns: [run],
    };
  });
}

function mutateFactorEvidence(
  result: ReturnType<typeof coveredCandidate>[number],
  mutate: (evidence: Record<string, unknown>) => void,
): void {
  const row = result.officialRuns[0]!.rows[0]!;
  const evidence = JSON.parse(row.evidence_json) as Record<string, unknown>;
  mutate(evidence);
  row.evidence_json = JSON.stringify(evidence);
}

test('qualifies fully covered runner-control factors', () => {
  const candidate = coveredCandidate('runner', 100, 220).map(row => ({
    ...row, factorConfigPaths: ['maintenance.enabled', 'maintenance.apply'],
  }));
  for (const row of candidate) {
    mutateFactorEvidence(row, evidence => {
      evidence.projection_only = [];
      evidence.runner_controls = [
        { path: 'maintenance.enabled', reason: 'maintenance_runner_control' },
        { path: 'maintenance.apply', reason: 'maintenance_runner_control' },
      ];
    });
  }
  assert.match(renderCampaignResults(candidate, 'C1'), /runner \(3\/3\)/);
});

test('nulls a duration-based run when the varied factor is a one-shot row limit', () => {
  const candidate = coveredCandidate('one-shot-limit', 100, 235).map(row => ({
    ...row, factorConfigPaths: ['tunables.client.max_rows', 'parquet_writer.defaults.compression'],
  }));
  for (const row of candidate) {
    Object.assign(row.officialRuns[0]!.config.tunables, { client: { max_rows: 129 } });
    mutateFactorEvidence(row, evidence => {
      evidence.inert_fields = [{ path: 'client.max_rows', reason: 'duration_based_run' }];
      evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }];
    });
  }
  const svg = renderCampaignResults(candidate, 'C1');
  assert.match(svg, /inert: client\.max_rows: duration_based_run/);
  assert.doesNotMatch(svg, /observed mean 100 rows\/s|>100\*?<\/text>/);
});

test('nulls connection backoff when connection retries are disabled', () => {
  const candidate = coveredCandidate('backoff', 100, 235).map(row => ({
    ...row, factorConfigPaths: ['tunables.transport.retry_backoff_ms', 'parquet_writer.defaults.compression'],
  }));
  for (const row of candidate) {
    Object.assign(row.officialRuns[0]!.config.tunables, { transport: { retry_backoff_ms: 37 } });
    mutateFactorEvidence(row, evidence => {
      evidence.inert_fields = [{ path: 'transport.retry_backoff_ms', reason: 'connection_retries_disabled' }];
      evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }];
    });
  }
  const svg = renderCampaignResults(candidate, 'C1');
  assert.match(svg, /inert: transport\.retry_backoff_ms: connection_retries_disabled/);
  assert.doesNotMatch(svg, /observed mean 100 rows\/s|>100\*?<\/text>/);
});

test('nulls stream backoff when stream retries are disabled', () => {
  const candidate = coveredCandidate('stream-backoff', 100, 235).map(row => ({
    ...row, factorConfigPaths: ['tunables.client.stream_retry_base_backoff_ms', 'parquet_writer.defaults.compression'],
  }));
  for (const row of candidate) {
    Object.assign(row.officialRuns[0]!.config.tunables, { client: { stream_retry_base_backoff_ms: 13 } });
    mutateFactorEvidence(row, evidence => {
      evidence.inert_fields = [{ path: 'client.stream_retry_base_backoff_ms', reason: 'stream_retries_disabled' }];
      evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }];
    });
  }
  const svg = renderCampaignResults(candidate, 'C1');
  assert.match(svg, /inert: client\.stream_retry_base_backoff_ms: stream_retries_disabled/);
  assert.doesNotMatch(svg, /observed mean 100 rows\/s|>100\*?<\/text>/);
});

test('nulls HTTP/2 factors that are overridden or disabled', () => {
  for (const [field, reason] of [
    ['stream_window_bytes', 'adaptive_http2_window'],
    ['keepalive_timeout_ms', 'http2_keepalive_disabled'],
    ['tcp_keepalive_retries', 'tcp_keepalive_disabled'],
    ['rate_limit_period_ms', 'rate_limit_disabled'],
  ] as const) {
    const candidate = coveredCandidate('http2', 100, 235).map(row => ({
      ...row, factorConfigPaths: [`tunables.transport.${field}`, 'parquet_writer.defaults.compression'],
    }));
    for (const row of candidate) {
      Object.assign(row.officialRuns[0]!.config.tunables, { transport: { [field]: 2000 } });
      mutateFactorEvidence(row, evidence => {
        evidence.inert_fields = [{ path: `transport.${field}`, reason }];
        evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }];
      });
    }
    const svg = renderCampaignResults(candidate, 'C1');
    assert.ok(svg.includes(`inert: transport.${field}: ${reason}`));
    assert.doesNotMatch(svg, /observed mean 100 rows\/s|>100\*?<\/text>/);
  }
});

test('nulls a per-column dictionary page limit when dictionary is explicitly disabled on that column', () => {
  const candidate = coveredCandidate('dictionary-off', 100, 240).map(row => ({
    ...row,
    factorX: 'dictionary_page_size_limit',
    valueX: '4096',
    factorY: 'codec',
    valueY: 'dictionary-off',
    factorConfigPaths: [
      'tunables.parquet_writer.per_column.payload.dictionary_page_size_limit',
      'parquet_writer.defaults.compression',
    ],
  }));
  for (const row of candidate) {
    Object.assign(row.officialRuns[0]!.config.tunables.parquet_writer, {
      per_column: {
        payload: { dictionary_enabled: false, dictionary_page_size_limit: 4096 },
        other: { dictionary_enabled: true, dictionary_page_size_limit: 4096 },
      },
    });
    mutateFactorEvidence(row, evidence => {
      evidence.inert_fields = [{
        path: 'parquet_writer.per_column.payload.dictionary_page_size_limit',
        reason: 'dictionary_disabled',
      }];
      evidence.projection_only = [{
        path: 'parquet_writer.defaults.compression',
        reason: 'consumer_startup_proof_unverified',
      }];
    });
  }
  const svg = renderCampaignResults(candidate, 'C1');
  assert.match(svg, /dictionary-off: unqualified.*no committed-rate observation \(inert: parquet_writer\.per_column\.payload\.dictionary_page_size_limit: dictionary_disabled/);
  assert.doesNotMatch(svg, /dictionary-off \(3\/3\)|observed mean 100 rows\/s|>100\*?<\/text>/);
  for (const row of candidate) {
    mutateFactorEvidence(row, evidence => {
      evidence.inert_fields = [{
        path: 'parquet_writer.per_column.other.dictionary_page_size_limit',
        reason: 'dictionary_disabled',
      }];
    });
  }
  const unrelatedSvg = renderCampaignResults(candidate, 'C1');
  assert.match(unrelatedSvg, /varied factor coverage unknown/);
  assert.doesNotMatch(unrelatedSvg, /dictionary-off: unqualified.*\(inert:/);
});

test('fails closed on duplicate or unknown classifications even with otherwise complete coverage', () => {
  for (const scenario of ['unknown', 'duplicate', 'verified'] as const) {
    const candidate = coveredCandidate('runner', 100, 230);
    for (const row of candidate) {
      mutateFactorEvidence(row, evidence => {
        evidence.projection_only = [];
        evidence.consumer_verified = [
          { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_readback' },
          { path: 'parquet_writer.defaults.compression_level', reason: 'consumer_startup_readback' },
        ];
      });
    }
    mutateFactorEvidence(candidate[1]!, evidence => {
      if (scenario === 'unknown') {
        evidence.consumer_verified = [
          { path: 'parquet_writer.defaults.compression', reason: 'new_runner_reason' },
          { path: 'parquet_writer.defaults.compression_level', reason: 'consumer_startup_readback' },
        ];
      } else if (scenario === 'duplicate') {
        evidence.projection_only = [
          { path: 'tunables.parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' },
        ];
      } else {
        evidence.consumer_verified = [
          { path: 'tunables.parquet_writer.defaults.compression', reason: 'unrecognized' },
        ];
      }
    });
    const svg = renderCampaignResults(candidate, 'C1');
    assert.match(svg, /varied factor coverage unknown/);
    assert.doesNotMatch(svg, /runner \(3\/3\)|observed mean 100 rows\/s|>100<\/text>/);
  }
});

test('excludes an entire cell when any repeat has an inert factor, including unprefixed evidence paths', () => {
  const active = coveredCandidate('active', 100, 100);
  const inert = coveredCandidate('inert', 900, 110);
  mutateFactorEvidence(inert[1]!, evidence => {
    evidence.inert_fields = [{ path: 'parquet_writer.defaults.compression_level', reason: 'maintenance_disabled' }];
    evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }];
  });
  const svg = renderCampaignResults([...active, ...inert], 'C1');
  assert.match(svg, /No qualified winner/);
  assert.doesNotMatch(svg, /inert \(3\/3\)/);
  assert.match(svg, /inert: unqualified.*no committed-rate observation \(inert: parquet_writer\.defaults\.compression_level: maintenance_disabled/);
  assert.doesNotMatch(svg, />900(?:\.00)?</);
});

test('nulls projection-only observations pending independent applicability proof and matches prefixed evidence paths', () => {
  const projected = coveredCandidate('projected', 800, 120);
  mutateFactorEvidence(projected[0]!, evidence => {
    evidence.projection_only = [
      { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' },
      { path: 'tunables.parquet_writer.defaults.compression_level', reason: 'consumer_startup_proof_unverified' },
    ];
  });
  const svg = renderCampaignResults([...coveredCandidate('active', 100, 130), ...projected], 'C1');
  assert.match(svg, /No qualified winner/);
  assert.doesNotMatch(svg, /projected \(3\/3\)/);
  assert.match(svg, />null<\/text>/);
  assert.doesNotMatch(svg, />800\*?<\/text>|observed mean 800 rows\/s/);
  assert.match(svg, /projected: unqualified.*no committed-rate observation \(consumption unproven; applicability unknown: .*tunables\.parquet_writer\.defaults\.compression_level: consumer_startup_proof_unverified/);
});

test('a projection-only repeat nulls its whole cell and preserves a dead Kustainer comment', () => {
  const candidate = coveredCandidate('projected-death', 800, 250);
  for (const row of candidate) {
    mutateFactorEvidence(row, evidence => {
      evidence.projection_only = [];
      evidence.consumer_verified = [
        { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_readback' },
        { path: 'parquet_writer.defaults.compression_level', reason: 'consumer_startup_readback' },
      ];
    });
  }
  mutateFactorEvidence(candidate[1]!, evidence => {
    evidence.consumer_verified = [
      { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_readback' },
    ];
    evidence.projection_only = [
      { path: 'parquet_writer.defaults.compression_level', reason: 'consumer_startup_proof_unverified' },
    ];
  });
  const results = candidate.map(row => row.repeat === 2 ?
    { ...row, diedFirst: 'Kustainer', comment: 'emulator exited before downstream observation' } : row);
  const svg = renderCampaignResults(results, 'C1');
  assert.match(svg, /consumption unproven; applicability unknown/);
  assert.match(svg, /Kustainer \(collector-only; downstream metrics: null; committed rows\/s: null\).*emulator exited before downstream observation/);
  assert.doesNotMatch(svg, /projected-death \(3\/3\)|observed mean 800 rows\/s|>800\*?<\/text>/);
});

test('accepts table-writer readback for active Parquet limits', () => {
  const candidate = coveredCandidate('writer-limits', 800, 260).map(row => ({
    ...row, factorConfigPaths: ['tunables.parquet_writer.max_row_group_rows', 'tunables.parquet_writer.data_page_row_count_limit'],
  }));
  for (const row of candidate) {
    Object.assign(row.officialRuns[0]!.config.tunables, { parquet_writer: {
      max_row_group_rows: 16384, data_page_row_count_limit: 2048,
    } });
    mutateFactorEvidence(row, evidence => {
      evidence.projection_only = [];
      evidence.consumer_verified = [
        { path: 'parquet_writer.max_row_group_rows', reason: 'consumer_table_writer_readback' },
        { path: 'parquet_writer.data_page_row_count_limit', reason: 'consumer_table_writer_readback' },
      ];
    });
  }
  assert.match(renderCampaignResults(candidate, 'C1'), />800\*?<\/text>/);
});

test('accepts table-adaptive readback for an active sizing factor', () => {
  const candidate = coveredCandidate('adaptive-sizing', 800, 260).map(row => ({
    ...row, factorConfigPaths: ['tunables.collector.batching.adaptive_file.target_parquet_bytes',
      'tunables.collector.batching.adaptive_file.min_input_bytes'],
  }));
  for (const row of candidate) {
    Object.assign(row.officialRuns[0]!.config.tunables, { collector: {
      batching: { adaptive_file: { target_parquet_bytes: 134217728, min_input_bytes: 16777216 } },
    } });
    mutateFactorEvidence(row, evidence => {
      evidence.projection_only = [];
      evidence.consumer_verified = [
        { path: 'collector.batching.adaptive_file.target_parquet_bytes', reason: 'consumer_table_adaptive_readback' },
        { path: 'collector.batching.adaptive_file.min_input_bytes', reason: 'consumer_table_adaptive_readback' },
      ];
    });
  }
  assert.match(renderCampaignResults(candidate, 'C1'), />800\*?<\/text>/);
});

test('an active cell retains its committed rate after Kustainer death', () => {
  const candidate = coveredCandidate('active-death', 800, 260);
  for (const row of candidate) {
    mutateFactorEvidence(row, evidence => {
      evidence.projection_only = [];
      evidence.consumer_verified = [
        { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_readback' },
        { path: 'parquet_writer.defaults.compression_level', reason: 'consumer_startup_readback' },
      ];
    });
  }
  const results = candidate.map(row => row.repeat === 2 ?
    { ...row, diedFirst: 'Kustainer', comment: 'emulator exited before downstream observation' } : row);
  const [death] = validateCampaignMeasurements([results[1]!]);
  assert.equal(death!.committedRowsPerSecond, 800);
  assert.equal(death!.qapCompletionPercent, null);
  assert.equal(death!.qapPendingArtifacts, null);
  assert.equal(death!.qapUnhealthyCatalogs, null);
  const svg = renderCampaignResults(results, 'C1');
  assert.match(svg, /Kustainer \(collector-only; downstream metrics: null; committed rows\/s: 800\).*emulator exited before downstream observation/);
  assert.match(svg, />800\*?<\/text>/);
});

test('fails closed on missing or malformed coverage and does not infer a legacy factor path', () => {
  const mutations: Array<(evidence: Record<string, unknown>) => void> = [
    evidence => { delete evidence.inert_fields; },
    evidence => { delete evidence.projection_only; },
    evidence => { delete evidence.runner_controls; },
    evidence => { delete evidence.consumer_verified; },
    evidence => { evidence.inert_fields = null; },
    evidence => { evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: '' }]; },
    evidence => { evidence.projection_only = [{ path: 'parquet_writer.defaults.compression', reason: 'new_reason' }]; },
    evidence => { evidence.inert_fields = [{ path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }]; },
    evidence => { evidence.projection_only = [
      { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' },
      { path: 'tunables.parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' },
    ]; },
    evidence => {
      evidence.inert_fields = [{ path: 'parquet_writer.defaults.compression', reason: 'maintenance_disabled' }];
      evidence.projection_only = [{ path: 'tunables.parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' }];
    },
    evidence => { evidence.inert_fields = [{ path: 'unrelated', reason: 'unrecognized' }]; },
    evidence => { evidence.inert_fields = [{ path: 'tunables..bad', reason: 'maintenance_disabled' }]; },
    evidence => { evidence.projection_only = []; },
    evidence => { evidence.projection_only = [{ path: 'parquet_writer.defaults', reason: 'consumer_startup_proof_unverified' }]; },
    evidence => { evidence.runner_controls = [{ path: 'other', reason: 'unrecognized' }]; },
    evidence => { evidence.consumer_verified = [{ path: 'other', reason: 'unrecognized' }]; },
    evidence => {
      evidence.runner_controls = [{ path: 'tunables.parquet_writer.defaults.compression', reason: 'unrecognized' }];
    },
    evidence => {
      evidence.consumer_verified = [{ path: 'tunables.parquet_writer.defaults.compression', reason: 'unrecognized' }];
    },
    evidence => { evidence.runner_controls = null; },
    evidence => {
      evidence.runner_controls = [{ path: 'other', reason: 'unrecognized' }];
      evidence.consumer_verified = [{ path: 'tunables.other', reason: 'unrecognized' }];
    },
  ];
  for (const [index, mutate] of mutations.entries()) {
    const candidate = coveredCandidate('unknown', 700, 150 + index * 3);
    mutateFactorEvidence(candidate[1]!, mutate);
    const svg = renderCampaignResults(candidate, 'C1');
    assert.match(svg, /varied factor coverage unknown/);
    assert.doesNotMatch(svg, /unknown \(3\/3\)|observed mean 700|>700<\/text>/);
  }
  const legacy = coveredCandidate('legacy', 700, 170).map(row => ({ ...row, factorConfigPaths: undefined }));
  const svg = renderCampaignResults(legacy, 'C1');
  assert.match(svg, /varied factor coverage unknown/);
  assert.doesNotMatch(svg, /legacy \(3\/3\)|observed mean 700/);
  const invalidJson = coveredCandidate('bad-json', 700, 190);
  invalidJson[1]!.officialRuns[0]!.rows[0]!.evidence_json = '{';
  const invalidSvg = renderCampaignResults(invalidJson, 'C1');
  assert.match(invalidSvg, /varied factor coverage unknown/);
  assert.doesNotMatch(invalidSvg, /bad-json \(3\/3\)|observed mean 700/);
});

test('empty arrays do not establish factor coverage and unrelated classifications do not cover a leaf', () => {
  const candidate = coveredCandidate('active', 100, 180);
  mutateFactorEvidence(candidate[0]!, evidence => {
    evidence.projection_only = [];
  });
  assert.match(renderCampaignResults(candidate, 'C1'), /varied factor coverage unknown/);
  mutateFactorEvidence(candidate[0]!, evidence => {
    evidence.inert_fields = [{ path: 'tunables.parquet_writer.defaults.other', reason: 'maintenance_disabled' }];
    evidence.projection_only = [
      { path: 'collector.other', reason: 'consumer_startup_proof_unverified' },
      { path: 'parquet_writer.defaults.compression', reason: 'consumer_startup_proof_unverified' },
    ];
  });
  const svg = renderCampaignResults(candidate, 'C1');
  assert.match(svg, /varied factor coverage unknown/);
  assert.doesNotMatch(svg, /active \(3\/3\)|observed mean 100 rows\/s/);
});

test('inert evidence dominates missing coverage for candidates sharing a heatmap cell', () => {
  const complete = coveredCandidate('shared', 100, 200);
  const other = coveredCandidate('other', 900, 210).map(row => ({ ...row, valueX: 'shared' }));
  for (const row of other) row.officialRuns[0]!.config.tunables.parquet_writer.defaults.compression = 'shared';
  mutateFactorEvidence(other[0]!, evidence => {
    evidence.inert_fields = [{ path: 'tunables.parquet_writer.defaults.compression', reason: 'maintenance_disabled' }];
    delete evidence.projection_only;
  });
  const svg = renderCampaignResults([...complete, ...other], 'C1');
  assert.doesNotMatch(svg, /shared \(3\/3\)|other \(3\/3\)|observed mean (?:100|900) rows\/s|>(?:100|900)<\/text>/);
  assert.match(svg, /inert: tunables\.parquet_writer\.defaults\.compression: maintenance_disabled/);
});
