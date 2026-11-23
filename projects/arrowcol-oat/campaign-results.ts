import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { evaluateDataFlow } from './dataflow-verdict.js';
import type { OatRunStatus } from './launch.js';

export interface CampaignMeasurement {
  cohort: string;
  candidate: string;
  factorX: string;
  valueX: string;
  factorY: string;
  valueY: string;
  repeat: number;
  committedRowsPerSecond: number | null;
  expectedRepeats: number | null;
  factorConfigPaths?: [string, string];
  officialRuns?: OfficialRun[];
  diedFirst?: string | null;
  comment?: string | null;
  downstreamStatus?: 'unavailable' | 'observed_or_not_attempted';
  qapCompletionPercent: number | null;
  qapPendingArtifacts: number | null;
  qapUnhealthyCatalogs: number | null;
  collectorOnly: boolean;
}

interface OfficialRun {
  status: OatRunStatus;
  rows: RecordValue[];
  expectedSha: string;
  mode: 'local' | 'aks';
  config: RecordValue;
}

type RecordValue = Record<string, unknown>;
type FactorCoverage = { status: 'active' | 'projection_only' | 'unknown' | 'inert'; reason: string };
type Cell = { x: string; y: string; rates: number[]; coverage: FactorCoverage };
type Summary = {
  candidate: string;
  rates: number[];
  mean: number;
  min: number;
  max: number;
  completedRepeats: number;
  expectedRepeats: number | null;
  status: 'qualified' | 'collector-only' | 'failed' | 'unqualified';
  reason: string;
};
type OfficialWindow = { rate: number };
type RepeatAssessment = { qualified: boolean; rates: number[]; reason: string };

function isRecord(value: unknown): value is RecordValue {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requiredText(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(field + ' must be a non-empty string');
  return value;
}

function nullableMetric(value: unknown, field: string, maximum = Number.POSITIVE_INFINITY): number | null {
  if (value == null) return null;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || value > maximum) {
    throw new Error(field + ' must be a non-negative finite number or null');
  }
  return value;
}

function parseOfficialRun(value: unknown, field: string): OfficialRun {
  if (!isRecord(value) || !isRecord(value.status) || !Array.isArray(value.rows) || !isRecord(value.config)) {
    throw new Error(field + ' must contain an official run status, rows, and config');
  }
  const status = value.status;
  const state = status.state;
  if (typeof status.id !== 'string' || (state !== 'queued' && state !== 'running' && state !== 'succeeded' && state !== 'failed' && state !== 'cancelled') ||
      typeof status.progress !== 'string' || !Number.isSafeInteger(status.result_count) ||
      typeof status.error !== 'string' && status.error !== null) {
    throw new Error(field + ' has an invalid official run status');
  }
  const runRows = value.rows.map(row => {
    if (!isRecord(row)) throw new Error(field + ' rows must be official result objects');
    return row;
  });
  if (value.mode !== 'local' && value.mode !== 'aks') throw new Error(field + ' mode must be local or aks');
  return {
    status: { id: status.id, config_sha256: typeof status.config_sha256 === 'string' && /^[0-9a-f]{64}$/.test(status.config_sha256) ? status.config_sha256 : null,
      state, progress: status.progress, result_count: Number(status.result_count), error: status.error },
    rows: runRows,
    expectedSha: requiredText(value.expectedSha, field + ' expectedSha'),
    mode: value.mode,
    config: value.config,
  };
}

function escapeXml(value: string): string {
  const entities: Record<string, string> = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' };
  return value.replace(/[&<>"']/g, character => entities[character]!);
}

function numberLabel(value: number): string {
  return value.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

export function validateCampaignMeasurements(value: unknown): CampaignMeasurement[] {
  if (!Array.isArray(value) || value.length === 0) throw new Error('results must be a non-empty array');
  const identities = new Set<string>();
  const candidates = new Map<string, { factorX: string; valueX: string; factorY: string; valueY: string; expectedRepeats: number | null }>();
  return value.map((raw, index) => {
    if (!isRecord(raw)) throw new Error('result ' + index + ' must be an object');
    const rate = raw.committedRowsPerSecond;
    if (rate !== null && (typeof rate !== 'number' || !Number.isFinite(rate) || rate < 0)) {
      throw new Error('result ' + index + ' committedRowsPerSecond must be a non-negative number or null');
    }
    const cohort = requiredText(raw.cohort, 'result ' + index + ' cohort');
    const candidate = requiredText(raw.candidate, 'result ' + index + ' candidate');
    const factorX = requiredText(raw.factorX, 'result ' + index + ' factorX');
    const valueX = requiredText(raw.valueX, 'result ' + index + ' valueX');
    const factorY = requiredText(raw.factorY, 'result ' + index + ' factorY');
    const valueY = requiredText(raw.valueY, 'result ' + index + ' valueY');
    if (!Number.isSafeInteger(raw.repeat) || Number(raw.repeat) < 1) throw new Error('result ' + index + ' repeat must be a positive integer');
    const repeat = Number(raw.repeat);
    const expectedRepeats = raw.expectedRepeats === undefined ? null : Number(raw.expectedRepeats);
    if (expectedRepeats !== null && (!Number.isSafeInteger(expectedRepeats) || expectedRepeats < 1 || repeat > expectedRepeats)) {
      throw new Error('result ' + index + ' expectedRepeats must include its positive repeat number');
    }
    const identity = JSON.stringify([cohort, candidate, repeat]);
    if (identities.has(identity)) throw new Error('duplicate candidate/repeat identity: ' + cohort + '/' + candidate + '/' + repeat);
    identities.add(identity);
    const candidateKey = JSON.stringify([cohort, candidate]);
    const candidateInfo = candidates.get(candidateKey);
    if (candidateInfo && (candidateInfo.factorX !== factorX || candidateInfo.valueX !== valueX || candidateInfo.factorY !== factorY ||
        candidateInfo.valueY !== valueY || candidateInfo.expectedRepeats !== expectedRepeats)) {
      throw new Error('candidate repeats have incompatible factors or expected repeat counts: ' + cohort + '/' + candidate);
    }
    candidates.set(candidateKey, { factorX, valueX, factorY, valueY, expectedRepeats });
    const diedFirst = raw.diedFirst == null ? null : requiredText(raw.diedFirst, 'result ' + index + ' diedFirst');
    const comment = raw.comment == null ? null : requiredText(raw.comment, 'result ' + index + ' comment');
    const kustainerDied = diedFirst?.toLowerCase() === 'kustainer';
    if (kustainerDied && !comment) throw new Error('a dead Kustainer requires a concrete comment');
    const qapCompletionPercent = nullableMetric(raw.qapCompletionPercent, 'result ' + index + ' qapCompletionPercent', 100);
    const qapPendingArtifacts = nullableMetric(raw.qapPendingArtifacts, 'result ' + index + ' qapPendingArtifacts');
    const qapUnhealthyCatalogs = nullableMetric(raw.qapUnhealthyCatalogs, 'result ' + index + ' qapUnhealthyCatalogs');
    if (kustainerDied && [qapCompletionPercent, qapPendingArtifacts, qapUnhealthyCatalogs].some(metric => metric !== null)) {
      throw new Error('a dead Kustainer requires unavailable downstream metrics to be null');
    }
    const downstreamStatus = raw.downstreamStatus;
    if (downstreamStatus !== undefined && downstreamStatus !== 'unavailable' && downstreamStatus !== 'observed_or_not_attempted') {
      throw new Error('result ' + index + ' downstreamStatus is invalid');
    }
    const factorConfigPaths = raw.factorConfigPaths;
    if (factorConfigPaths !== undefined && (!Array.isArray(factorConfigPaths) || factorConfigPaths.length !== 2 ||
        factorConfigPaths.some(path => typeof path !== 'string' || path.trim() === ''))) {
      throw new Error('result ' + index + ' factorConfigPaths must contain two config paths');
    }
    if (Object.hasOwn(raw, 'officialWindows')) throw new Error('result ' + index + ' must provide one officialRuns entry per repeat, not per window');
    const officialRuns = raw.officialRuns;
    if (officialRuns !== undefined && !Array.isArray(officialRuns)) throw new Error('result ' + index + ' officialRuns must be an array');
    return {
      cohort, candidate, factorX, valueX, factorY, valueY, repeat,
      committedRowsPerSecond: rate,
      expectedRepeats,
      factorConfigPaths: factorConfigPaths === undefined ? undefined : [String(factorConfigPaths[0]), String(factorConfigPaths[1])],
      officialRuns: officialRuns?.map((run, runIndex) => parseOfficialRun(run, 'result ' + index + ' officialRuns[' + runIndex + ']')),
      diedFirst, comment,
      downstreamStatus: kustainerDied ? 'unavailable' : downstreamStatus as CampaignMeasurement['downstreamStatus'],
      qapCompletionPercent: kustainerDied ? null : qapCompletionPercent,
      qapPendingArtifacts: kustainerDied ? null : qapPendingArtifacts,
      qapUnhealthyCatalogs: kustainerDied ? null : qapUnhealthyCatalogs,
      collectorOnly: kustainerDied || downstreamStatus === 'unavailable',
    };
  });
}

function svgText(x: number, y: number, value: string, size = 13, weight = 'normal'): string {
  return '<text x="' + x + '" y="' + y + '" font-size="' + size + '" font-weight="' + weight + '">' + escapeXml(value) + '</text>';
}

function cellColor(value: number, minimum: number, maximum: number): string {
  const ratio = maximum === minimum ? 0.65 : (value - minimum) / (maximum - minimum);
  const red = Math.round(239 - ratio * 170);
  const green = Math.round(247 - ratio * 80);
  const blue = Math.round(255 - ratio * 15);
  return 'rgb(' + red + ',' + green + ',' + blue + ')';
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(stableJson).join(',') + ']';
  if (isRecord(value)) return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + stableJson(value[key])).join(',') + '}';
  return JSON.stringify(value);
}

type ConfigFactorPath = { root: 'config' | 'tunables'; parts: string[] };

function configFactorPath(config: RecordValue, path: string): ConfigFactorPath | undefined {
  const parts = path.split('.');
  if (parts.some(part => part.trim() === '')) return undefined;
  if (parts[0] === 'tunables') {
    if (!isRecord(config.tunables) || parts.length === 1) return undefined;
    return { root: 'tunables', parts: parts.slice(1) };
  }
  if (parts.length === 1 && parts[0] === 'dfe_worker_concurrency' && Object.hasOwn(config, parts[0])) {
    return { root: 'config', parts };
  }
  if (!Object.hasOwn(config, parts[0]!) && isRecord(config.tunables)) return { root: 'tunables', parts };
  return undefined;
}

function resolveFactorLeaf(config: RecordValue, path: string): { parent: RecordValue; key: string } | undefined {
  const factorPath = configFactorPath(config, path);
  if (!factorPath) return undefined;
  const root = factorPath.root === 'tunables' ? config.tunables : config;
  if (!isRecord(root)) return undefined;
  let parent: RecordValue = root;
  for (const part of factorPath.parts.slice(0, -1)) {
    if (!isRecord(parent[part])) return undefined;
    parent = parent[part] as RecordValue;
  }
  const key = factorPath.parts[factorPath.parts.length - 1]!;
  if (!Object.hasOwn(parent, key) || isRecord(parent[key])) return undefined;
  return { parent, key };
}

function factorPathsOverlap(config: RecordValue, first: string, second: string): boolean {
  const a = configFactorPath(config, first);
  const b = configFactorPath(config, second);
  if (!a || !b) return true;
  if (a.root !== b.root) return false;
  const shared = Math.min(a.parts.length, b.parts.length);
  return a.parts.slice(0, shared).every((part, index) => part === b.parts[index]);
}

function removeFactorPath(config: RecordValue, path: string): boolean {
  const leaf = resolveFactorLeaf(config, path);
  if (!leaf) return false;
  delete leaf.parent[leaf.key];
  return true;
}

function officialImageIds(run: OfficialRun): string[] | null {
  const images: string[] = [];
  for (const row of run.rows) {
    if (typeof row.evidence_json !== 'string') continue;
    try {
      const evidence: unknown = JSON.parse(row.evidence_json);
      if (!isRecord(evidence) || !Array.isArray(evidence.processes)) continue;
      for (const process of evidence.processes) {
        if (isRecord(process) && process.component === 'collector' && typeof process.image_id === 'string') images.push(process.image_id);
      }
    } catch { /* invalid official evidence remains unqualified */ }
  }
  return images.length ? images.sort() : null;
}

function assertCompatibleBaselines(rows: CampaignMeasurement[], cohort: string): void {
  let source: string | undefined;
  let configFingerprint: string | undefined;
  let imageFingerprint: string | undefined;
  let mode: string | undefined;
  for (const row of rows) {
    if (!row.officialRuns?.length) continue;
    if (!row.factorConfigPaths) continue;
    for (const run of row.officialRuns) {
      if (factorPathsOverlap(run.config, row.factorConfigPaths[0], row.factorConfigPaths[1]) ||
          !resolveFactorLeaf(run.config, row.factorConfigPaths[0]) || !resolveFactorLeaf(run.config, row.factorConfigPaths[1])) {
        throw new Error('cohort ' + cohort + ' factorConfigPaths must identify distinct scalar or array config leaves');
      }
      const config = structuredClone(run.config);
      if (!removeFactorPath(config, row.factorConfigPaths[0]) || !removeFactorPath(config, row.factorConfigPaths[1])) {
        throw new Error('cohort ' + cohort + ' factorConfigPaths do not resolve in official config');
      }
      const fingerprint = stableJson(config);
      const images = officialImageIds(run);
      const image = images ? stableJson(images) : undefined;
      if (source !== undefined && (source !== run.expectedSha || configFingerprint !== fingerprint || mode !== run.mode) ||
          image !== undefined && imageFingerprint !== undefined && image !== imageFingerprint) {
        throw new Error('cohort ' + cohort + ' has incompatible frozen source/resource/load baselines');
      }
      source = run.expectedSha;
      configFingerprint = fingerprint;
      if (image !== undefined) imageFingerprint = image;
      mode = run.mode;
    }
  }
}

function canonicalFactorPath(path: string): string | undefined {
  const parts = path.split('.');
  if (parts.some(part => part.trim() === '' || part !== part.trim())) return undefined;
  if (parts[0] === 'tunables') parts.shift();
  return parts.length ? parts.join('.') : undefined;
}

const proofReasonPaths: Readonly<Record<string, readonly string[]>> = {
  no_nullable_generated_columns: ['client.null_fraction'],
  no_eligible_generated_rows: [
    'client.pod_ordinal', 'client.guid_uniqueness', 'client.value_variation',
    'client.string_width', 'client.string_length_distribution',
  ],
  all_values_skewed: ['client.value_variation', 'client.cardinality'],
  one_byte_maximum_width: ['client.string_length_distribution'],
  consumer_startup_and_prepared_corpus: [
    'client.pod_ordinal', 'client.guid_uniqueness', 'client.value_variation', 'client.string_width',
    'client.string_length_distribution', 'client.entropy', 'client.cardinality', 'client.seed', 'client.skew',
  ],
  committed_table_metadata_readback: ['collector.commit.delta_stats_columns'],
};

function factorCoverage(row: CampaignMeasurement): FactorCoverage {
  const unknown: FactorCoverage = { status: 'unknown', reason: 'varied factor coverage unknown' };
  if (!row.factorConfigPaths || !row.officialRuns?.length) return unknown;
  const paths = row.factorConfigPaths.map(canonicalFactorPath);
  if (paths.some(path => !path) || new Set(paths).size !== paths.length) return unknown;
  const matches = (entryPath: string): boolean => {
    const canonical = canonicalFactorPath(entryPath);
    return canonical !== undefined && paths.includes(canonical);
  };
  let incomplete = false;
  const inert: string[] = [];
  const projected: string[] = [];
  for (const run of row.officialRuns) {
    if (row.factorConfigPaths.some(path => !resolveFactorLeaf(run.config, path)) || !run.rows.length) incomplete = true;
    for (const result of run.rows) {
      let evidence: unknown;
      try { evidence = typeof result.evidence_json === 'string' ? JSON.parse(result.evidence_json) : null; } catch { evidence = null; }
      if (!isRecord(evidence)) { incomplete = true; continue; }
      const seen = new Set<string>();
      for (const [field, reasons, allowedReasons] of [
        ['inert_fields', inert, ['maintenance_disabled', 'dictionary_disabled', 'duration_based_run', 'connection_retries_disabled', 'stream_retries_disabled', 'http2_keepalive_disabled', 'adaptive_http2_window', 'tcp_keepalive_disabled', 'rate_limit_disabled', 'no_nullable_generated_columns', 'no_eligible_generated_rows', 'all_values_skewed', 'one_byte_maximum_width']],
        ['projection_only', projected, ['consumer_startup_proof_unverified']],
        ['runner_controls', undefined, ['maintenance_runner_control', 'router_log_filter_inheritance']],
        ['consumer_verified', undefined, ['consumer_startup_readback', 'consumer_table_writer_readback', 'consumer_table_adaptive_readback', 'consumer_startup_and_prepared_corpus', 'committed_table_metadata_readback']],
      ] as const) {
        const entries = evidence[field];
        if (!Array.isArray(entries)) { incomplete = true; continue; }
        for (const entry of entries) {
          if (!isRecord(entry) || typeof entry.path !== 'string') {
            incomplete = true;
            continue;
          }
          const path = canonicalFactorPath(entry.path);
          if (!path) { incomplete = true; continue; }
          if (seen.has(path)) incomplete = true;
          seen.add(path);
          if (typeof entry.reason !== 'string' || !(allowedReasons as readonly string[]).includes(entry.reason) ||
              (Object.hasOwn(proofReasonPaths, entry.reason) && !proofReasonPaths[entry.reason]!.includes(path))) {
            incomplete = true;
            continue;
          }
          if (reasons && matches(entry.path)) reasons.push(entry.path + ': ' + entry.reason);
        }
      }
      if (paths.some(path => !seen.has(path!))) incomplete = true;
    }
  }
  if (inert.length) return {
    status: 'inert',
    reason: 'inert: ' + [...new Set(inert)].join('; ') + (incomplete ? '; varied factor coverage unknown' : ''),
  };
  if (incomplete) return unknown;
  if (projected.length) return {
    status: 'projection_only',
    reason: 'consumption unproven; applicability unknown: ' + [...new Set(projected)].join('; '),
  };
  return { status: 'active', reason: 'factor coverage complete' };
}

function combineCoverage(coverages: FactorCoverage[]): FactorCoverage {
  for (const status of ['inert', 'unknown', 'projection_only', 'active'] as const) {
    const matching = coverages.filter(coverage => coverage.status === status);
    if (matching.length) return { status, reason: [...new Set(matching.map(coverage => coverage.reason))].join('; ') };
  }
  return { status: 'unknown', reason: 'varied factor coverage unknown' };
}

function nonNegativeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}



function hasCommitWindowProvenance(value: RecordValue): boolean {
  return value.scope === 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort' &&
    value.clock === 'collector_system_unix_ms_shared_schedule_clock_skew_not_measured' &&
    value.warmup_backlog === 'included_when_commit_finishes_in_window' &&
    value.rewrite_commits === 'excluded_by_ingestion_only_producer_capability' &&
    value.unique_delivered_rows === null && value.complete_delivery_established === false;
}

const maintenanceOperationContracts = {
  optimize: { sources: ['snapshot_active_files', 'bounded_physical_inventory'], unit: 'parquet_files' },
  purge: { sources: ['snapshot_active_files', 'bounded_physical_inventory'], unit: 'parquet_files' },
  vacuum: { sources: ['bounded_tombstone_replay'], unit: 'tombstones' },
  checkpoint: { sources: ['checkpoint_log_inventory'], unit: 'commits' },
  prune_log: { sources: ['bounded_log_inventory', 'verified_prune_proof'], unit: 'log_objects' },
} as const;
const maintenanceTable = 'telemetry.sqlserver.sqlserver_availability_replica_states';
type MaintenanceOperation = keyof typeof maintenanceOperationContracts;

export function maintenanceDueWorkIssue(
  processes: unknown,
  maintenanceConfig: unknown,
  start: number,
  end: number,
): string | undefined {
  const unavailable = 'complete five-operation maintenance due-work evidence is unavailable or invalid';
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end <= start) return unavailable;
  const operations = isRecord(maintenanceConfig) ? maintenanceConfig.operations : undefined;
  if (!isRecord(maintenanceConfig) || maintenanceConfig.enabled !== true || maintenanceConfig.apply !== true ||
      !Array.isArray(operations) ||
      operations.some(operation => typeof operation !== 'string' || !Object.hasOwn(maintenanceOperationContracts, operation)) ||
      new Set(operations).size !== operations.length ||
      Object.keys(maintenanceOperationContracts).some(operation => !operations.includes(operation))) return unavailable;
  if (!Array.isArray(processes)) return unavailable;
  const maintenanceProcesses = processes.filter((process): process is RecordValue =>
    isRecord(process) && process.component === 'maintenance');
  if (maintenanceProcesses.length !== 1) return unavailable;
  const process = maintenanceProcesses[0]!;
  const policy = process.maintenance_table_policies;
  if (process.log_complete !== true || !isRecord(policy) ||
      policy.source !== 'validated_consumer_per_table_policy' ||
      policy.operation_report_timing_scope !== 'process_lifetime_including_warmup_and_drain_not_shared_measurement_window' ||
      policy.operation_reports_dropped !== 0 || !Array.isArray(policy.operation_reports) ||
      !isRecord(policy.tables) || Object.keys(policy.tables).length !== 1 ||
      !isRecord(policy.tables[maintenanceTable]) || policy.tables[maintenanceTable].active !== true) return unavailable;
  const outcomes = policy.operation_outcomes;
  if (!isRecord(outcomes) || outcomes.scope !== 'process_lifetime' || outcomes.error_count !== 0) return unavailable;
  const retryCount = outcomes.retry_count;
  if (retryCount !== undefined && (nonNegativeInteger(retryCount) === undefined ||
      !isRecord(outcomes.retry_kind_counts) ||
      Object.entries(outcomes.retry_kind_counts).some(([kind, count]) => kind !== 'stale_plan' ||
        nonNegativeInteger(count) === undefined) ||
      Object.values(outcomes.retry_kind_counts).reduce<number>((sum, count) => sum + Number(count), 0) !== retryCount)) {
    return unavailable;
  }

  const windowCount = Math.floor((end - start) / 30_000);
  if (windowCount < 5) return unavailable;
  const observations = new Map<MaintenanceOperation, Array<Array<RecordValue>>>();
  let observedRetries = 0;
  for (const operation of Object.keys(maintenanceOperationContracts) as MaintenanceOperation[]) {
    observations.set(operation, Array.from({ length: windowCount }, () => []));
  }
  for (const report of policy.operation_reports) {
    if (!isRecord(report)) return unavailable;
    if (report.table !== maintenanceTable || typeof report.operation !== 'string' ||
        !Object.hasOwn(maintenanceOperationContracts, report.operation)) return unavailable;
    const operation = report.operation as MaintenanceOperation;
    if (report.operation_failed === true || report.interrupted === true || report.report_outcome === 'partial') {
      return 'maintenance operation failed or its due-work observation is incomplete';
    }
    if (report.operation_retry === true) {
      if (report.retry_kind !== 'stale_plan' || report.operation_failed !== false ||
          report.error_kind !== null || report.maintenance_due_work !== null || report.compaction_debt !== null ||
          !Number.isSafeInteger(retryCount) || Number(retryCount) < 1) return unavailable;
      observedRetries++;
      continue;
    }
    if (report.operation_retry !== undefined && report.operation_retry !== false ||
        report.retry_kind !== undefined && report.retry_kind !== null) return unavailable;
    if (report.operation_failed !== false ||
        report.interrupted !== undefined && typeof report.interrupted !== 'boolean' ||
        !['noop', 'planned', 'committed', 'already_applied', 'deleted', 'checkpointed', 'pruned'].includes(String(report.report_outcome))) {
      return unavailable;
    }
    const dueWork = report.maintenance_due_work;
    if (!isRecord(dueWork)) return unavailable;
    if (!Number.isSafeInteger(dueWork.observed_at_ms) || Number(dueWork.observed_at_ms) <= 0) {
      return unavailable;
    }
    if (dueWork.completeness !== 'complete' || dueWork.incomplete_reason !== null) {
      return 'maintenance operation failed or its due-work observation is incomplete';
    }
    const observedAt = Number(dueWork.observed_at_ms);
    if (observedAt < start || observedAt >= end) continue;
    const index = Math.floor((observedAt - start) / 30_000);
    if (index >= windowCount) continue;
    const contract = maintenanceOperationContracts[operation];
    const eligibleCount = nonNegativeInteger(dueWork.eligible_count);
    const backlogCount = dueWork.backlog_count === null ? null : nonNegativeInteger(dueWork.backlog_count);
    const eligibleBytes = dueWork.eligible_bytes === null ? null : nonNegativeInteger(dueWork.eligible_bytes);
    const backlogBytes = dueWork.backlog_bytes === null ? null : nonNegativeInteger(dueWork.backlog_bytes);
    const requiresBacklogCount = operation !== 'checkpoint';
    const requiresByteCounts = operation === 'optimize' || operation === 'purge';
    if (!contract.sources.some(source => source === dueWork.source) || dueWork.unit !== contract.unit ||
        typeof dueWork.due !== 'boolean' ||
        eligibleCount === undefined || dueWork.backlog_count !== null && backlogCount === undefined ||
        requiresBacklogCount && backlogCount === null ||
        backlogCount !== null && backlogCount !== undefined && eligibleCount !== undefined && backlogCount > eligibleCount ||
        eligibleCount === 0 && dueWork.due !== false ||
        dueWork.eligible_bytes !== null && eligibleBytes === undefined ||
        dueWork.backlog_bytes !== null && backlogBytes === undefined ||
        requiresByteCounts && (eligibleBytes === null || backlogBytes === null) ||
        backlogBytes !== null && backlogBytes !== undefined && eligibleBytes !== null && eligibleBytes !== undefined &&
          backlogBytes > eligibleBytes ||
        (eligibleBytes === null) !== (backlogBytes === null) ||
        eligibleCount === 0 && (eligibleBytes !== null && eligibleBytes !== 0 ||
          backlogCount !== null && backlogCount !== 0 || backlogBytes !== null && backlogBytes !== 0)) return unavailable;
    const oldest = dueWork.oldest_eligible_timestamp_ms;
    const basis = dueWork.oldest_timestamp_basis;
    if (oldest === undefined || basis === undefined ||
        oldest !== null && (!Number.isSafeInteger(oldest) || Number(oldest) < 0 || Number(oldest) > observedAt) ||
        basis !== null && !['ingestion_partition_start', 'delta_remove_timestamp', 'log_object_last_modified'].includes(String(basis)) ||
        (oldest === null) !== (basis === null) ||
        eligibleCount === 0 && oldest !== null) return unavailable;
    if (oldest !== null) {
      const expectedBasis = contract.unit === 'parquet_files' ? 'ingestion_partition_start' :
        contract.unit === 'tombstones' ? 'delta_remove_timestamp' : 'log_object_last_modified';
      if (basis !== expectedBasis) return unavailable;
    }
    const window = observations.get(operation)![index]!;
    if (window.some(sample => sample.observed_at_ms === observedAt)) return unavailable;
    let protectedFiles: number | null = null;
    let protectedBytes: number | null = null;
    if (operation === 'optimize' && isRecord(report.compaction_debt)) {
      const debt = report.compaction_debt;
      const protectedBacklog = debt.protected_partition_backlog;
      if (protectedBacklog !== null && protectedBacklog !== undefined) {
        if (!isRecord(protectedBacklog) || debt.observed_at_ms !== observedAt || debt.source !== dueWork.source ||
            nonNegativeInteger(protectedBacklog.files) === undefined ||
            nonNegativeInteger(protectedBacklog.bytes) === undefined ||
            protectedBacklog.oldest_partition_start_ms !== null &&
              (nonNegativeInteger(protectedBacklog.oldest_partition_start_ms) === undefined ||
                Number(protectedBacklog.oldest_partition_start_ms) > observedAt) ||
            protectedBacklog.files === 0 && (protectedBacklog.bytes !== 0 ||
              protectedBacklog.oldest_partition_start_ms !== null)) return unavailable;
        protectedFiles = Number(protectedBacklog.files);
        protectedBytes = Number(protectedBacklog.bytes);
      }
    }
    window.push({
      ...dueWork,
      _eligible_count: eligibleCount,
      _eligible_bytes: eligibleBytes,
      _backlog_count: backlogCount,
      _backlog_bytes: backlogBytes,
      _protected_files: protectedFiles,
      _protected_bytes: protectedBytes,
    });
  }
  if (retryCount !== undefined && observedRetries !== retryCount) return unavailable;

  for (const [operation, windows] of observations) {
    if (windows.some(samples => samples.length === 0)) return 'maintenance due-work is missing from one or more complete 30-second windows';
    const means = new Map<string, number[]>();
    const coverage = new Map<string, boolean>();
    const metrics = ['_eligible_count', '_backlog_count', '_eligible_bytes', '_backlog_bytes',
      ...(operation === 'optimize' ? ['_protected_files', '_protected_bytes'] : [])];
    for (const samples of windows) {
      if (samples.length === 0) return 'maintenance due-work is missing from one or more complete 30-second windows';
      for (const metric of metrics) {
        const values = samples.map(sample => sample[metric]);
        const hasNull = values.some(value => value === null);
        const measured = !hasNull;
        if (metric.startsWith('_protected_') && hasNull) return unavailable;
        if (hasNull && values.some(value => value !== null) ||
            coverage.has(metric) && coverage.get(metric) !== measured) {
          return 'maintenance due-work measurement coverage is inconsistent across the measured windows';
        }
        coverage.set(metric, measured);
        if (measured) {
          const metricMeans = means.get(metric) ?? [];
          metricMeans.push(average(values.map(Number)));
          means.set(metric, metricMeans);
        }
      }
    }
    const isGrowing = (values: number[]): boolean => {
      const center = (values.length - 1) / 2;
      const mean = average(values);
      const slope = values.reduce((sum, value, index) => sum + (index - center) * (value - mean), 0);
      return slope > 0 || values[values.length - 1]! > values[0]!;
    };
    if (metrics.some(metric => coverage.get(metric) && isGrowing(means.get(metric)!))) {
      return operation + ' maintenance due-work grows across the measured windows';
    }
  }
  const cellIssue = maintenanceCellOutcomeIssue(process.maintenance_operation_outcomes);
  if (cellIssue) return cellIssue;
  return undefined;
}

function maintenanceCellOutcomeIssue(value: unknown): string | undefined {
  const unavailable = 'cell-scoped maintenance retry/error records are unavailable or invalid';
  if (!isRecord(value) || value.status !== 'observed' || value.cell_scope !== 'stdout_stderr_append_offsets' ||
      !nonNegativeInteger(value.operation_count) || typeof value.operation_failure !== 'boolean' ||
      !isRecord(value.error_kind_counts) || !isRecord(value.retry_kind_counts) || !isRecord(value.operations)) return unavailable;
  const maps = [value.error_kind_counts, value.retry_kind_counts];
  if (maps.some(counts => Object.values(counts).some(count => nonNegativeInteger(count) === undefined)) ||
      Object.keys(value.retry_kind_counts).some(kind => kind !== 'stale_plan')) return unavailable;
  const failures = Object.values(value.error_kind_counts).reduce<number>((sum, count) => sum + Number(count), 0);
  let operations = 0;
  const retriesByKind: Record<string, number> = {};
  const errorsByKind: Record<string, number> = {};
  for (const [operation, counts] of Object.entries(value.operations)) {
    if (!Object.hasOwn(maintenanceOperationContracts, operation) || !isRecord(counts) ||
        !nonNegativeInteger(counts.operation_count) ||
        !isRecord(counts.retry_kind_counts) || !isRecord(counts.error_kind_counts)) return unavailable;
    operations += Number(counts.operation_count);
    for (const [source, target] of [
      [counts.retry_kind_counts, retriesByKind], [counts.error_kind_counts, errorsByKind],
    ] as const) {
      for (const [kind, count] of Object.entries(source)) {
        if (nonNegativeInteger(count) === undefined) return unavailable;
        target[kind] = (target[kind] ?? 0) + Number(count);
      }
    }
  }
  if (operations !== value.operation_count || stableJson(retriesByKind) !== stableJson(value.retry_kind_counts) ||
      stableJson(errorsByKind) !== stableJson(value.error_kind_counts)) return unavailable;
  const retries = Object.values(retriesByKind).reduce((sum, count) => sum + count, 0);
  if (failures + retries > operations) return unavailable;
  if (value.operation_failure !== (failures > 0)) return unavailable;
  if (failures > 0) return 'cell-scoped maintenance operation failure';
  return undefined;
}

function maintenanceShutdownIssue(value: unknown): string | undefined {
  if (!isRecord(value) || !Array.isArray(value.maintenance) || value.maintenance.length !== 1) {
    return 'maintenance shutdown outcome is unknown';
  }
  const process = value.maintenance[0];
  if (!isRecord(process)) return 'maintenance shutdown outcome is unknown';
  const issue = maintenanceCellOutcomeIssue(process.outcomes);
  if (issue) return issue;
  if (process.exit_classification !== 'no_operation_failures' ||
      nonNegativeInteger(process.exit_code) === undefined || ![0, 143].includes(Number(process.exit_code))) {
    return 'maintenance shutdown exit has no proved classified cause';
  }
  return undefined;
}

export function publicationBacklogIssue(value: unknown, start: number, end: number): string | undefined {
  const unavailable = 'publication backlog contract evidence is unavailable or invalid';
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end - start < 150_000 ||
      !isRecord(value) || value.schema_version !== 1 || value.source !== 'arrowcol_publication_backlog_*' ||
      value.scope !== 'unique_table_owner_admissions_not_confirmed_durable_commit' ||
      value.timestamp_basis !== 'UTC request start/end around direct /metrics GET; each is a scrape-time snapshot' ||
      !isRecord(value.measurement_window) || value.measurement_window.start_unix_ms !== start ||
      value.measurement_window.end_unix_ms !== end || value.sampling_failures !== 0 ||
      value.samples_dropped !== 0 || value.last_unavailable_reason !== null ||
      !Array.isArray(value.samples) || value.samples.length < 2 || value.samples.length > 10_000) return unavailable;
  const interval = nonNegativeInteger(value.sampling_interval_ms);
  const timeout = nonNegativeInteger(value.request_timeout_ms);
  if (!interval || interval > 30_000 || !timeout || timeout > 30_000) return unavailable;

  const samples: Array<{ start: number; end: number; gauges: number[] }> = [];
  for (const sample of value.samples) {
    if (!isRecord(sample) || sample.available !== true || sample.unavailable_reason !== null ||
        sample.status !== null && typeof sample.status !== 'string') return unavailable;
    const requestStart = nonNegativeInteger(sample.request_start_unix_ms);
    const requestEnd = nonNegativeInteger(sample.request_end_unix_ms);
    const requestBudget = nonNegativeInteger(sample.request_timeout_ms);
    const publications = nonNegativeInteger(sample.publications);
    const rows = nonNegativeInteger(sample.rows);
    const bytes = nonNegativeInteger(sample.logical_bytes);
    const age = sample.oldest_age_seconds;
    if (requestStart === undefined || requestEnd === undefined || !requestBudget ||
        requestBudget > timeout || requestBudget > end - requestStart || requestEnd < requestStart ||
        requestEnd - requestStart > requestBudget || requestStart < start || requestEnd > end ||
        publications === undefined || rows === undefined || bytes === undefined ||
        typeof age !== 'number' || !Number.isFinite(age) || age < 0) return unavailable;
    const previous = samples[samples.length - 1];
    if (previous && (requestStart <= previous.start || requestStart < previous.end ||
        requestStart - previous.start > interval + timeout)) return 'publication backlog samples are gapped or clock-reversed';
    samples.push({ start: requestStart, end: requestEnd, gauges: [publications, rows, bytes, age] });
  }
  if (samples[0]!.end > start + interval + timeout || samples[samples.length - 1]!.start < end - interval - timeout) {
    return 'publication backlog samples do not cover the measurement interval';
  }
  const means: number[][] = [];
  if (Math.floor((end - start) / 30_000) > samples.length / 2) {
    return 'publication backlog samples do not cover each complete commit window';
  }
  for (let bucketStart = start; bucketStart + 30_000 <= end; bucketStart += 30_000) {
    const bucket = samples.filter(sample => sample.start >= bucketStart && sample.end < bucketStart + 30_000);
    if (bucket.length < 2) return 'publication backlog samples do not cover each complete commit window';
    means.push([0, 1, 2, 3].map(index => average(bucket.map(sample => sample.gauges[index]!))));
  }
  for (let gauge = 0; gauge < 4; gauge++) {
    const values = means.map(mean => mean[gauge]!);
    const center = (values.length - 1) / 2;
    const mean = average(values);
    const slopeNumerator = values.reduce((sum, value, index) => sum + (index - center) * (value - mean), 0);
    if (slopeNumerator > 0 || values[values.length - 1]! > values[0]!) {
      return 'publication backlog has a positive pending publications/rows/bytes/age trend';
    }
  }
  return undefined;
}

const guardCapMetrics = ['container_memory_bytes', 'collector_rss_bytes',
  'collector_cgroup_memory_bytes', 'collector_buffered_bytes'];

function overloadStopIssue(evidence: RecordValue): string | undefined {
  const stop = evidence.overload_stop;
  if (stop === undefined || stop === null) return undefined;
  if (!isRecord(stop) || nonNegativeInteger(stop.timestamp_ms) === undefined ||
      nonNegativeInteger(stop.window_index) === undefined) return 'overload_stop evidence is invalid';
  const hardCap = stop.reason === 'hard_cap_fraction' && guardCapMetrics.includes(String(stop.metric)) &&
    nonNegativeInteger(stop.observed_value) !== undefined && nonNegativeInteger(stop.cap) !== undefined &&
    Number(stop.cap) > 0 && typeof stop.fraction === 'number' && Number.isFinite(stop.fraction) &&
    stop.fraction === Number(stop.observed_value) / Number(stop.cap);
  const stalled = stop.reason === 'zero_ingestion_commit_progress' && stop.metric === 'new_ingestion_commits' &&
    stop.observed_value === 0 && stop.cap === null && stop.fraction === null;
  if (!hardCap && !stalled) return 'overload_stop evidence is invalid';
  return `above_ceiling: overload_stop ${String(stop.reason)} (${String(stop.metric)})`;
}

function overloadGuardIssue(evidence: RecordValue, config: RecordValue, runId: unknown, start: number, end: number): string | undefined {
  const invalid = 'overload_guard protection unavailable or invalid';
  if (config.overload_guard !== undefined && !isRecord(config.overload_guard)) return invalid;
  const requested = isRecord(config.overload_guard) ? config.overload_guard : {};
  const enabled = requested.enabled === undefined ? true : requested.enabled;
  const fraction = requested.cap_fraction === undefined ? 0.9 : requested.cap_fraction;
  const zeroWindows = requested.zero_progress_windows === undefined ? 2 : requested.zero_progress_windows;
  const memorySignal = requested.memory_signal === undefined ? 'process_rss' : requested.memory_signal;
  if (memorySignal !== 'process_rss' && memorySignal !== 'cgroup_memory') return invalid;
  const metrics = ['container_memory_bytes', memorySignal === 'cgroup_memory' ?
    'collector_cgroup_memory_bytes' : 'collector_rss_bytes', 'collector_buffered_bytes'];
  const warmup = nonNegativeInteger(config.warmup_seconds);
  if (enabled !== true || typeof fraction !== 'number' || !Number.isFinite(fraction) || fraction <= 0 || fraction > 0.9 ||
      nonNegativeInteger(zeroWindows) === undefined || Number(zeroWindows) < 1 || Number(zeroWindows) > 2 ||
      warmup === undefined || start < warmup * 1000) return invalid;
  const guard = evidence.overload_guard;
  if (!isRecord(guard)) return invalid;
  const caps = guard.last_caps;
  const platform = isRecord(config.platform) ? config.platform : undefined;
  const local = platform && isRecord(platform.local) ? platform.local : undefined;
  const requestedCap = local?.collector_memory_limit_bytes;
  const tunables = isRecord(config.tunables) ? config.tunables : undefined;
  const collector = tunables && isRecord(tunables.collector) ? tunables.collector : undefined;
  const pressure = collector && isRecord(collector.pressure) ? collector.pressure : undefined;
  const requestedRssCap = pressure?.rss_limit_bytes;
  if (memorySignal === 'cgroup_memory' &&
      (nonNegativeInteger(requestedRssCap) === undefined || Number(requestedRssCap) === 0)) return invalid;
  if (requestedCap !== undefined && requestedCap !== null &&
      (nonNegativeInteger(requestedCap) === undefined || Number(requestedCap) < 8388608 || Number(requestedCap) > 1099511627776)) return invalid;
  if (guard.requested_container_memory_limit_bytes !== (requestedCap ?? null)) return invalid;
  const expectedStart = start - warmup * 1000;
  if (guard.status !== 'complete' || guard.run_id !== runId ||
      guard.run_start_ms !== expectedStart || guard.measurement_end_ms !== end ||
      !isRecord(guard.settings) || guard.settings.enabled !== true || guard.settings.cap_fraction !== fraction ||
      guard.settings.zero_progress_windows !== zeroWindows ||
      (guard.settings.memory_signal ?? 'process_rss') !== memorySignal ||
      nonNegativeInteger(guard.coverage_start_ms) === undefined || Number(guard.coverage_start_ms) > expectedStart ||
      nonNegativeInteger(guard.coverage_end_ms) === undefined || Number(guard.coverage_end_ms) < end ||
      nonNegativeInteger(guard.max_sampling_gap_ms) === undefined || Number(guard.max_sampling_gap_ms) > 5000 ||
      nonNegativeInteger(guard.sample_count) === undefined || Number(guard.sample_count) < Math.ceil((end - expectedStart) / 5000) + 1 ||
      typeof guard.peak_cap_fraction !== 'number' || !Number.isFinite(guard.peak_cap_fraction) ||
      guard.peak_cap_fraction < 0 || guard.peak_cap_fraction >= fraction ||
      !Array.isArray(caps) || caps.length !== metrics.length ||
      requestedCap != null && !caps.some(cap => isRecord(cap) && cap.metric === 'container_memory_bytes' && cap.cap === requestedCap) ||
      memorySignal === 'cgroup_memory' && !caps.some(cap => isRecord(cap) && cap.metric === 'collector_cgroup_memory_bytes' &&
        cap.cap === requestedRssCap && typeof cap.observed_value === 'number' && cap.observed_value > 0) ||
      !metrics.every(metric =>
        caps.filter(cap => isRecord(cap) && cap.metric === metric && cap.confirmed === true &&
          nonNegativeInteger(cap.observed_value) !== undefined && nonNegativeInteger(cap.cap) !== undefined &&
          Number(cap.cap) > 0 && Number(cap.observed_value) / Number(cap.cap) < fraction).length === 1) ||
      !Array.isArray(guard.complete_windows) || guard.complete_windows.length !== Math.floor((end - expectedStart) / 30_000)) return invalid;
  let zeroStreak = 0;
  for (const [index, window] of guard.complete_windows.entries()) {
    if (!isRecord(window) || window.index !== index || window.start_ms !== expectedStart + index * 30_000 ||
        window.end_ms !== expectedStart + (index + 1) * 30_000 || window.covered !== true ||
        typeof window.outstanding !== 'boolean' || nonNegativeInteger(window.ingestion_commits) === undefined) return invalid;
    zeroStreak = window.outstanding && window.ingestion_commits === 0 ? zeroStreak + 1 : 0;
    if (zeroStreak >= Number(zeroWindows)) return invalid;
  }
  return undefined;
}

function officialOverloadStop(run: OfficialRun): string | undefined {
  for (const row of run.rows) {
    if (typeof row.evidence_json !== 'string') continue;
    let evidence: unknown;
    try { evidence = JSON.parse(row.evidence_json); } catch { continue; }
    if (isRecord(evidence)) {
      const reason = overloadStopIssue(evidence);
      if (reason) return reason;
    }
  }
  return undefined;
}

function inspectOfficialRun(run: OfficialRun): { windows?: OfficialWindow[]; reason?: string } {
  const stop = officialOverloadStop(run);
  if (stop) return { reason: stop };
  if (!officialImageIds(run)) return { reason: 'official collector image evidence is missing' };
  const verdict = evaluateDataFlow(run.status, run.rows, run.expectedSha, run.mode, run.config, 'single_hot_table_campaign');
  if (verdict.ingestPass !== true || verdict.rows.length !== 1) return { reason: 'official campaign ingest evidence did not pass: ' + verdict.reasons.join('; ') };
  const row = verdict.rows[0]!;
  if (typeof row.evidence_json !== 'string') return { reason: 'official evidence_json is missing' };
  let evidence: unknown;
  try { evidence = JSON.parse(row.evidence_json); } catch { return { reason: 'official evidence_json is invalid' }; }
  if (!isRecord(evidence) || evidence.version !== 1 || !isRecord(evidence.durable_commits)) {
    return { reason: 'official durable commit evidence is missing' };
  }
  const durable = evidence.durable_commits;
  const output = durable.output_throughput;
  const buckets = durable.complete_30_second_commit_windows;
  if (!isRecord(output) || !hasCommitWindowProvenance(output) || !Array.isArray(buckets)) return { reason: 'official complete 30-second commit buckets are missing or invalid' };
  const start = nonNegativeInteger(output.measurement_start_unix_ms);
  const end = nonNegativeInteger(output.measurement_end_unix_ms);
  const outputRows = nonNegativeInteger(output.rows);
  const outputRate = output.rows_per_second;
  const durationMs = start === undefined || end === undefined ? 0 : end - start;
  const seconds = durationMs / 1000;
  if (start === undefined || end === undefined || durationMs < 150_000 || output.window_seconds !== seconds ||
      outputRows === undefined || typeof outputRate !== 'number' || !Number.isFinite(outputRate) || outputRate <= 0 ||
      Math.abs(outputRate - outputRows / seconds) > 1e-9) {
    return { reason: 'official run must contain a valid sustained measurement interval of at least 150 seconds' };
  }
  const publishedOutput = evidence.commit_window_output_throughput;
  const guardIssue = overloadGuardIssue(evidence, run.config, row.run_id, start, end);
  if (guardIssue) return { reason: guardIssue };
  if (!isRecord(publishedOutput) || stableJson(publishedOutput) !== stableJson(output)) {
    return { reason: 'top-level and durable commit-window evidence disagree' };
  }
  const expectedBucketCount = Math.floor(durationMs / 30_000);
  if (expectedBucketCount < 5 || buckets.length !== expectedBucketCount) {
    return { reason: 'one official run must contain every complete 30-second commit bucket' };
  }
  const windows: OfficialWindow[] = [];
  let bucketRowsTotal = 0;
  for (const [index, rawBucket] of buckets.entries()) {
    if (!isRecord(rawBucket) || !hasCommitWindowProvenance(rawBucket)) return { reason: 'official commit bucket is invalid' };
    const bucketStart = nonNegativeInteger(rawBucket.measurement_start_unix_ms);
    const bucketEnd = nonNegativeInteger(rawBucket.measurement_end_unix_ms);
    const bucketRows = nonNegativeInteger(rawBucket.rows);
    const bucketRate = rawBucket.rows_per_second;
    const expectedStart = start + index * 30_000;
    if (bucketStart !== expectedStart || bucketEnd !== expectedStart + 30_000 || rawBucket.window_seconds !== 30 ||
        bucketRows === undefined || bucketRows === 0 || typeof bucketRate !== 'number' || !Number.isFinite(bucketRate) ||
        bucketRate <= 0 || Math.abs(bucketRate - bucketRows / 30) > 1e-9) {
      return { reason: 'official commit buckets must be positive, complete, contiguous 30-second windows' };
    }
    bucketRowsTotal += bucketRows;
    windows.push({ rate: bucketRate });
  }
  if (bucketRowsTotal > outputRows || (durationMs % 30_000 === 0 && bucketRowsTotal !== outputRows)) {
    return { reason: 'complete commit buckets do not reconcile to the exact-window run output total' };
  }
  const reconciliation = evidence.reconciliation;
  if (!isRecord(reconciliation) || reconciliation.status !== 'within_bound' || reconciliation.settled !== true ||
      reconciliation.lower_bound_met !== true || reconciliation.upper_bound_met !== true ||
      typeof reconciliation.exact_match_required !== 'boolean' || typeof reconciliation.exact_match !== 'boolean') {
    return { reason: 'official source/new-commit reconciliation is incomplete or unsettled' };
  }
  const deltaRows = nonNegativeInteger(reconciliation.delta_rows);
  const completedRows = nonNegativeInteger(reconciliation.completed_rows);
  const ambiguousRows = nonNegativeInteger(reconciliation.ambiguous_rows);
  const minimumRows = nonNegativeInteger(reconciliation.minimum_delta_rows);
  const maximumRows = nonNegativeInteger(reconciliation.maximum_delta_rows);
  if ([deltaRows, completedRows, ambiguousRows, minimumRows, maximumRows].some(value => value === undefined) ||
      minimumRows !== completedRows || maximumRows !== completedRows! + ambiguousRows! ||
      reconciliation.lower_bound_met !== (completedRows! <= deltaRows!) ||
      reconciliation.upper_bound_met !== (deltaRows! <= maximumRows!) ||
      reconciliation.exact_match_required !== (ambiguousRows === 0) ||
      reconciliation.exact_match !== (deltaRows === completedRows)) {
    return { reason: 'official source/new-commit reconciliation bounds are inconsistent' };
  }
  const tunables = run.config.tunables;
  const maintenanceConfig = isRecord(tunables) ? tunables.maintenance : undefined;
  const maintenanceIssue = maintenanceDueWorkIssue(evidence.processes, maintenanceConfig, start, end);
  const shutdownIssue = maintenanceShutdownIssue(evidence.shutdown_drain);
  const backlogIssue = publicationBacklogIssue(evidence.backlog, start, end);
  return {
    windows,
    reason: [
      ...(maintenanceIssue ? [maintenanceIssue] : []),
      ...(shutdownIssue ? [shutdownIssue] : []),
      ...(backlogIssue ? [backlogIssue] : []),
    ].join('; '),
  };
}

function assessRepeat(row: CampaignMeasurement): RepeatAssessment {
  const overloadStop = row.officialRuns?.map(officialOverloadStop).find(reason => reason !== undefined);
  if (overloadStop) return { qualified: false, rates: [], reason: overloadStop };
  if (row.diedFirst) return { qualified: false, rates: [], reason: row.collectorOnly ? 'collector-only downstream failure' : 'a component died during the run' };
  if (!row.expectedRepeats || !row.factorConfigPaths || !row.officialRuns || row.officialRuns.length !== 1) {
    return { qualified: false, rates: [], reason: 'each repeat must contain exactly one official sustained run' };
  }
  const inspected = inspectOfficialRun(row.officialRuns[0]!);
  if (!inspected.windows) return { qualified: false, rates: [], reason: inspected.reason ?? 'invalid official run' };
  const rates = inspected.windows.map(window => window.rate);
  const mean = average(rates);
  if ((Math.max(...rates) - Math.min(...rates)) / mean > 0.05) {
    return { qualified: false, rates, reason: 'official window range exceeds 5% of the mean' };
  }
  if (inspected.reason) return { qualified: false, rates, reason: inspected.reason };
  return { qualified: true, rates, reason: 'protocol evidence complete' };
}

function summarizeCandidates(rows: CampaignMeasurement[], cells: Map<string, Cell>): Summary[] {
  const groups = new Map<string, CampaignMeasurement[]>();
  for (const row of rows) {
    const group = groups.get(row.candidate) ?? [];
    group.push(row);
    groups.set(row.candidate, group);
  }
  return [...groups].map(([candidate, group]) => {
    const expectedRepeats = group[0]!.expectedRepeats ?? null;
    const assessments = group.map(row => ({ row, evidence: assessRepeat(row) }));
    const completed = assessments.filter(item => item.evidence.qualified);
    const repeatIds = new Set(group.map(row => row.repeat));
    const completeSet = expectedRepeats !== null && repeatIds.size === expectedRepeats &&
      Array.from({ length: expectedRepeats }, (_, index) => repeatIds.has(index + 1)).every(Boolean);
    const coverage = cells.get(JSON.stringify([group[0]!.valueX, group[0]!.valueY]))!.coverage;
    const qualified = coverage.status === 'active' && expectedRepeats !== null && expectedRepeats >= 3 && completeSet && completed.length === expectedRepeats;
    const officialRepeatRates = completed.map(item => average(item.evidence.rates));
    const observedRates = coverage.status !== 'active' ? [] :
      group.flatMap(row => row.committedRowsPerSecond === null ? [] : [row.committedRowsPerSecond]);
    const rates = qualified ? officialRepeatRates : observedRates;
    const deaths = group.map(row => row.diedFirst).filter((value): value is string => value !== null && value !== undefined);
    const status = qualified ? 'qualified' : deaths.some(value => value.toLowerCase() !== 'kustainer') ? 'failed' :
      deaths.length || group.some(row => row.collectorOnly) ? 'collector-only' : 'unqualified';
    return {
      candidate, rates, mean: rates.length ? average(rates) : 0,
      min: rates.length ? Math.min(...rates) : 0,
      max: rates.length ? Math.max(...rates) : 0,
      completedRepeats: completed.length, expectedRepeats, status,
      reason: qualified ? 'protocol evidence complete' : [
        ...(coverage.status === 'active' ? [] : [coverage.reason]),
        assessments.find(item => !item.evidence.qualified)?.evidence.reason ?? 'protocol evidence incomplete',
      ].join('; '),
    };
  });
}

export function sortCampaignLeaderboard<T extends { candidate: string; mean: number }>(items: T[]): T[] {
  return [...items].sort((a, b) => b.mean - a.mean || (a.candidate < b.candidate ? -1 : a.candidate > b.candidate ? 1 : 0));
}

export function renderCampaignResults(input: unknown, cohort: string): string {
  const allRows = validateCampaignMeasurements(input);
  const rows = allRows.filter(row => row.cohort === cohort);
  if (rows.length === 0) throw new Error('no results for cohort ' + cohort);
  const factorX = rows[0]!.factorX;
  const factorY = rows[0]!.factorY;
  if (rows.some(row => row.factorX !== factorX || row.factorY !== factorY)) {
    throw new Error('a cohort picture must use exactly one pair of factors');
  }
  assertCompatibleBaselines(rows, cohort);
  const xValues = [...new Set(rows.map(row => row.valueX))].sort();
  const yValues = [...new Set(rows.map(row => row.valueY))].sort();
  const factorPairs = new Set(rows.flatMap(row => {
    if (!row.factorConfigPaths) return [];
    const paths = row.factorConfigPaths.map(canonicalFactorPath);
    return paths.every(path => path !== undefined) ? [JSON.stringify(paths)] : [];
  }));
  const cells = new Map<string, Cell>();
  for (const row of rows) {
    const key = JSON.stringify([row.valueX, row.valueY]);
    const coverage = combineCoverage([
      factorCoverage(row),
      ...(factorPairs.size > 1 ?
        [{ status: 'unknown' as const, reason: 'varied factor coverage unknown: inconsistent factor paths in cohort' }] : []),
    ]);
    const cell = cells.get(key) ?? { x: row.valueX, y: row.valueY, rates: [], coverage };
    cell.coverage = combineCoverage([cell.coverage, coverage]);
    if (row.committedRowsPerSecond !== null) cell.rates.push(row.committedRowsPerSecond);
    cells.set(key, cell);
  }
  const rates = [...cells.values()].flatMap(cell =>
    cell.coverage.status !== 'active' ? [] : cell.rates);
  const minimum = rates.length ? Math.min(...rates) : 0;
  const maximum = rates.length ? Math.max(...rates) : 0;
  const summaries = summarizeCandidates(rows, cells);
  const leaderboard = sortCampaignLeaderboard(summaries.filter(item => item.status === 'qualified'));
  const exploratory = summaries.filter(item => item.status !== 'qualified');
  const maxMean = Math.max(1, ...leaderboard.map(item => item.mean));
  const deaths = rows.filter(row => row.diedFirst);
  const width = 1220;
  const heatX = 45;
  const heatY = 130;
  const cellWidth = Math.max(76, Math.min(150, 500 / Math.max(1, xValues.length)));
  const cellHeight = 42;
  const leaderboardX = 610;
  const noteY = Math.max(heatY + yValues.length * cellHeight + 70, 145 + leaderboard.length * 38);
  const exploratoryY = noteY + 45 + Math.min(deaths.length, 8) * 23;
  const height = Math.max(540, 145 + leaderboard.length * 38, exploratoryY + 45 + exploratory.length * 24);
  const parts: string[] = [
    '<svg xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="title desc" width="' + width + '" height="' + height + '" viewBox="0 0 ' + width + ' ' + height + '">',
    '<title id="title">' + escapeXml(cohort + ' throughput heatmap and sustainable leaderboard') + '</title>',
    '<desc id="desc">Committed new rows per second; repeat ranges are ranked only with complete official campaign evidence. Projection-only factors render null until independent applicability proof is available. Null rates are not plotted as zero.</desc>',
    '<rect width="100%" height="100%" fill="#ffffff"/>',
    '<g fill="#1f2328" font-family="Segoe UI, Arial, sans-serif">',
    svgText(40, 42, cohort + ' | committed new rows/s', 22, 'bold'),
    svgText(40, 76, factorX + ' × ' + factorY + ' heatmap', 16, 'bold'),
    svgText(leaderboardX, 76, 'Sustainable leaderboard | official evidence only', 16, 'bold'),
    svgText(heatX, heatY - 18, factorX + ' →', 12, 'bold'),
  ];
  xValues.forEach((value, index) => parts.push(svgText(heatX + 126 + index * cellWidth, heatY - 18, value, 11)));
  yValues.forEach((value, rowIndex) => {
    const y = heatY + rowIndex * cellHeight;
    parts.push(svgText(heatX, y + 25, value, 11));
    xValues.forEach((x, columnIndex) => {
      const cell = cells.get(JSON.stringify([x, value]));
      const xPosition = heatX + 120 + columnIndex * cellWidth;
      if (!cell || !cell.rates.length || cell.coverage.status !== 'active') {
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="#f6f8fa" stroke="#d0d7de"/>');
        parts.push(svgText(xPosition + 8, y + 22, 'null', 11));
      } else {
        const mean = average(cell.rates);
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="' + cellColor(mean, minimum, maximum) + '" stroke="#8c959f"/>');
        parts.push(svgText(xPosition + 8, y + 22, numberLabel(mean), 11, 'bold'));
      }
    });
  });
  parts.push(svgText(heatX, heatY + yValues.length * cellHeight + 20, factorY + ' ↓ | null includes projection-only: consumption unproven; applicability unknown', 11));
  const barStart = leaderboardX + 190;
  const barWidth = 300;
  if (!leaderboard.length) parts.push(svgText(leaderboardX, 112, 'No qualified winner: complete official evidence is missing or invalid.', 12, 'bold'));
  leaderboard.forEach((item, index) => {
    const y = 112 + index * 38;
    parts.push(svgText(leaderboardX, y + 13, item.candidate + ' (' + item.completedRepeats + '/' + item.expectedRepeats + ')', 12));
    const meanWidth = item.mean / maxMean * barWidth;
    const minX = barStart + item.min / maxMean * barWidth;
    const maxX = barStart + item.max / maxMean * barWidth;
    parts.push('<rect x="' + barStart + '" y="' + y + '" width="' + meanWidth + '" height="18" rx="3" fill="#218bff" opacity="0.75"/>');
    parts.push('<line x1="' + minX + '" y1="' + (y + 9) + '" x2="' + maxX + '" y2="' + (y + 9) + '" stroke="#1f2328" stroke-width="2"/>');
    parts.push('<line x1="' + minX + '" y1="' + (y + 4) + '" x2="' + minX + '" y2="' + (y + 14) + '" stroke="#1f2328"/>');
    parts.push('<line x1="' + maxX + '" y1="' + (y + 4) + '" x2="' + maxX + '" y2="' + (y + 14) + '" stroke="#1f2328"/>');
    parts.push(svgText(barStart + barWidth + 12, y + 13, numberLabel(item.mean) + ' (' + numberLabel(item.min) + '–' + numberLabel(item.max) + ')', 11));
  });
  parts.push(svgText(40, noteY, 'What died first', 16, 'bold'));
  if (!deaths.length) parts.push(svgText(40, noteY + 25, 'No failure recorded in these rows.', 12));
  deaths.slice(0, 8).forEach((row, index) => {
    const coverage = cells.get(JSON.stringify([row.valueX, row.valueY]))!.coverage;
    const rate = row.committedRowsPerSecond === null || coverage.status !== 'active' ?
      'committed rows/s: null' : 'committed rows/s: ' + numberLabel(row.committedRowsPerSecond);
    const status = row.collectorOnly ? 'collector-only; downstream metrics: null; ' : '';
    const comment = row.comment ? ' — ' + row.comment : '';
    parts.push(svgText(40, noteY + 25 + index * 23, row.candidate + ' repeat ' + row.repeat + ': ' + row.diedFirst + ' (' + status + rate + ')' + comment, 11));
  });
  parts.push(svgText(40, exploratoryY, 'Exploratory / failed observations (not ranked)', 16, 'bold'));
  if (!exploratory.length) parts.push(svgText(40, exploratoryY + 25, 'All observed candidates have complete sustainable evidence.', 12));
  exploratory.forEach((item, index) => {
    const expected = item.expectedRepeats === null ? '?' : String(item.expectedRepeats);
    const observed = item.rates.length ? 'observed mean ' + numberLabel(item.mean) + ' rows/s' : 'no committed-rate observation';
    parts.push(svgText(40, exploratoryY + 25 + index * 24,
      item.candidate + ': ' + item.status + ', ' + item.completedRepeats + '/' + expected + ' protocol-complete repeats; ' + observed + ' (' + item.reason + ')', 11));
  });
  parts.push('</g></svg>');
  return parts.join('\n');
}

async function main(args: string[]): Promise<void> {
  let inputPath: string | undefined;
  let outputPath: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const flag = args[index];
    if (flag === '--input') inputPath = args[++index];
    else if (flag === '--out') outputPath = args[++index];
    else throw new Error('unknown option: ' + flag);
    if (!args[index] || args[index].startsWith('--')) throw new Error('expected a value after ' + flag);
  }
  if (!inputPath || !outputPath) throw new Error('usage: campaign-results --input <results.json> --out <directory>');
  const rows = validateCampaignMeasurements(JSON.parse(await readFile(inputPath, 'utf8')));
  await mkdir(outputPath, { recursive: true });
  for (const cohort of [...new Set(rows.map(row => row.cohort))]) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/.test(cohort)) throw new Error('unsafe cohort name: ' + cohort);
    await writeFile(resolve(outputPath, cohort + '.svg'), renderCampaignResults(rows, cohort), 'utf8');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
