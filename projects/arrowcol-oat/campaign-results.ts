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
type Cell = { x: string; y: string; rates: number[] };
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
    status: { id: status.id, state, progress: status.progress, result_count: Number(status.result_count), error: status.error },
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
  let mode: string | undefined;
  for (const row of rows) {
    if (!row.officialRuns?.length) continue;
    if (!row.factorConfigPaths) throw new Error('official evidence requires factorConfigPaths to compare frozen baselines');
    for (const run of row.officialRuns) {
      if (factorPathsOverlap(run.config, row.factorConfigPaths[0], row.factorConfigPaths[1]) ||
          !resolveFactorLeaf(run.config, row.factorConfigPaths[0]) || !resolveFactorLeaf(run.config, row.factorConfigPaths[1])) {
        throw new Error('cohort ' + cohort + ' factorConfigPaths must identify distinct scalar or array config leaves');
      }
      const config = structuredClone(run.config);
      if (!removeFactorPath(config, row.factorConfigPaths[0]) || !removeFactorPath(config, row.factorConfigPaths[1])) {
        throw new Error('cohort ' + cohort + ' factorConfigPaths do not resolve in official config');
      }
      const fingerprint = stableJson({ config, collectorImages: officialImageIds(run) });
      if (source !== undefined && (source !== run.expectedSha || configFingerprint !== fingerprint || mode !== run.mode)) {
        throw new Error('cohort ' + cohort + ' has incompatible frozen source/resource/load baselines');
      }
      source = run.expectedSha;
      configFingerprint = fingerprint;
      mode = run.mode;
    }
  }
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

function hasInWindowCompactionObservation(processes: unknown, start: number, end: number): boolean {
  if (!Array.isArray(processes)) return false;
  const maintenanceProcesses = processes.filter((process): process is RecordValue =>
    isRecord(process) && process.component === 'maintenance');
  if (maintenanceProcesses.length === 0) return false;

  for (const process of maintenanceProcesses) {
    const policy = process.maintenance_table_policies;
    if (process.log_complete !== true || !isRecord(policy) ||
        policy.source !== 'validated_consumer_per_table_policy' ||
        policy.operation_report_timing_scope !== 'process_lifetime_including_warmup_and_drain_not_shared_measurement_window' ||
        policy.operation_reports_dropped !== 0 || !Array.isArray(policy.operation_reports) ||
        !isRecord(policy.tables) || !isRecord(policy.tables.sqlserver_availability_replica_states) ||
        policy.tables.sqlserver_availability_replica_states.active !== true) return false;

    const outcomes = policy.operation_outcomes;
    if (!isRecord(outcomes) || outcomes.scope !== 'process_lifetime' || outcomes.error_count !== 0) return false;
    let samples = 0;
    for (const report of policy.operation_reports) {
      if (!isRecord(report)) return false;
      if (report.table !== 'sqlserver_availability_replica_states' || report.operation !== 'optimize') continue;
      const debt = report.compaction_debt;
      if (!isRecord(debt)) return false;
      const observedAt = debt.observed_at_ms;
      if (typeof observedAt !== 'number' || !Number.isSafeInteger(observedAt) || observedAt <= 0) return false;
      if (observedAt < start || observedAt >= end) continue;
      if (report.operation_failed !== false || typeof report.negative_probe !== 'boolean' ||
          report.mutation_mode !== 'apply_authorized_not_proof_of_mutation' ||
          (debt.source !== 'snapshot_active_files' && debt.source !== 'bounded_physical_inventory') ||
          !Number.isSafeInteger(debt.eligible_files) || Number(debt.eligible_files) < 0 ||
          !Number.isSafeInteger(debt.eligible_bytes) || Number(debt.eligible_bytes) < 0 ||
          !Number.isSafeInteger(debt.backlog_files) || Number(debt.backlog_files) < 0 ||
          !Number.isSafeInteger(debt.backlog_bytes) || Number(debt.backlog_bytes) < 0 ||
          Number(debt.backlog_files) > Number(debt.eligible_files) ||
          Number(debt.backlog_bytes) > Number(debt.eligible_bytes) ||
          debt.oldest_eligible_partition_start_ms !== null &&
            (!Number.isSafeInteger(debt.oldest_eligible_partition_start_ms) ||
              Number(debt.oldest_eligible_partition_start_ms) > observedAt)) return false;
      if (report.negative_probe && (debt.source !== 'bounded_physical_inventory' ||
          debt.eligible_files !== 0 || debt.eligible_bytes !== 0 || debt.backlog_files !== 0 ||
          debt.backlog_bytes !== 0 || debt.oldest_eligible_partition_start_ms !== null)) return false;
      samples++;
    }
    if (samples === 0) return false;
  }
  return true;
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

function inspectOfficialRun(run: OfficialRun): { windows?: OfficialWindow[]; reason?: string } {
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
  const compactionObserved = hasInWindowCompactionObservation(evidence.processes, start, end);
  const backlogIssue = publicationBacklogIssue(evidence.backlog, start, end);
  return {
    windows,
    reason: [
      ...(!compactionObserved ? ['timestamped in-window compaction-debt observation is unavailable or invalid'] : []),
      ...(backlogIssue ? [backlogIssue] : []),
      'backlog cross-language fixture check pending; sustained compaction debt and other maintenance due-work vectors remain unknown',
    ].join('; '),
  };
}

function assessRepeat(row: CampaignMeasurement): RepeatAssessment {
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

function summarizeCandidates(rows: CampaignMeasurement[]): Summary[] {
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
    const qualified = expectedRepeats !== null && expectedRepeats >= 3 && completeSet && completed.length === expectedRepeats;
    const officialRepeatRates = completed.map(item => average(item.evidence.rates));
    const observedRates = group.flatMap(row => row.committedRowsPerSecond === null ? [] : [row.committedRowsPerSecond]);
    const rates = qualified ? officialRepeatRates : observedRates;
    const deaths = group.map(row => row.diedFirst).filter((value): value is string => value !== null && value !== undefined);
    const status = qualified ? 'qualified' : deaths.some(value => value.toLowerCase() !== 'kustainer') ? 'failed' :
      deaths.length || group.some(row => row.collectorOnly) ? 'collector-only' : 'unqualified';
    return {
      candidate, rates, mean: rates.length ? average(rates) : 0,
      min: rates.length ? Math.min(...rates) : 0,
      max: rates.length ? Math.max(...rates) : 0,
      completedRepeats: completed.length, expectedRepeats, status,
      reason: qualified ? 'protocol evidence complete' : assessments.find(item => !item.evidence.qualified)?.evidence.reason ?? 'protocol evidence incomplete',
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
  const cells = new Map<string, Cell>();
  for (const row of rows) {
    const key = JSON.stringify([row.valueX, row.valueY]);
    const cell = cells.get(key) ?? { x: row.valueX, y: row.valueY, rates: [] };
    if (row.committedRowsPerSecond !== null) cell.rates.push(row.committedRowsPerSecond);
    cells.set(key, cell);
  }
  const rates = rows.flatMap(row => row.committedRowsPerSecond === null ? [] : [row.committedRowsPerSecond]);
  const minimum = rates.length ? Math.min(...rates) : 0;
  const maximum = rates.length ? Math.max(...rates) : 0;
  const summaries = summarizeCandidates(rows);
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
    '<desc id="desc">Committed new rows per second; repeat ranges are ranked only with complete official campaign evidence. Unqualified rates remain exploratory and null rates are not plotted as zero.</desc>',
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
      if (!cell || !cell.rates.length) {
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="#f6f8fa" stroke="#d0d7de"/>');
        parts.push(svgText(xPosition + 8, y + 22, 'null', 11));
      } else {
        const mean = average(cell.rates);
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="' + cellColor(mean, minimum, maximum) + '" stroke="#8c959f"/>');
        parts.push(svgText(xPosition + 8, y + 22, numberLabel(mean), 11, 'bold'));
      }
    });
  });
  parts.push(svgText(heatX, heatY + yValues.length * cellHeight + 20, factorY + ' ↓ | observed repeat means; null = unavailable', 11));
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
    const rate = row.committedRowsPerSecond === null ? 'committed rows/s: null' : 'committed rows/s: ' + numberLabel(row.committedRowsPerSecond);
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
