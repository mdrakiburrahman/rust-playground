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
  baselineBefore?: OfficialRun;
  baselineAfter?: OfficialRun;
  pairOrder?: 'AB' | 'BA';
  evidenceState?: 'configured' | 'projected' | 'consumer-verified' | 'physically-observed' | 'measured';
  disposition?: string;
  outcome?: string;
  status?: string;
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
type Cell = { x: string; y: string; rates: number[]; coverage: FactorCoverage; failures: string[]; notes: string[] };
type Summary = {
  candidate: string;
  rates: number[];
  mean: number;
  normalizedMean: number | null;
  min: number;
  max: number;
  completedRepeats: number;
  expectedRepeats: number | null;
  status: 'qualified' | 'collector-only' | 'failed' | 'unqualified';
  reason: string;
};
type OfficialWindow = { rate: number; resolution: number | null };
type RepeatAssessment = { qualified: boolean; rates: number[]; reason: string; ratio?: number; pairRatio?: number; candidateBytes?: number; baselineBytes?: number; pairedBaselineRate?: number };
type RankedObservation = {
  rate: number; resolution: number; bytesPerRow: number | null;
  commitCount: number; maxCommitRows: number; slope: number | null;
};

function isRecord(value: unknown): value is RecordValue {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function parseLocalCollectorCpuQuota(value: unknown, hostCpus?: number): {
  source: 'platform.local.collector_cpu_quota' | 'coupled_to_dfe_worker_concurrency';
  requested_quota: number | 'unlimited' | null;
} {
  const field = 'platform.local.collector_cpu_quota';
  if (value == null) return { source: 'coupled_to_dfe_worker_concurrency', requested_quota: null };
  if (value !== 'unlimited' && (typeof value !== 'number' || !Number.isFinite(value) || value < 0.01 || value > 4096 ||
      (String(value).split('.')[1]?.length ?? 0) > 9)) {
    throw new Error(`${field} must be a finite CPU quota from 0.01 to 4096 with at most nanocore precision, or "unlimited"`);
  }
  if (hostCpus !== undefined && (!Number.isSafeInteger(hostCpus) || hostCpus < 1 ||
      (typeof value === 'number' && value > hostCpus))) {
    throw new Error(`${field} exceeds or cannot validate Docker host logical CPUs (${hostCpus})`);
  }
  return { source: field, requested_quota: value as number | 'unlimited' };
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
  if (value.mode === 'local') {
    const platform = isRecord(value.config.platform) ? value.config.platform : undefined;
    const local = platform && isRecord(platform.local) ? platform.local : undefined;
    parseLocalCollectorCpuQuota(local?.collector_cpu_quota);
  }
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

function parseCampaignMeasurements(value: unknown): CampaignMeasurement[] {
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

export function validateCampaignMeasurements(value: unknown): CampaignMeasurement[] {
  if (!Array.isArray(value)) throw new Error('results must be a non-empty array');
  const rows = parseCampaignMeasurements(value);
  return rows.map((row, index) => {
    const raw = value[index] as RecordValue;
    const before = raw.baselineBefore === undefined ? undefined : parseOfficialRun(raw.baselineBefore, `result ${index} baselineBefore`);
    const after = raw.baselineAfter === undefined ? undefined : parseOfficialRun(raw.baselineAfter, `result ${index} baselineAfter`);
    const disposition = raw.disposition == null ? undefined : requiredText(raw.disposition, `result ${index} disposition`);
    const outcome = raw.outcome === undefined ? undefined : requiredText(raw.outcome, `result ${index} outcome`);
    const status = raw.status === undefined ? undefined : requiredText(raw.status, `result ${index} status`);
    if (raw.pairOrder !== undefined && raw.pairOrder !== 'AB' && raw.pairOrder !== 'BA') {
      throw new Error(`result ${index} pairOrder must be AB or BA`);
    }
    if (raw.evidenceState !== undefined && !['configured', 'projected', 'consumer-verified', 'physically-observed', 'measured'].includes(String(raw.evidenceState))) {
      throw new Error(`result ${index} evidenceState is invalid`);
    }
    const downstream = row.officialRuns?.flatMap(run => run.rows).map(evidenceForRow)
      .find(evidence => evidence?.downstream_status === 'unavailable');
    const officialRuns = row.officialRuns?.map(run => ({
      ...run,
      rows: run.rows.map(result => evidenceForRow(result)?.downstream_status === 'unavailable' ? {
        ...result, qap_completion_percent: null, qap_pending_artifacts: null, qap_cached_bytes: null,
        qap_unhealthy_catalogs: null, qap_lag_seconds: null, qap_observation_complete: 0, qap_observed_tables: 0,
      } : result),
    }));
    return {
      ...row, officialRuns, baselineBefore: before, baselineAfter: after, pairOrder: raw.pairOrder,
      evidenceState: raw.evidenceState as CampaignMeasurement['evidenceState'], disposition, outcome, status,
      ...(downstream ? {
        downstreamStatus: 'unavailable' as const, collectorOnly: true,
        qapCompletionPercent: null, qapPendingArtifacts: null, qapUnhealthyCatalogs: null,
        comment: typeof downstream.downstream_comment === 'string' && downstream.downstream_comment.trim() ? downstream.downstream_comment :
          typeof downstream.downstream_reason === 'string' ? downstream.downstream_reason : row.comment ?? 'QAP observation unavailable',
      } : {}),
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
    for (const run of [...row.officialRuns, ...(row.baselineBefore ? [row.baselineBefore] : []), ...(row.baselineAfter ? [row.baselineAfter] : [])]) {
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
  consumer_linux_thp_flag_readback: ['collector.thp_disabled'],
  consumer_glibc_tunables_startup_readback: [
    'collector.malloc.arena_max', 'collector.malloc.arena_test', 'collector.malloc.mmap_threshold',
    'collector.malloc.mmap_max', 'collector.malloc.trim_threshold', 'collector.malloc.top_pad', 'collector.malloc.mxfast',
    'collector.malloc.tcache_count', 'collector.malloc.tcache_max', 'collector.malloc.tcache_unsorted_limit',
    'collector.malloc.hugetlb',
  ],
  consumer_mimalloc_option_readback: [
    'collector.mimalloc.purge_delay_ms', 'collector.mimalloc.purge_decommits', 'collector.mimalloc.arena_eager_commit',
    'collector.mimalloc.allow_large_os_pages', 'collector.mimalloc.reserve_huge_os_pages',
    'collector.mimalloc.reserve_os_memory_kib', 'collector.mimalloc.arena_reserve_kib',
    'collector.mimalloc.arena_purge_mult', 'collector.mimalloc.minimal_purge_size_kib',
    'collector.mimalloc.page_commit_on_demand', 'collector.mimalloc.page_max_candidates',
    'collector.mimalloc.page_full_retain', 'collector.mimalloc.generic_collect',
    'collector.mimalloc.page_reclaim_on_free', 'collector.mimalloc.page_max_reclaim',
    'collector.mimalloc.page_cross_thread_max_reclaim', 'collector.mimalloc.use_numa_nodes',
  ],
  consumer_linux_listener_buffer_readback: ['collector.socket.recv_buffer_bytes', 'collector.socket.send_buffer_bytes'],
  consumer_accepted_socket_readback: ['collector.socket.tcp_nodelay'],
  consumer_cpu_pool_readback: ['collector.runtime.cpu_thread_stack_size'],
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
        ['inert_fields', inert, ['maintenance_disabled', 'dictionary_disabled', 'duration_based_run', 'collector_shutdown_after_measurement', 'connection_retries_disabled', 'stream_retries_disabled', 'http2_keepalive_disabled', 'adaptive_http2_window', 'tcp_keepalive_disabled', 'rate_limit_disabled', 'no_nullable_generated_columns', 'no_eligible_generated_rows', 'all_values_skewed', 'one_byte_maximum_width']],
        ['projection_only', projected, ['consumer_startup_proof_unverified']],
        ['runner_controls', undefined, ['maintenance_runner_control', 'router_log_filter_inheritance']],
        ['consumer_verified', undefined, ['consumer_startup_readback', 'consumer_table_writer_readback', 'consumer_table_adaptive_readback', 'consumer_linux_thp_flag_readback', 'consumer_glibc_tunables_startup_readback', 'consumer_mimalloc_option_readback', 'consumer_linux_listener_buffer_readback', 'consumer_accepted_socket_readback', 'consumer_cpu_pool_readback', 'consumer_startup_and_prepared_corpus', 'committed_table_metadata_readback']],
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

function evidenceForRow(row: RecordValue): RecordValue | undefined {
  if (typeof row.evidence_json !== 'string') return undefined;
  try {
    const evidence: unknown = JSON.parse(row.evidence_json);
    return isRecord(evidence) ? evidence : undefined;
  } catch {
    return undefined;
  }
}

function rankedObservation(run: OfficialRun): RankedObservation | undefined {
  const evidence = evidenceForRow(run.rows[0] ?? {});
  const durable = isRecord(evidence?.durable_commits) ? evidence.durable_commits : undefined;
  const reconciliation = isRecord(durable?.delta_log_reconciliation) ? durable.delta_log_reconciliation : undefined;
  const csv = isRecord(reconciliation?.csv) ? reconciliation.csv : undefined;
  const reconciled = csv;
  const screen = isRecord(evidence?.screen_v1) ? evidence.screen_v1 : undefined;
  const observed = screen;
  if (!observed || !reconciled || stableJson(observed) !== stableJson(reconciled)) return undefined;
  const output = isRecord(durable?.output_throughput) ? durable.output_throughput : undefined;
  const seconds = output?.window_seconds;
  const rate = observed.durable_rows_per_s;
  const outputRate = output?.rows_per_second;
  const resolution = observed.rate_resolution_rows_per_second;
  const count = nonNegativeInteger(observed.window_commit_count);
  const maximum = nonNegativeInteger(observed.max_commit_rows);
  if (typeof rate !== 'number' || !Number.isFinite(rate) || rate <= 0 ||
      typeof outputRate !== 'number' || !Number.isFinite(outputRate) ||
      Math.abs(rate - outputRate) > 1e-9 ||
      typeof resolution !== 'number' || !Number.isFinite(resolution) || resolution <= 0 ||
      typeof seconds !== 'number' || !Number.isFinite(seconds) || seconds <= 0 ||
      count === undefined || count === 0 || maximum === undefined || maximum === 0 ||
      Math.abs(resolution - maximum / seconds) > Math.max(1e-9, resolution * 1e-9)) return undefined;
  const bytes = observed.bytes_per_row;
  const slope = observed.slope_rows_per_second;
  return {
    rate, resolution, commitCount: count, maxCommitRows: maximum,
    slope: typeof slope === 'number' && Number.isFinite(slope) ? slope : null,
    bytesPerRow: typeof bytes === 'number' && Number.isFinite(bytes) && bytes > 0 ? bytes : null,
  };
}

function cellFailure(row: CampaignMeasurement): string | undefined {
  for (const result of row.officialRuns?.flatMap(run => run.rows) ?? []) {
    const evidence = evidenceForRow(result);
    if (evidence?.screen_v1_csv_reason === 'cell_failed') return 'screen_v1: cell_failed';
    for (const key of ['pre_teardown_reconciliation', 'reconciliation']) {
      const reconciliation = evidence?.[key];
      if (isRecord(reconciliation) && reconciliation.exact_match_required === true && reconciliation.exact_match === false) {
        return typeof reconciliation.reason === 'string' ? reconciliation.reason : 'delta_exact_match_failed';
      }
    }
  }
  if (row.committedRowsPerSecond === null) return row.comment ?? 'committed rate unavailable';
  if (row.status !== undefined && row.status !== 'succeeded') return row.comment ?? 'result status is ' + row.status;
  if (row.disposition?.endsWith('_quarantined_not_settled')) return row.comment ?? row.disposition;
  if (row.outcome !== undefined && row.outcome !== 'ok' && row.outcome !== 'succeeded' && row.outcome !== 'kustainer-null') {
    return row.comment ?? 'outcome: ' + row.outcome;
  }
  if (row.diedFirst && row.diedFirst.toLowerCase() !== 'kustainer') {
    return row.comment ?? 'component failure: ' + row.diedFirst;
  }
  for (const run of row.officialRuns ?? []) {
    const stop = officialOverloadStop(run);
    if (stop) return stop;
    for (const result of run.rows) {
      const evidence = evidenceForRow(result);
      const durable = isRecord(evidence?.durable_commits) ? evidence.durable_commits : undefined;
      const output = isRecord(durable?.output_throughput) ? durable.output_throughput : undefined;
      if (typeof output?.rows_per_second === 'number' && Number.isFinite(output.rows_per_second) &&
          Math.abs(output.rows_per_second - row.committedRowsPerSecond) > 1e-9) {
        return 'committed rate disagrees with official measurement';
      }
      for (const value of [result.disposition, evidence?.disposition]) {
        if (typeof value === 'string' && value.endsWith('_quarantined_not_settled')) return row.comment ?? value;
      }
      if (Array.isArray(evidence?.teardown_failures) && evidence.teardown_failures.some(
        failure => isRecord(failure) && failure.kind === 'dataflow_drain_incomplete',
      )) return row.comment ?? 'dataflow_drain_incomplete';
      if (evidence?.teardown_outcome === 'failed' || evidence?.teardown_outcome === 'not_started') {
        return row.comment ?? 'teardown ' + evidence.teardown_outcome;
      }
      if (result.status !== 'succeeded') return row.comment ?? 'result status is ' + String(result.status);
      if ((row.collectorOnly || evidence?.downstream_status === 'unavailable') &&
          (!isRecord(evidence?.reconciliation) || evidence.reconciliation.exact_match !== true)) {
        return 'QAP unavailable; exact Delta reconciliation not established';
      }
      if (run.status.state !== 'succeeded') return row.comment ?? 'run state is ' + run.status.state;
    }
    const verdict = evaluateDataFlow(run.status, run.rows, run.expectedSha, run.mode, run.config, 'single_hot_table_campaign');
    if (verdict.ingestPass !== true) return 'official campaign ingest evidence did not pass: ' + verdict.reasons.join('; ');
  }
  if (row.collectorOnly && !row.officialRuns?.length) return 'QAP unavailable; exact Delta reconciliation not established';
  return undefined;
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

export function guardContainerMemoryMetric(config: RecordValue): 'container_memory_bytes' | 'container_working_set_bytes' | undefined {
  if (config.overload_guard !== undefined && !isRecord(config.overload_guard)) return undefined;
  const requested = isRecord(config.overload_guard) ? config.overload_guard : {};
  const selection = requested.container_memory_metric === undefined ? 'usage' : requested.container_memory_metric;
  if (selection === 'usage') return 'container_memory_bytes';
  if (selection === 'working_set') return 'container_working_set_bytes';
  return undefined;
}

export function guardContainerMemoryMatches(config: RecordValue, guard: RecordValue): boolean {
  const metric = guardContainerMemoryMetric(config);
  if (!metric) return false;
  const selection = metric === 'container_memory_bytes' ? 'usage' : 'working_set';
  if (guard.settings !== undefined && (!isRecord(guard.settings) ||
      (guard.settings.container_memory_metric === undefined ? 'usage' : guard.settings.container_memory_metric) !== selection)) return false;
  if (guard.settings === undefined && selection !== 'usage') return false;
  if (guard.container_memory_metric !== undefined && guard.container_memory_metric !== selection) return false;
  if (guard.last_caps !== undefined && (!Array.isArray(guard.last_caps) || guard.last_caps.some(cap =>
      isRecord(cap) && (cap.metric === 'container_memory_bytes' || cap.metric === 'container_working_set_bytes') && cap.metric !== metric))) return false;
  return true;
}

const guardCapMetrics = ['container_memory_bytes', 'container_working_set_bytes', 'collector_rss_bytes',
  'collector_cgroup_memory_bytes', 'collector_buffered_bytes'];

function overloadStopIssue(evidence: RecordValue, config: RecordValue): string | undefined {
  const stop = evidence.overload_stop;
  if (stop === undefined || stop === null) return undefined;
  if (!isRecord(stop) || nonNegativeInteger(stop.timestamp_ms) === undefined ||
      nonNegativeInteger(stop.window_index) === undefined) return 'overload_stop evidence is invalid';
  const selectedMetric = guardContainerMemoryMetric(config);
  if (!selectedMetric || ((stop.metric === 'container_memory_bytes' || stop.metric === 'container_working_set_bytes') &&
      stop.metric !== selectedMetric) || (isRecord(evidence.overload_guard) && !guardContainerMemoryMatches(config, evidence.overload_guard))) {
    return 'overload_stop evidence is invalid';
  }
  const hardCap = stop.reason === 'hard_cap_fraction' && guardCapMetrics.includes(String(stop.metric)) &&
    nonNegativeInteger(stop.observed_value) !== undefined && nonNegativeInteger(stop.cap) !== undefined &&
    Number(stop.cap) > 0 && typeof stop.fraction === 'number' && Number.isFinite(stop.fraction) &&
    stop.fraction === Number(stop.observed_value) / Number(stop.cap);
  const stalled = stop.reason === 'zero_ingestion_commit_progress' && stop.metric === 'new_ingestion_commits' &&
    stop.observed_value === 0 && stop.cap === null && stop.fraction === null;
  if (!hardCap && !stalled) return 'overload_stop evidence is invalid';
  return `above_ceiling: overload_stop ${String(stop.reason)} (${String(stop.metric)})`;
}

function guardTiming(config: RecordValue): { readTimeoutMs: number; maxSamplingGapMs: number } | undefined {
  if (config.overload_guard !== undefined && !isRecord(config.overload_guard)) return undefined;
  const requested = isRecord(config.overload_guard) ? config.overload_guard : {};
  const readTimeoutMs = requested.read_timeout_ms === undefined ? 3000 : requested.read_timeout_ms;
  const maxSamplingGapMs = requested.max_sampling_gap_ms === undefined ? 5000 : requested.max_sampling_gap_ms;
  if (nonNegativeInteger(readTimeoutMs) === undefined || nonNegativeInteger(maxSamplingGapMs) === undefined) return undefined;
  const timeout = Number(readTimeoutMs);
  const gap = Number(maxSamplingGapMs);
  if (timeout < 1 || timeout > 3000 || gap < 1 || gap > 5000 || timeout >= gap) return undefined;
  return { readTimeoutMs: timeout, maxSamplingGapMs: gap };
}

function overloadGuardIssue(evidence: RecordValue, config: RecordValue, runId: unknown, start: number, end: number): string | undefined {
  const invalid = 'overload_guard protection unavailable or invalid';
  if (config.overload_guard !== undefined && !isRecord(config.overload_guard)) return invalid;
  const requested = isRecord(config.overload_guard) ? config.overload_guard : {};
  const timing = guardTiming(config);
  if (!timing) return invalid;
  const gap = timing.maxSamplingGapMs;
  const enabled = requested.enabled === undefined ? true : requested.enabled;
  const fraction = requested.cap_fraction === undefined ? 0.9 : requested.cap_fraction;
  const zeroWindows = requested.zero_progress_windows === undefined ? 2 : requested.zero_progress_windows;
  const memorySignal = requested.memory_signal === undefined ? 'process_rss' : requested.memory_signal;
  if (memorySignal !== 'process_rss' && memorySignal !== 'cgroup_memory') return invalid;
  const selectedMetric = guardContainerMemoryMetric(config);
  if (!selectedMetric) return invalid;
  const metrics = [selectedMetric, memorySignal === 'cgroup_memory' ?
    'collector_cgroup_memory_bytes' : 'collector_rss_bytes', 'collector_buffered_bytes'];
  const warmup = nonNegativeInteger(config.warmup_seconds);
  if (enabled !== true || typeof fraction !== 'number' || !Number.isFinite(fraction) || fraction <= 0 || fraction > 0.9 ||
      nonNegativeInteger(zeroWindows) === undefined || Number(zeroWindows) < 1 || Number(zeroWindows) > 2 ||
      warmup === undefined || start < warmup * 1000) return invalid;
  const guard = evidence.overload_guard;
  if (!isRecord(guard) || !isRecord(guard.settings) || !guardContainerMemoryMatches(config, guard)) return invalid;
  const { read_timeout_ms: readTimeoutMs = 3000, max_sampling_gap_ms: maxSamplingGapMs = 5000 } = guard.settings;
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
      guard.settings.enabled !== true || guard.settings.cap_fraction !== fraction ||
      guard.settings.zero_progress_windows !== zeroWindows ||
      (guard.settings.memory_signal ?? 'process_rss') !== memorySignal ||
      readTimeoutMs !== timing.readTimeoutMs || maxSamplingGapMs !== gap ||
      nonNegativeInteger(guard.coverage_start_ms) === undefined || Number(guard.coverage_start_ms) > expectedStart ||
      nonNegativeInteger(guard.coverage_end_ms) === undefined || Number(guard.coverage_end_ms) < end ||
      nonNegativeInteger(guard.max_sampling_gap_ms) === undefined || Number(guard.max_sampling_gap_ms) > gap ||
      nonNegativeInteger(guard.sample_count) === undefined || Number(guard.sample_count) < Math.ceil((end - expectedStart) / gap) + 1 ||
      typeof guard.peak_cap_fraction !== 'number' || !Number.isFinite(guard.peak_cap_fraction) ||
      guard.peak_cap_fraction < 0 || guard.peak_cap_fraction >= fraction ||
      !Array.isArray(caps) || caps.length !== metrics.length ||
      requestedCap != null && !caps.some(cap => isRecord(cap) && cap.metric === selectedMetric && cap.cap === requestedCap) ||
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
      const reason = overloadStopIssue(evidence, run.config);
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
    const resolution = rawBucket.rate_resolution_rows_per_second;
    windows.push({ rate: bucketRate, resolution: typeof resolution === 'number' && Number.isFinite(resolution) && resolution > 0 ? resolution : null });
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
  const failure = cellFailure(row);
  if (failure) return { qualified: false, rates: [], reason: failure };
  if (row.outcome === 'kustainer-null') return { qualified: false, rates: [], reason: 'downstream unavailable; outcome is not succeeded' };
  const overloadStop = row.officialRuns?.map(officialOverloadStop).find(reason => reason !== undefined);
  if (overloadStop) return { qualified: false, rates: [], reason: overloadStop };
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
  if (inspected.windows.some(window => window.resolution === null)) {
    return { qualified: false, rates, reason: 'provisional: commit-rate resolution unavailable' };
  }
  const ranked = rankedObservation(row.officialRuns[0]!);
  if (!ranked) return { qualified: false, rates, reason: 'provisional: reconciled ranked rate or resolution unavailable' };
  if (row.evidenceState !== 'physically-observed' && row.evidenceState !== 'measured') {
    return { qualified: false, rates, reason: 'provisional: physical factor evidence unavailable' };
  }
  if (!row.baselineBefore || !row.baselineAfter || !row.pairOrder) {
    return { qualified: false, rates, reason: 'provisional: adjacent baseline evidence or pair order unavailable' };
  }
  const baselineRuns = [row.baselineBefore, row.baselineAfter];
  const baselines = baselineRuns.map(run => {
    const baselineEvidence = evidenceForRow(run.rows[0] ?? {});
    const durable = isRecord(baselineEvidence?.durable_commits) ? baselineEvidence.durable_commits : undefined;
    const output = isRecord(durable?.output_throughput) ? durable.output_throughput : undefined;
    const baselineRate = typeof output?.rows_per_second === 'number' ? output.rows_per_second : null;
    const failure = cellFailure({ ...row, committedRowsPerSecond: baselineRate, officialRuns: [run], diedFirst: null, collectorOnly: false });
    return failure ? { reason: failure } : inspectOfficialRun(run);
  });
  if (baselines.some(baseline => !baseline.windows || baseline.reason)) {
    return { qualified: false, rates, reason: 'invalid adjacent baseline: ' + (baselines.find(baseline => !baseline.windows || baseline.reason)?.reason ?? 'missing windows') };
  }
  const [before, after] = baselines.map(baseline => baseline.windows!);
  if (before!.some(window => window.resolution === null) || after!.some(window => window.resolution === null)) {
    return { qualified: false, rates, reason: 'provisional: baseline commit-rate resolution unavailable' };
  }
  const baselineRanked = baselineRuns.map(rankedObservation);
  if (baselineRanked.some(observation => !observation)) {
    return { qualified: false, rates, reason: 'provisional: reconciled baseline rate or resolution unavailable' };
  }
  const baselineRates = baselineRanked.map(observation => observation!.rate);
  const baselineMean = average(baselineRates);
  if ([before!, after!].some(windows => {
    const values = windows.map(window => window.rate);
    return (Math.max(...values) - Math.min(...values)) / average(values) > 0.05;
  })) return { qualified: false, rates, reason: 'invalid A/A: baseline window range exceeds 5%' };
  if (Math.abs(baselineRates[1]! - baselineRates[0]!) > baselineRates[0]! * 0.05 + Number.EPSILON * baselineMean * 4) {
    return { qualified: false, rates, reason: 'invalid A/A: adjacent baselines differ by more than 5%' };
  }
  const baselineResolution = Math.max(...baselineRanked.map(observation => observation!.resolution));
  if (Math.max(
    baselineRanked[0]!.resolution / baselineRates[0]!,
    baselineRanked[1]!.resolution / baselineRates[1]!,
  ) > 0.01) {
    return { qualified: false, rates, reason: 'invalid A/A: commit-rate resolution exceeds 1%' };
  }
  const resolution = Math.max(ranked.resolution, baselineResolution);
  if (Math.abs(ranked.rate - baselineMean) < resolution) {
    return { qualified: false, rates, reason: 'unresolved: difference is smaller than commit-rate resolution' };
  }
  const output = (run: OfficialRun): RecordValue | undefined => {
    const evidence = evidenceForRow(run.rows[0] ?? {});
    return isRecord(evidence?.durable_commits) && isRecord(evidence.durable_commits.output_throughput)
      ? evidence.durable_commits.output_throughput : undefined;
  };
  const beforeOutput = output(row.baselineBefore);
  const afterOutput = output(row.baselineAfter);
  const candidateOutput = output(row.officialRuns[0]!);
  if (!beforeOutput || !afterOutput || !candidateOutput ||
      typeof beforeOutput.measurement_end_unix_ms !== 'number' ||
      typeof candidateOutput.measurement_start_unix_ms !== 'number' ||
      typeof candidateOutput.measurement_end_unix_ms !== 'number' ||
      typeof afterOutput.measurement_start_unix_ms !== 'number' ||
      beforeOutput.measurement_end_unix_ms > candidateOutput.measurement_start_unix_ms ||
      candidateOutput.measurement_end_unix_ms > afterOutput.measurement_start_unix_ms ||
      new Set([row.baselineBefore.status.id, row.officialRuns[0]!.status.id, row.baselineAfter.status.id]).size !== 3) {
    return { qualified: false, rates, reason: 'invalid adjacent baseline ordering' };
  }
  if (Math.abs(ranked.rate - baselineRates[row.pairOrder === 'AB' ? 0 : 1]!) < resolution) {
    return { qualified: false, rates, reason: 'unresolved: paired difference is smaller than commit-rate resolution' };
  }
  const candidateBytes = ranked.bytesPerRow;
  const baselineBytes = baselineRanked[row.pairOrder === 'AB' ? 0 : 1]!.bytesPerRow;
  if (typeof candidateBytes !== 'number' || !Number.isFinite(candidateBytes) || candidateBytes <= 0 ||
      typeof baselineBytes !== 'number' || !Number.isFinite(baselineBytes) || baselineBytes <= 0) {
    return { qualified: false, rates, reason: 'provisional: paired bytes per row unavailable' };
  }
  return {
    qualified: true, rates: [ranked.rate], ratio: ranked.rate / baselineMean,
    pairRatio: ranked.rate / baselineRates[row.pairOrder === 'AB' ? 0 : 1]!,
    candidateBytes, baselineBytes, pairedBaselineRate: baselineRates[row.pairOrder === 'AB' ? 0 : 1]!,
    reason: 'normalized to adjacent baseline mean; resolution ' + numberLabel(resolution) + ' rows/s',
  };
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
    const ordered = [...group].sort((a, b) => a.repeat - b.repeat);
    const pairOrderValid = ordered.map(row => row.pairOrder).join(',') === 'AB,BA,AB';
    const pairSpans = ordered.map(row => {
      const candidate = row.officialRuns?.[0];
      const baseline = row.pairOrder === 'AB' ? row.baselineBefore : row.baselineAfter;
      const interval = (run: OfficialRun | undefined): [number, number] | undefined => {
        const evidence = run && evidenceForRow(run.rows[0] ?? {});
        const durable = isRecord(evidence?.durable_commits) ? evidence.durable_commits : undefined;
        const output = isRecord(durable?.output_throughput) ? durable.output_throughput : undefined;
        return typeof output?.measurement_start_unix_ms === 'number' && typeof output.measurement_end_unix_ms === 'number'
          ? [output.measurement_start_unix_ms, output.measurement_end_unix_ms] : undefined;
      };
      const a = interval(baseline);
      const b = interval(candidate);
      return a && b ? [Math.min(a[0], b[0]), Math.max(a[1], b[1])] : undefined;
    });
    const sequenceValid = pairSpans.length === 3 && pairSpans.every((span, index) =>
      span !== undefined && (index === 0 || pairSpans[index - 1]![1]! <= span[0]!));
    const runIds = group.flatMap(row => row.officialRuns?.map(run => run.status.id) ?? []);
    const pairedBaselineIds = group.flatMap(row =>
      (row.pairOrder === 'AB' ? row.baselineBefore : row.baselineAfter)?.status.id ?? []);
    const paired = completed.map(item => item.evidence);
    const logs = paired.map(item => Math.log(item.pairRatio!));
    const logMean = logs.length ? average(logs) : 0;
    const logSd = logs.length === 3 ? Math.sqrt(logs.reduce((sum, value) => sum + (value - logMean) ** 2, 0) / 2) : 0;
    const pairedT = logSd === 0 ? (logMean > 0 ? Infinity : 0) : logMean / (logSd / Math.sqrt(3));
    const bytesRatio = paired.length === 3 ? average(paired.map(item => item.candidateBytes!)) /
      average(paired.map(item => item.baselineBytes!)) : Infinity;
    const logCandidates = paired.map(item => Math.log(average(item.rates)));
    const logBaselines = paired.map(item => Math.log(item.pairedBaselineRate!));
    const variance = (values: number[]) => values.reduce((sum, value) => sum + (value - average(values)) ** 2, 0) / (values.length - 1);
    const welchDenominator = paired.length === 3 ? Math.sqrt((variance(logCandidates) + variance(logBaselines)) / 3) : NaN;
    const welch = welchDenominator === 0 ? (logMean > 0 ? Infinity : 0) :
      (average(logCandidates) - average(logBaselines)) / welchDenominator;
    const stats = paired.length === 3 ? 'paired effect ' + numberLabel(Math.exp(logMean)) + ', t ' + numberLabel(pairedT) +
      ', bytes/row B/A ' + numberLabel(bytesRatio) + ', Welch t (reference) ' + numberLabel(welch) : '';
    const winner = paired.length === 3 && pairOrderValid && sequenceValid && new Set(runIds).size === 3 && new Set(pairedBaselineIds).size === 3 &&
      Math.exp(logMean) >= 1.05 && pairedT >= 4.303 &&
      bytesRatio <= 1.10;
    const qualified = coverage.status === 'active' && expectedRepeats === 3 && completeSet && completed.length === 3 && winner;
    const officialRepeatRates = completed.map(item => average(item.evidence.rates));
    const observedRates = coverage.status !== 'active' || cells.get(JSON.stringify([group[0]!.valueX, group[0]!.valueY]))!.failures.length ? [] :
      group.flatMap(row => row.committedRowsPerSecond === null || cellFailure(row) ? [] : [row.committedRowsPerSecond]);
    const rates = qualified ? officialRepeatRates : observedRates;
    const deaths = group.map(row => row.diedFirst).filter((value): value is string => value !== null && value !== undefined);
    const status = qualified ? 'qualified' : group.some(row => cellFailure(row)) ? 'failed' :
      deaths.some(value => value.toLowerCase() !== 'kustainer') ? 'failed' :
      deaths.length || group.some(row => row.collectorOnly) ? 'collector-only' : 'unqualified';
    return {
      candidate, rates, mean: rates.length ? average(rates) : 0,
      normalizedMean: qualified ? average(paired.map(item => item.ratio!)) : null,
      min: rates.length ? Math.min(...rates) : 0,
      max: rates.length ? Math.max(...rates) : 0,
      completedRepeats: completed.length, expectedRepeats, status,
      reason: qualified ? stats : [
        ...(coverage.status === 'active' ? [] : [coverage.reason]),
        assessments.find(item => !item.evidence.qualified)?.evidence.reason ??
          (pairOrderValid && sequenceValid ? 'paired winner thresholds not met' : 'AB/BA/AB pair order missing or out of sequence'),
        ...(stats ? [stats] : []),
      ].join('; '),
    };
  });
}

function normalizedRatio(row: CampaignMeasurement): number | undefined {
  if (cellFailure(row) || !row.officialRuns?.[0] || !row.baselineBefore || !row.baselineAfter) return undefined;
  for (const baseline of [row.baselineBefore, row.baselineAfter]) {
    const evidence = evidenceForRow(baseline.rows[0] ?? {});
    const durable = isRecord(evidence?.durable_commits) ? evidence.durable_commits : undefined;
    const output = isRecord(durable?.output_throughput) ? durable.output_throughput : undefined;
    const rate = typeof output?.rows_per_second === 'number' ? output.rows_per_second : null;
    if (cellFailure({ ...row, committedRowsPerSecond: rate, officialRuns: [baseline], diedFirst: null, collectorOnly: false })) return undefined;
  }
  const runs = [row.officialRuns[0], row.baselineBefore, row.baselineAfter].map(inspectOfficialRun);
  if (runs.some(run => !run.windows)) return undefined;
  const observations = [row.officialRuns[0], row.baselineBefore, row.baselineAfter].map(rankedObservation);
  if (observations.some(observation => !observation)) return undefined;
  return observations[0]!.rate / average([observations[1]!.rate, observations[2]!.rate]);
}

function lowCommitWindowLabels(evidence: RecordValue | undefined): string[] {
  const durable = isRecord(evidence?.durable_commits) ? evidence.durable_commits : undefined;
  const windows = Array.isArray(durable?.complete_30_second_commit_windows) ? durable.complete_30_second_commit_windows : [];
  return windows.flatMap((window, index) => {
    const count = isRecord(window) ? nonNegativeInteger(window.window_commit_count) : undefined;
    return count !== undefined && count < 100 ? [String(index + 1) + ' (' + count + ')'] : [];
  });
}

function measurementDetails(row: CampaignMeasurement): string {
  const failure = cellFailure(row);
  if (failure) return row.candidate + ' #' + row.repeat + ': null — ' + failure;
  const result = row.officialRuns?.[0]?.rows[0];
  const evidence = result && evidenceForRow(result);
  const durable = isRecord(evidence?.durable_commits) ? evidence.durable_commits : undefined;
  const output = isRecord(durable?.output_throughput) ? durable.output_throughput : undefined;
  const lowCommitWindows = lowCommitWindowLabels(evidence);
  const baselineWarnings = ([['before', row.baselineBefore], ['after', row.baselineAfter]] as const).flatMap(([name, run]) => {
    const labels = lowCommitWindowLabels(run ? evidenceForRow(run.rows[0] ?? {}) : undefined);
    return labels.length ? ['; baseline ' + name + ' low-commit windows (<100): ' + labels.join(', ')] : [];
  }).join('');
  const ranked = row.officialRuns?.[0] ? rankedObservation(row.officialRuns[0]) : undefined;
  const rate = typeof output?.rows_per_second === 'number' && Number.isFinite(output.rows_per_second) && output.rows_per_second > 0 ? output.rows_per_second : null;
  const collector = result && typeof result.cpu_millicores === 'number' && Number.isFinite(result.cpu_millicores) && result.cpu_millicores >= 0 && rate
    ? result.cpu_millicores / 1000 / rate * 1_000_000 : null;
  const reports = evidence?.client_phase_reports;
  const clientCores = Array.isArray(reports) && reports.length ? reports.map(report => {
    const phase = isRecord(report) && isRecord(report.phase_counters) ? report.phase_counters : undefined;
    const window = isRecord(phase?.measurement_window) ? phase.measurement_window : undefined;
    const resources = isRecord(window?.client_resources) ? window.client_resources : undefined;
    return typeof resources?.cpu_core_equivalents === 'number' && Number.isFinite(resources.cpu_core_equivalents) &&
      resources.cpu_core_equivalents >= 0 ? resources.cpu_core_equivalents : null;
  }) : [];
  const client = rate && clientCores.length && clientCores.every(value => value !== null)
    ? (clientCores as number[]).reduce((sum, value) => sum + value, 0) / rate * 1_000_000 : null;
  let concurrency: number | null = null;
  let quotaError = false;
  if (typeof result?.tunables_json === 'string') {
    try {
      const tunables: unknown = JSON.parse(result.tunables_json);
      const quota = isRecord(tunables) && isRecord(tunables.cpu_quota) ? tunables.cpu_quota : undefined;
      const quotaClient = isRecord(quota?.client) ? quota.client : undefined;
      if (typeof quotaClient?.concurrency_per_cpu_core === 'number' && Number.isFinite(quotaClient.concurrency_per_cpu_core) &&
          quotaClient.concurrency_per_cpu_core >= 0) concurrency = quotaClient.concurrency_per_cpu_core;
    } catch { quotaError = true; }
  }
  const label = (value: number | null | undefined) => value == null ? 'null' : numberLabel(value);
  return row.candidate + ' #' + row.repeat + ': normalized ' + label(normalizedRatio(row)) +
    ', resolution ' + label(ranked?.resolution) + ' rows/s, commits ' + label(ranked?.commitCount) +
    ', max commit rows ' + label(ranked?.maxCommitRows) + ', slope ' + label(ranked?.slope) +
    ' rows/s, concurrency/core ' + label(concurrency) +
    (quotaError ? ' (invalid tunables JSON)' : '') +
    '; CPU s/M rows (estimated): collector ' + label(collector) +
    ', DFE null, client ' + label(client) +
    (lowCommitWindows.length ? '; low-commit windows (<100): ' + lowCommitWindows.join(', ') : '') +
    baselineWarnings +
    (row.comment ? '; ' + row.comment : '');
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
    const cell = cells.get(key) ?? { x: row.valueX, y: row.valueY, rates: [], coverage, failures: [], notes: [] };
    cell.coverage = combineCoverage([cell.coverage, coverage]);
    cell.notes.push(measurementDetails(row));
    const failure = cellFailure(row);
    if (failure) cell.failures.push(failure);
    else if (row.committedRowsPerSecond !== null) cell.rates.push(row.committedRowsPerSecond);
    cells.set(key, cell);
  }
  const rates = [...cells.values()].flatMap(cell =>
    cell.coverage.status !== 'active' || cell.failures.length ? [] : cell.rates);
  const minimum = rates.length ? Math.min(...rates) : 0;
  const maximum = rates.length ? Math.max(...rates) : 0;
  const summaries = summarizeCandidates(rows, cells);
  const leaderboard = summaries.filter(item => item.status === 'qualified').sort((a, b) =>
    b.normalizedMean! - a.normalizedMean! || (a.candidate < b.candidate ? -1 : a.candidate > b.candidate ? 1 : 0));
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
  const detailsY = exploratoryY + 55 + exploratory.length * 24;
  const height = Math.max(540, 145 + leaderboard.length * 38, detailsY + 25 + rows.length * 20);
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
      if (!cell || !cell.rates.length || cell.coverage.status !== 'active' || cell.failures.length) {
        const note = cell ? [...cell.failures, cell.coverage.reason, ...cell.notes].join('; ') : 'no observation';
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="#f6f8fa" stroke="#d0d7de"><title>' + escapeXml(note) + '</title></rect>');
        parts.push(svgText(xPosition + 8, y + 22, 'null', 11));
      } else {
        const mean = average(cell.rates);
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="' + cellColor(mean, minimum, maximum) + '" stroke="#8c959f"><title>' + escapeXml(cell.notes.join('; ')) + '</title></rect>');
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
    parts.push('<g><title>' + escapeXml(item.reason) + '</title>' + svgText(leaderboardX, y + 13, item.candidate + ' (' + item.completedRepeats + '/' + item.expectedRepeats + ')', 12) + '</g>');
    const meanWidth = item.mean / maxMean * barWidth;
    const minX = barStart + item.min / maxMean * barWidth;
    const maxX = barStart + item.max / maxMean * barWidth;
    parts.push('<rect x="' + barStart + '" y="' + y + '" width="' + meanWidth + '" height="18" rx="3" fill="#218bff" opacity="0.75"/>');
    parts.push('<line x1="' + minX + '" y1="' + (y + 9) + '" x2="' + maxX + '" y2="' + (y + 9) + '" stroke="#1f2328" stroke-width="2"/>');
    parts.push('<line x1="' + minX + '" y1="' + (y + 4) + '" x2="' + minX + '" y2="' + (y + 14) + '" stroke="#1f2328"/>');
    parts.push('<line x1="' + maxX + '" y1="' + (y + 4) + '" x2="' + maxX + '" y2="' + (y + 14) + '" stroke="#1f2328"/>');
    parts.push(svgText(barStart + barWidth + 12, y + 13, numberLabel(item.mean) + ' (' + numberLabel(item.min) + '–' + numberLabel(item.max) + '), ' + numberLabel(item.normalizedMean!) + '×', 11));
  });
  parts.push(svgText(40, noteY, 'What died first', 16, 'bold'));
  if (!deaths.length) parts.push(svgText(40, noteY + 25, 'No failure recorded in these rows.', 12));
  deaths.slice(0, 8).forEach((row, index) => {
    const coverage = cells.get(JSON.stringify([row.valueX, row.valueY]))!.coverage;
    const rate = row.committedRowsPerSecond === null || coverage.status !== 'active' || cellFailure(row) ?
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
  parts.push(svgText(40, detailsY, 'Per-repeat evidence', 16, 'bold'));
  rows.forEach((row, index) => parts.push(svgText(40, detailsY + 22 + index * 20, measurementDetails(row), 11)));
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
