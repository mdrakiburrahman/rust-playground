import { createHash } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect } from '../../infra/azure/effect-boundary.js';
import type { CampaignState, CampaignStore, OfficialCellMeasurementReference, RunInventory } from './aks-campaign.js';
import type { ScaleCustodyToken, ScaleEvidenceEnvelope, ScaleEvidenceVerifier, ScaleMeasurementEnvelope, ScaleVerificationWindow } from './aks-scale.js';

const MAX_SOURCE_BYTES = 4_194_304;
const ORIGINAL_WINDOW_SCOPE = 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort';
const PUBLICATION_SCOPE = 'original_official_cell_window_not_paired_scale_authorization';
type JsonRecord = Record<string, unknown>;
function record(value: unknown): value is JsonRecord {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
function object(value: unknown, description: string): JsonRecord {
  if (!record(value)) throw new Error(description + ' is UNKNOWN.');
  return value;
}
function exact(value: JsonRecord, keys: string[]): boolean {
  return Object.keys(value).sort().join() === [...keys].sort().join();
}
function sha(bytes: Uint8Array | string): string { return createHash('sha256').update(bytes).digest('hex'); }
function integer(value: unknown): value is number { return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0; }

export async function readBoundedSourceBytes(response: Response, limit: number): Promise<Uint8Array> {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_SOURCE_BYTES) throw new Error('Invalid source byte bound.');
  const declared = response.headers.get('content-length');
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > limit)) {
    await response.body?.cancel();
    throw new Error('Authenticated source response exceeds its byte bound.');
  }
  if (!response.body) throw new Error('Authenticated source response has no body.');
  const bytes = new Uint8Array(limit), reader = response.body.getReader();
  let length = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      if (next.value.byteLength > limit - length) {
        await reader.cancel();
        throw new Error('Authenticated source response exceeds its byte bound.');
      }
      bytes.set(next.value, length);
      length += next.value.byteLength;
    }
  } finally { reader.releaseLock(); }
  if (declared !== null && Number(declared) !== length) throw new Error('Authenticated source response is truncated.');
  return bytes.subarray(0, length);
}

export interface OfficialCellMeasurement {
  reference: OfficialCellMeasurementReference;
  inventory: RunInventory;
  effectiveConfig: JsonRecord;
  officialResult: JsonRecord;
  output: JsonRecord;
  startMs: number | null;
  endMs: number | null;
  committedRowsPerSecond: number | null;
  unavailableReasons: string[];
}

type SourceOptions = Parameters<typeof readOfficialCellMeasurement>[0];
function assertSourceDeadline(options: SourceOptions) {
  if (!Number.isSafeInteger(options.deadlineAtMs) || (options.now ?? Date.now)() >= options.deadlineAtMs)
    throw new Error('Original scale telemetry deadline expired.');
}
async function currentMeasurementState(options: SourceOptions): Promise<CampaignState> {
  assertSourceDeadline(options);
  const state = (await options.store.read())?.state, now = (options.now ?? Date.now)(), token = options.token;
  assertSourceDeadline(options);
  if (!state || state.status !== 'active' || state.mode !== 'armed' || state.ownerId !== token.ownerId ||
      state.fencingToken !== token.fencingToken || now < state.heartbeatAtMs || now >= state.expiresAtMs ||
      now - state.heartbeatAtMs >= 180_000 || !state.runInventory ||
      state.runInventory.owner_id !== token.ownerId || state.runInventory.fencing_token !== token.fencingToken ||
      state.runInventory.campaign_id !== state.campaignId || state.runInventory.load_admission !== 'authorized')
    throw new Error('Authenticated measurement custody is fenced, stale or not authorized.');
  return state;
}
async function assertOriginalCustody(options: SourceOptions, original: OfficialCellMeasurement) {
  const state = await currentMeasurementState(options);
  if (canonical_json(state.runInventory) !== canonical_json(original.inventory))
    throw new Error('Authenticated measurement custody changed during the source read.');
}

/** Reads only the exact immutable object admitted by authenticated current custody, never a caller path or source label. */
export async function readOfficialCellMeasurement(options: {
  azure: Azure; store: CampaignStore; token: ScaleCustodyToken; deadlineAtMs: number;
  fetcher?: typeof fetch; now?: () => number;
}): Promise<OfficialCellMeasurement> {
  const { azure, deadlineAtMs } = options, now = options.now ?? Date.now;
  const assertDeadline = () => assertSourceDeadline(options);
  const current = () => currentMeasurementState(options);
  const before = await current(), inventory = before.runInventory!;
  const reference = inventory.official_measurement, destination = inventory.evidence_destination;
  if (!reference || !destination) throw new Error('Current RunInventory has no authenticated immutable official measurement reference.');
  if (destination.account !== azure.c.account || destination.container !== azure.c.checkpoints ||
      reference.blob !== destination.blob_prefix + '/official-cell-measurement-v1.json' ||
      reference.byte_length < 1 || reference.byte_length > MAX_SOURCE_BYTES || !Number.isSafeInteger(reference.byte_length))
    throw new Error('Official measurement does not belong to the selected stamp custody destination.');
  const binding = (state: CampaignState) => canonical_json({ campaign: state.campaignId, owner: state.ownerId,
    fence: state.fencingToken, inventory: state.runInventory });
  const expectedBinding = binding(before);
  const url = new URL(`https://${destination.account}.blob.core.windows.net/${destination.container}/` +
    reference.blob.split('/').map(encodeURIComponent).join('/'));
  if (reference.version !== null) url.searchParams.set('versionid', reference.version);
  const accessToken = await azure.token('https://storage.azure.com/');
  assertDeadline();
  if (binding(await current()) !== expectedBinding) throw new Error('Authenticated measurement custody changed before the source read.');
  const bytes = await dispatchExternalEffect({ transport: 'azure-rest', method: 'GET',
    resource: url.origin + '|https://storage.azure.com/', path: url.pathname + url.search }, async () => {
    assertDeadline();
    const response = await (options.fetcher ?? fetch)(url, { method: 'GET', redirect: 'error',
      signal: AbortSignal.timeout(Math.max(1, Math.min(30_000, deadlineAtMs - now()))),
      headers: { authorization: 'Bearer ' + accessToken, 'If-Match': reference.etag,
        'x-ms-version': '2023-11-03', 'x-ms-date': new Date(now()).toUTCString() } });
    if (response.status !== 200 || response.headers.get('etag') !== reference.etag ||
        reference.version !== null && response.headers.get('x-ms-version-id') !== reference.version) {
      await response.body?.cancel();
      throw new Error('Official measurement source status, ETag or version differs from authenticated custody.');
    }
    return readBoundedSourceBytes(response, reference.byte_length);
  });
  assertDeadline();
  if (binding(await current()) !== expectedBinding) throw new Error('Authenticated measurement custody changed during the source read.');
  if (bytes.byteLength !== reference.byte_length || sha(bytes) !== reference.sha256)
    throw new Error('Official measurement bytes do not match the retained immutable publication.');
  let decoded: unknown;
  try { decoded = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as unknown; }
  catch { throw new Error('Official measurement source is malformed or truncated JSON.'); }
  const payload = object(decoded, 'Official measurement');
  const origin = { campaign_id: inventory.campaign_id, owner_id: inventory.owner_id, fencing_token: inventory.fencing_token,
    orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id, namespace_uid: inventory.namespace.uid };
  if (!exact(payload, ['schema_version', 'scope', 'origin', 'submitted_config_sha256', 'effective_config_json',
      'effective_config_sha256', 'source_revision', 'official_result']) ||
      payload.schema_version !== 1 || payload.scope !== PUBLICATION_SCOPE || canonical_json(payload.origin) !== canonical_json(origin) ||
      payload.submitted_config_sha256 !== reference.submitted_config_sha256 || payload.source_revision !== reference.source_revision ||
      typeof payload.effective_config_json !== 'string' || sha(payload.effective_config_json) !== reference.effective_config_sha256 ||
      payload.effective_config_sha256 !== reference.effective_config_sha256)
    throw new Error('Official measurement source/run/config provenance differs from authenticated custody.');
  const effectiveConfig = object(JSON.parse(payload.effective_config_json) as unknown, 'Official effective config');
  const result = object(payload.official_result, 'Original official result');
  const tunables = object(result.tunables, 'Original official tunables'), evidence = object(result.evidence, 'Original official evidence');
  const typed = object(effectiveConfig.typed_oat_config, 'Original typed effective config');
  if (!exact(result, ['run_id', 'table', 'status', 'git_sha', 'tunables', 'evidence']) ||
      result.run_id !== inventory.cell_run_id || tunables.orchestration_run_id !== inventory.orchestration_run_id ||
      result.git_sha !== (reference.source_revision ?? 'unknown') || !Array.isArray(typed.tables) ||
      typed.tables.length !== 1 || typed.tables[0] !== result.table || evidence.version !== 1)
    throw new Error('Official result does not match the original cell, table and source.');
  const output = object(evidence.commit_window_output_throughput, 'Original official commit window');
  const start = output.measurement_start_unix_ms, end = output.measurement_end_unix_ms;
  const unavailableReasons: string[] = [];
  if (reference.submitted_config_sha256 === null) unavailableReasons.push('authoritative_submitted_config_identity_unavailable');
  if (reference.source_revision === null) unavailableReasons.push('official_source_revision_unavailable');
  if (result.status !== 'succeeded') unavailableReasons.push('original_official_cell_did_not_succeed');
  const complete = integer(start) && integer(end) && start >= before.startedAtMs && end > start && end <= now() &&
    start === reference.measurement_start_unix_ms && end === reference.measurement_end_unix_ms &&
    output.window_seconds === (end - start) / 1000 && output.scope === ORIGINAL_WINDOW_SCOPE &&
    output.unavailable_reason == null && typeof output.rows_per_second === 'number' &&
    Number.isFinite(output.rows_per_second) && output.rows_per_second >= 0;
  if (!complete) unavailableReasons.push('original_official_commit_window_incomplete');
  return { reference: structuredClone(reference), inventory: structuredClone(inventory), effectiveConfig,
    officialResult: result, output, startMs: integer(start) ? start : null, endMs: integer(end) ? end : null,
    committedRowsPerSecond: complete ? output.rows_per_second as number : null, unavailableReasons };
}

// Microsoft.Kusto/clusters: Percent/Average/PT1M, not row latency or cache fullness.
// https://learn.microsoft.com/azure/azure-monitor/reference/supported-metrics/microsoft-kusto-clusters-metrics
export const KUSTO_SATURATION_THRESHOLDS = Object.freeze({ cpuPercent: 90, ingestionUtilizationPercent: 90 });
const MONITOR_GRAIN_MS = 60_000;
interface MonitorMetric {
  averagePercent: number | null;
  unavailableReason: string | null;
}
export interface OfficialKustoWindow {
  original: OfficialCellMeasurement;
  resourceId: string;
  windowStartMs: number | null;
  windowEndMs: number | null;
  cpu: MonitorMetric;
  ingestionUtilization: MonitorMetric;
  saturated: boolean | null;
  responseSha256: string | null;
}

function monitorMetric(body: JsonRecord, name: string, resourceId: string, start: number, end: number): MonitorMetric {
  const unavailable = (reason: string): MonitorMetric => ({ averagePercent: null, unavailableReason: reason });
  if (!Array.isArray(body.value)) return unavailable('azure_monitor_metric_list_missing');
  const matches = body.value.filter(value => record(value) && record(value.name) && value.name.value === name);
  if (matches.length !== 1) return unavailable('azure_monitor_metric_missing_or_duplicated');
  const metric = object(matches[0], 'Metric');
  if (typeof metric.id !== 'string' ||
      metric.id.toLowerCase() !== `${resourceId}/providers/Microsoft.Insights/metrics/${name}`.toLowerCase() ||
      metric.type !== 'Microsoft.Insights/metrics' || metric.unit !== 'Percent' ||
      metric.errorCode !== undefined && metric.errorCode !== 'Success')
    return unavailable('azure_monitor_metric_resource_unit_or_status_differs');
  if (!Array.isArray(metric.timeseries) || metric.timeseries.length !== 1)
    return unavailable('azure_monitor_metric_series_missing_or_ambiguous');
  const series = metric.timeseries[0];
  if (!record(series)) return unavailable('azure_monitor_metric_series_invalid');
  if (!Array.isArray(series.metadatavalues) || series.metadatavalues.length !== 0 ||
      !Array.isArray(series.data) || series.data.length !== (end - start) / MONITOR_GRAIN_MS)
    return unavailable('azure_monitor_complete_grains_missing');
  let total = 0;
  for (let index = 0; index < series.data.length; index++) {
    const point: unknown = series.data[index];
    if (!record(point)) return unavailable('azure_monitor_grain_invalid');
    const timestamp = point.timeStamp, average = point.average;
    if (typeof timestamp !== 'string' || Date.parse(timestamp) !== start + index * MONITOR_GRAIN_MS ||
        typeof average !== 'number' || !Number.isFinite(average) || average < 0 || average > 100)
      return unavailable('azure_monitor_grain_partial_misaligned_or_invalid');
    total += average;
  }
  return { averagePercent: total / series.data.length, unavailableReason: null };
}

/** Historical platform metrics for the original window; this is not scale admission or an additional workload scorer. */
export async function readOfficialKustoWindow(options: SourceOptions): Promise<OfficialKustoWindow> {
  const original = await readOfficialCellMeasurement(options), now = options.now ?? Date.now;
  const resourceId = options.azure.c.cluster_id, start = original.startMs, end = original.endMs;
  const result: OfficialKustoWindow = { original, resourceId, windowStartMs: start, windowEndMs: end,
    cpu: { averagePercent: null, unavailableReason: 'azure_monitor_not_observed' },
    ingestionUtilization: { averagePercent: null, unavailableReason: 'azure_monitor_not_observed' },
    saturated: null, responseSha256: null };
  const unavailable = (reason: string) => {
    result.cpu = { averagePercent: null, unavailableReason: reason };
    result.ingestionUtilization = { averagePercent: null, unavailableReason: reason };
    return result;
  };
  if (original.committedRowsPerSecond === null || start === null || end === null || start % MONITOR_GRAIN_MS !== 0 ||
      end % MONITOR_GRAIN_MS !== 0 || end - start < MONITOR_GRAIN_MS)
    return unavailable('original_window_cannot_be_covered_by_complete_aligned_PT1M_grains');
  if (!/^\/subscriptions\/[a-f0-9-]{36}\/resourceGroups\/[^/]+\/providers\/Microsoft\.Kusto\/clusters\/[^/]+$/.test(resourceId))
    return unavailable('exact_selected_kusto_ARM_resource_identity_unavailable');
  await assertOriginalCustody(options, original);
  const url = new URL(`https://management.azure.com${resourceId}/providers/Microsoft.Insights/metrics`);
  const timespan = new Date(start).toISOString() + '/' + new Date(end).toISOString();
  for (const [key, value] of Object.entries({ 'api-version': '2023-10-01', metricnames: 'CPU,IngestionUtilization',
    timespan, interval: 'PT1M', aggregation: 'Average', AutoAdjustTimegrain: 'false', ValidateDimensions: 'true' }))
    url.searchParams.set(key, value);
  const accessToken = await options.azure.token('https://management.azure.com/');
  await assertOriginalCustody(options, original);
  const response = await dispatchExternalEffect({ transport: 'azure-rest', method: 'GET',
    resource: 'https://management.azure.com|https://management.azure.com/', path: url.pathname + url.search },
  () => {
    assertSourceDeadline(options);
    return (options.fetcher ?? fetch)(url, { method: 'GET', redirect: 'error',
      signal: AbortSignal.timeout(Math.max(1, Math.min(30_000, options.deadlineAtMs - now()))),
      headers: { authorization: 'Bearer ' + accessToken } });
  });
  if (response.status !== 200) {
    await response.body?.cancel();
    await assertOriginalCustody(options, original);
    return unavailable('azure_monitor_HTTP_' + response.status);
  }
  const bytes = await readBoundedSourceBytes(response, 1_048_576);
  await assertOriginalCustody(options, original);
  result.responseSha256 = sha(bytes);
  let decoded: unknown;
  try { decoded = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as unknown; }
  catch { return unavailable('azure_monitor_malformed_response'); }
  if (!record(decoded)) return unavailable('azure_monitor_malformed_response');
  const body = decoded;
  if (body.interval !== 'PT1M' || typeof body.timespan !== 'string')
    return unavailable('azure_monitor_changed_timegrain_or_window');
  const parts = body.timespan.split('/');
  if (parts.length !== 2 || Date.parse(parts[0]) !== start || Date.parse(parts[1]) !== end)
    return unavailable('azure_monitor_changed_timegrain_or_window');
  result.cpu = monitorMetric(body, 'CPU', resourceId, start, end);
  result.ingestionUtilization = monitorMetric(body, 'IngestionUtilization', resourceId, start, end);
  const cpu = result.cpu.averagePercent, ingestion = result.ingestionUtilization.averagePercent;
  result.saturated = cpu !== null && cpu >= KUSTO_SATURATION_THRESHOLDS.cpuPercent ||
    ingestion !== null && ingestion >= KUSTO_SATURATION_THRESHOLDS.ingestionUtilizationPercent
    ? true : cpu !== null && ingestion !== null ? false : null;
  return result;
}

/** Authenticates original per-cell evidence, but cannot authorize a same-cell reschedule in place of explicit serial-cell pairing. */
export function createScaleEvidenceVerifier(options: Parameters<typeof readOfficialCellMeasurement>[0]): ScaleEvidenceVerifier {
  const verify = async (envelope: ScaleEvidenceEnvelope | ScaleMeasurementEnvelope, state: CampaignState,
    post?: ScaleVerificationWindow): Promise<never> => {
    const original = await readOfficialCellMeasurement(options);
    if (state.campaignId !== original.inventory.campaign_id || state.ownerId !== original.inventory.owner_id ||
        state.fencingToken !== original.inventory.fencing_token || canonical_json(original.inventory) !== canonical_json(state.runInventory) ||
        envelope.campaign_id !== state.campaignId || envelope.cell_run_id !== original.inventory.cell_run_id ||
        original.endMs !== envelope.observed_at_ms || original.startMs !== envelope.observed_at_ms - envelope.window_ms ||
        post && (original.startMs === null || original.endMs === null || original.startMs !== post.startMs ||
          original.endMs !== post.endMs || !Number.isSafeInteger(post.notBeforeMs) || original.startMs < post.notBeforeMs ||
          original.endMs > post.deadlineAtMs || post.deadlineAtMs !== options.deadlineAtMs))
      throw new Error('Official measurement does not match the selected original cell/window/deadline.');
    if (original.unavailableReasons.length) throw new Error('Official measurement is UNKNOWN: ' + original.unavailableReasons.join(','));
    throw new Error('Scale admission requires explicit paired baseline/trial cell custody; an original cell is not same-cell post-effect authorization.');
  };
  return { verifyInitial: (envelope, state) => verify(envelope, state),
    verifyMeasurement: (envelope, state, window) => verify(envelope, state, window) };
}
