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
/**
 * Original-cell qualification: 10s cadence + 3s capture; CPU >=90% of a frozen observed hard limit,
 * accounting for native timestamp precision; memory >=90% from in-window working-set samples (not peak RSS);
 * throttling >=10% of CFS periods. No authenticated link capacity means network saturation is UNKNOWN.
 */
export const NATIVE_TELEMETRY_THRESHOLDS = Object.freeze({
  cadenceMs: 10_000, requestTimeoutMs: 3000, maxGapMs: 13_000, maxEdgeAgeMs: 13_000,
  cpuLimitFraction: 0.9, memoryLimitFraction: 0.9, throttledPeriodFraction: 0.1,
});

interface NativeQapWindow {
  unit: 'pending_data_files';
  pendingDataFiles: number | null;
  pendingDataBytes: number | null;
  growing: boolean | null;
  samples: JsonRecord[];
  unavailableReason: string | null;
}
interface NativePodWindow {
  identity: JsonRecord;
  cpuCores: number | null;
  cpuCounterInterval: { startUnixNs: string; endUnixNs: string } | null;
  cpuSaturated: boolean | null;
  memorySaturated: boolean | null;
  rxBytesPerSecond: number | null;
  txBytesPerSecond: number | null;
  throttling: number | null;
  throttled: boolean | null;
  networkSaturated: null;
  unavailableReasons: string[];
}
export interface OfficialNativeWindow {
  original: OfficialCellMeasurement;
  qap: NativeQapWindow;
  pods: NativePodWindow[];
  thresholds: typeof NATIVE_TELEMETRY_THRESHOLDS;
  authorization: 'hold_explicit_paired_cell_custody_and_qualified_inputs_required';
  unavailableReasons: string[];
}
function unsigned(value: unknown): bigint | null {
  if (typeof value !== 'string' || !/^(0|[1-9]\d{0,19})$/.test(value)) return null;
  const parsed = BigInt(value);
  return parsed <= 18_446_744_073_709_551_615n ? parsed : null;
}
function quantity(value: unknown, memory: boolean): number | null {
  if (typeof value !== 'string') return null;
  const parsed = /^(\d+(?:\.\d+)?)([a-zA-Z]*)$/.exec(value);
  if (!parsed) return null;
  const factors: Record<string, number> = memory
    ? { '': 1, k: 1000, M: 1e6, G: 1e9, T: 1e12, Ki: 1024, Mi: 1024 ** 2, Gi: 1024 ** 3, Ti: 1024 ** 4 }
    : { '': 1, m: 0.001 };
  const factor = factors[parsed[2]!], result = Number(parsed[1]) * (factor ?? NaN);
  return Number.isFinite(result) && result > 0 && result <= Number.MAX_SAFE_INTEGER ? result : null;
}
function sameNativeOrigin(left: unknown, right: unknown): boolean {
  if (typeof left !== 'string' || typeof right !== 'string' || !left || !right) return false;
  try {
    const origins = [left, right].map(value => new URL(value));
    return origins.every(url => url.protocol === 'https:' && url.username === '' && url.password === '' &&
      url.pathname === '/' && url.search === '' && url.hash === '') && origins[0]!.origin === origins[1]!.origin;
  } catch { return false; }
}
function nativeSeries(rows: JsonRecord[], key: string, start: number, end: number,
  counter: string): { first: bigint; last: bigint; firstNs: bigint; lastNs: bigint; firstUpperNs: bigint; lastUpperNs: bigint } | null {
  const series = rows.map(row => {
    const metric = record(row[key]) ? row[key] : {};
    const identity = record(row.identity) ? row.identity : {};
    return { time: unsigned(metric.time_unix_ns), resolution: unsigned(metric.time_resolution_ns),
      value: unsigned(metric[counter]), epoch: unsigned(identity.started_unix_ns),
      requestEndNs: BigInt(Number(row.source_request_end_ms)) * 1_000_000n };
  });
  if (series.length < 2 || series.some(point => point.time === null || point.value === null ||
      point.resolution === null || point.resolution < 1n || point.resolution > 1_000_000_000n ||
      point.epoch === null || point.time < point.epoch || point.time > point.requestEndNs)) return null;
  const first = series[0]!, last = series.at(-1)!;
  const upper = (point: typeof first) => point.time! + point.resolution! < point.requestEndNs
    ? point.time! + point.resolution! : point.requestEndNs;
  const startNs = BigInt(start) * 1_000_000n, endNs = BigInt(end) * 1_000_000n;
  const bound = BigInt(NATIVE_TELEMETRY_THRESHOLDS.maxEdgeAgeMs) * 1_000_000n;
  if (upper(first) > startNs || first.time! < startNs - bound || last.time! < endNs ||
      upper(last) > endNs + bound || last.time! <= upper(first))
    return null;
  for (let i = 1; i < series.length; i++) {
    const before = series[i - 1]!, after = series[i]!;
    if (after.time! <= before.time! || after.time! - before.time! > bound || after.value! < before.value!) return null;
  }
  return { first: first.value!, last: last.value!, firstNs: first.time!, lastNs: last.time!,
    firstUpperNs: upper(first), lastUpperNs: upper(last) };
}
function nativeRate(series: ReturnType<typeof nativeSeries>, bytes: boolean): number | null {
  if (!series || series.last - series.first > BigInt(Number.MAX_SAFE_INTEGER)) return null;
  const rate = Number(series.last - series.first) / Number(series.lastNs - series.firstNs) * (bytes ? 1e9 : 1);
  return Number.isFinite(rate) ? rate : null;
}

/** Reads producer-retained native observations only through current authenticated immutable original-cell custody. */
export async function readOfficialNativeWindow(options: SourceOptions): Promise<OfficialNativeWindow> {
  const original = await readOfficialCellMeasurement(options), inventory = original.inventory;
  const qap: NativeQapWindow = { unit: 'pending_data_files', pendingDataFiles: null, pendingDataBytes: null,
    growing: null, samples: [], unavailableReason: 'original_cell_native_qap_history_unavailable' };
  const result: OfficialNativeWindow = { original, qap, pods: [], thresholds: NATIVE_TELEMETRY_THRESHOLDS,
    authorization: 'hold_explicit_paired_cell_custody_and_qualified_inputs_required', unavailableReasons: [] };
  const evidence = object(original.officialResult.evidence, 'Original official evidence');
  const history = record(evidence.native_telemetry) ? evidence.native_telemetry : {};
  const binding = record(history.binding) ? history.binding : {};
  const start = original.startMs, end = original.endMs;
  if (start === null || end === null || original.committedRowsPerSecond === null ||
      history.schema_version !== 1 || history.scope !== 'original_cell_source_observations_not_scale_authorization' ||
      history.window_start_unix_ms !== start || history.window_end_unix_ms !== end ||
      history.cadence_ms !== NATIVE_TELEMETRY_THRESHOLDS.cadenceMs || history.sample_timeout_ms !== 3000 ||
      binding.campaign_id !== inventory.campaign_id || binding.owner_id !== inventory.owner_id ||
      binding.fencing_token !== inventory.fencing_token || binding.cell_run_id !== inventory.cell_run_id ||
      !inventory.namespace.uid || binding.namespace_uid !== inventory.namespace.uid ||
      binding.namespace !== inventory.namespace.name || history.samples_dropped !== 0 ||
      !Array.isArray(history.samples) || history.samples.length < 2 || history.samples.length > 1024) {
    result.unavailableReasons.push('original_cell_native_history_binding_or_complete_window_unavailable');
    return result;
  }
  const frames = history.samples.filter(record);
  if (frames.length !== history.samples.length || frames.some((frame, i) =>
    !integer(frame.request_start_unix_ms) || !integer(frame.request_end_unix_ms) ||
    frame.request_end_unix_ms < frame.request_start_unix_ms ||
    frame.request_end_unix_ms - frame.request_start_unix_ms > NATIVE_TELEMETRY_THRESHOLDS.requestTimeoutMs ||
    frame.request_end_unix_ms > (options.now ?? Date.now)() ||
    i > 0 && frame.request_start_unix_ms < Number(frames[i - 1]!.request_end_unix_ms))) {
    result.unavailableReasons.push('original_cell_native_source_intervals_invalid');
    return result;
  }
  const first = frames[0]!, last = frames.at(-1)!;
  const complete = Number(first.request_end_unix_ms) <= start &&
    start - Number(first.request_end_unix_ms) <= NATIVE_TELEMETRY_THRESHOLDS.maxEdgeAgeMs &&
    Number(last.request_start_unix_ms) >= end &&
    Number(last.request_start_unix_ms) - end <= NATIVE_TELEMETRY_THRESHOLDS.maxEdgeAgeMs &&
    frames.every((frame, i) => i === 0 || Number(frame.request_start_unix_ms) -
      Number(frames[i - 1]!.request_end_unix_ms) <= NATIVE_TELEMETRY_THRESHOLDS.maxGapMs);
  if (!complete) {
    result.unavailableReasons.push('native_sources_do_not_bracket_the_complete_original_window');
    return result;
  }
  const typed = object(original.effectiveConfig.typed_oat_config, 'Original typed effective config');
  const admittedQap = inventory.qap;
  if (admittedQap?.bindings.length === 1 && admittedQap.reference && options.azure.c.uri &&
      binding.qap_endpoint === typed.qap_endpoint && sameNativeOrigin(binding.qap_endpoint, options.azure.c.uri) &&
      binding.qap_database === admittedQap.database &&
      canonical_json(binding.qap_bindings) === canonical_json(admittedQap.bindings) &&
      canonical_json(binding.qap_reference) === canonical_json(admittedQap.reference)) {
    const name = admittedQap.bindings[0]!.external_table;
    const samples = frames.map(frame => {
      const observed = record(frame.qap) ? frame.qap : {}, tables = record(observed.tables) ? observed.tables : {};
      const table = record(tables[name]) ? tables[name] : {};
      const stats = record(table.statistics) ? table.statistics : {}, state = record(table.state) ? table.state : {};
      return { requestStartMs: Number(frame.request_start_unix_ms), requestEndMs: Number(frame.request_end_unix_ms),
        observedAtMs: observed.observed_unix_ms, available: table.available,
        pendingDataFiles: stats.AccelerationPendingDataFilesCount, pendingDataBytes: stats.AccelerationPendingDataFilesSize,
        completePercentage: stats.AccelerationCompletePercentage, lastUpdatedDateTime: stats.LastUpdatedDateTime,
        eligible: stats.Eligible, healthy: state.IsHealthy, notHealthyReason: state.NotHealthyReason };
    });
    qap.samples = samples;
    if (samples.every(sample => sample.available === true && sample.eligible === true && sample.healthy === true &&
        (sample.notHealthyReason === null || sample.notHealthyReason === '') &&
        integer(sample.pendingDataFiles) && integer(sample.observedAtMs) &&
        sample.observedAtMs >= sample.requestStartMs && sample.observedAtMs <= sample.requestEndMs &&
        typeof sample.completePercentage === 'number' && Number.isFinite(sample.completePercentage) &&
        sample.completePercentage >= 0 && sample.completePercentage <= 100)) {
      const inside = samples.filter(sample => sample.requestStartMs >= start && sample.requestEndMs <= end);
      if (inside.length >= 2) {
        qap.pendingDataFiles = Number(inside.at(-1)!.pendingDataFiles);
        qap.pendingDataBytes = integer(inside.at(-1)!.pendingDataBytes) ? Number(inside.at(-1)!.pendingDataBytes) : null;
        qap.growing = qap.pendingDataFiles > Number(inside[0]!.pendingDataFiles);
        qap.unavailableReason = null;
      }
    }
  }
  const plan = inventory.fic?.plan;
  if (!plan || binding.server !== plan.server || typeof binding.kube_context !== 'string' ||
      binding.kube_context !== typed.kubectl_context || !Array.isArray(history.pod_targets)) {
    result.unavailableReasons.push('exact_api_server_context_or_original_pod_targets_unavailable');
    return result;
  }
  const resources = record(original.effectiveConfig.resolved_aks_resources) ? original.effectiveConfig.resolved_aks_resources : {};
  const targets = history.pod_targets.filter(record);
  if (targets.length !== history.pod_targets.length || targets.length === 0 || targets.length > 64 ||
      new Set(targets.map(target => canonical_json(target))).size !== targets.length) {
    result.unavailableReasons.push('original_pod_identity_set_incomplete_or_ambiguous');
    return result;
  }
  for (const target of targets) {
    const pod: NativePodWindow = { identity: target, cpuCores: null, cpuCounterInterval: null,
      cpuSaturated: null, memorySaturated: null,
      rxBytesPerSecond: null, txBytesPerSecond: null, throttling: null, throttled: null, networkSaturated: null,
      unavailableReasons: ['authenticated_network_link_capacity_unavailable'] };
    result.pods.push(pod);
    const rows = frames.map(frame => Array.isArray(frame.pods) ? frame.pods.filter(record).filter(row => {
      const identity = record(row.identity) ? row.identity : {};
      return identity.namespace === inventory.namespace.name &&
        ['role', 'pod', 'uid', 'node', 'container', 'container_id'].every(key => identity[key] === target[key]);
    }) : []);
    if (!['collector', 'client', 'router', 'catalog'].includes(String(target.role)) ||
        ['pod', 'uid', 'node', 'container', 'container_id'].some(key =>
          typeof target[key] !== 'string' || target[key] === '' || String(target[key]).length > 1024) ||
        rows.some(rows => rows.length !== 1) || rows.some(rows => !record(rows[0]!.identity))) {
      pod.unavailableReasons.push('original_pod_uid_node_container_id_missing_changed_or_ambiguous');
      continue;
    }
    const observations: Record<string, unknown>[] = rows.map((rows, i) =>
      ({ ...rows[0]!, source_request_end_ms: frames[i]!.request_end_unix_ms }));
    const firstIdentity = object(observations[0]!.identity, 'Native pod identity');
    const epoch = unsigned(firstIdentity.started_unix_ns);
    if (epoch === null || epoch === 0n) {
      pod.unavailableReasons.push('native_container_epoch_unavailable');
      continue;
    }
    if (observations.some(row => canonical_json(row.identity) !== canonical_json(observations[0]!.identity) ||
        canonical_json(row.limits) !== canonical_json(observations[0]!.limits))) {
      pod.unavailableReasons.push('pod_container_epoch_or_limits_changed_inside_original_window');
      continue;
    }
    const limits = record(observations[0]!.limits) ? observations[0]!.limits : {};
    const resource = typeof target.role === 'string' && record(resources[target.role]) ? resources[target.role] : {};
    const expected = record(resource.limits) ? resource.limits : {};
    const cpuLimit = quantity(limits.cpu, false), memoryLimit = quantity(limits.memory, true);
    const cpu = nativeSeries(observations, 'cpu', start, end, 'usage_core_nanoseconds');
    pod.cpuCores = nativeRate(cpu, false);
    if (cpu) pod.cpuCounterInterval = { startUnixNs: cpu.firstNs.toString(), endUnixNs: cpu.lastNs.toString() };
    if (cpuLimit !== null && cpuLimit === quantity(expected.cpu, false) && pod.cpuCores !== null && cpu) {
      const lower = Number(cpu.last - cpu.first) / Number(cpu.lastUpperNs - cpu.firstNs) / cpuLimit;
      const upper = Number(cpu.last - cpu.first) / Number(cpu.lastNs - cpu.firstUpperNs) / cpuLimit;
      pod.cpuSaturated = lower >= NATIVE_TELEMETRY_THRESHOLDS.cpuLimitFraction ? true :
        upper < NATIVE_TELEMETRY_THRESHOLDS.cpuLimitFraction ? false : null;
      if (pod.cpuSaturated === null) pod.unavailableReasons.push('native_cpu_timestamp_precision_straddles_saturation_threshold');
    }
    else pod.unavailableReasons.push('complete_native_cpu_counter_window_or_frozen_hard_limit_unavailable');
    const memory = observations.map(row => record(row.memory) ? row.memory : {});
    const memoryTimes = memory.map(value => unsigned(value.time_unix_ns));
    const memoryResolution = memory.map(value => unsigned(value.time_resolution_ns));
    const memoryValues = memory.map(value => unsigned(value.working_set_bytes));
    const startNs = BigInt(start) * 1_000_000n, endNs = BigInt(end) * 1_000_000n;
    const edgeNs = BigInt(NATIVE_TELEMETRY_THRESHOLDS.maxEdgeAgeMs) * 1_000_000n;
    const memoryUpper = memoryTimes.map((time, i) => time === null || memoryResolution[i] === null ? null :
      time + memoryResolution[i]! < BigInt(Number(frames[i]!.request_end_unix_ms)) * 1_000_000n ?
        time + memoryResolution[i]! : BigInt(Number(frames[i]!.request_end_unix_ms)) * 1_000_000n);
    const insideMemory = memoryValues.filter((_, i) => memoryTimes[i] !== null &&
      memoryTimes[i]! >= startNs && memoryUpper[i] !== null && memoryUpper[i]! <= endNs);
    if (memoryLimit !== null && memoryLimit === quantity(expected.memory, true) &&
        memoryTimes.every((time, i) => time !== null &&
          time >= epoch &&
          memoryResolution[i] !== null && memoryResolution[i]! >= 1n && memoryResolution[i]! <= 1_000_000_000n &&
          time <= BigInt(Number(frames[i]!.request_end_unix_ms)) * 1_000_000n &&
          (i === 0 || time > memoryTimes[i - 1]! && time - memoryTimes[i - 1]! <= 13_000_000_000n)) &&
        memoryValues.every(value => value !== null && value <= BigInt(Number.MAX_SAFE_INTEGER)) &&
        memoryUpper[0]! <= startNs && memoryTimes[0]! >= startNs - edgeNs &&
        memoryTimes.at(-1)! >= endNs && memoryUpper.at(-1)! <= endNs + edgeNs && insideMemory.length > 0)
      pod.memorySaturated = Math.max(...insideMemory.map(Number)) / memoryLimit >= NATIVE_TELEMETRY_THRESHOLDS.memoryLimitFraction;
    else pod.unavailableReasons.push('complete_native_memory_window_or_frozen_hard_limit_unavailable');
    if (observations.every(row => record(row.network) && row.network.scope === 'pod_network_not_container_or_link_capacity')) {
      pod.rxBytesPerSecond = nativeRate(nativeSeries(observations, 'network', start, end, 'rx_bytes'), true);
      pod.txBytesPerSecond = nativeRate(nativeSeries(observations, 'network', start, end, 'tx_bytes'), true);
    }
    const periods = nativeSeries(observations, 'throttling', start, end, 'periods');
    const throttled = nativeSeries(observations, 'throttling', start, end, 'throttled_periods');
    const validCfs = observations.every(row => {
      const metric = record(row.throttling) ? row.throttling : {};
      const total = unsigned(metric.periods), throttled = unsigned(metric.throttled_periods);
      return total !== null && throttled !== null && throttled <= total && total <= BigInt(Number.MAX_SAFE_INTEGER);
    });
    if (validCfs && periods && throttled && periods.firstNs === throttled.firstNs && periods.lastNs === throttled.lastNs &&
        periods.last > periods.first && throttled.last - throttled.first <= periods.last - periods.first) {
      pod.throttling = Number(throttled.last - throttled.first) / Number(periods.last - periods.first);
      pod.throttled = pod.throttling >= NATIVE_TELEMETRY_THRESHOLDS.throttledPeriodFraction;
    } else pod.unavailableReasons.push('original_window_source_timestamped_exact_cgroup_cfs_unavailable');
  }
  return result;
}

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
