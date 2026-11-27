import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { lstat, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { analyzeCoverage, type CoverageCohort, type CoverageInput, type CoverageObservation,
  type ExtentProvenance, type QapCoverageState, type SourceSnapshot } from '../../experiments/azure-qap/src/coverage.js';
import { object, tableRows, tables, UNACCELERATED_EXTENT } from '../../experiments/azure-qap/src/kusto.js';
import { canonical_json, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { parseDeltaVisibility } from './aks-qap-observer.js';
import { profileFromCluster, type ClusterProfile } from './aks-qap-cluster.js';
import { writePrivateFile } from './runtime.js';

type Row = Record<string, unknown>;
const PINNED_MANIFEST_SHA256 = '25070a04bec2a2cdf84efdb62fa4162b214558235cc6a253b5144877ed53892b';
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
function requireThat(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
function integer(value: unknown, label: string): number {
  requireThat(typeof value === 'number' && Number.isSafeInteger(value) && value >= 0, `${label} must be a measured nonnegative integer.`);
  return value;
}
function array(value: unknown, label: string): unknown[] {
  requireThat(Array.isArray(value), `${label} must be an array.`);
  return value;
}
function dynamic(value: unknown): Row { return object(typeof value === 'string' ? JSON.parse(value) : value); }
function safeSum(values: number[]): number { return integer(values.reduce((sum, value) => sum + value, 0), 'Total'); }

export function queryProvenance(value: unknown, expectedRequestId: string): ExtentProvenance {
  requireThat(Array.isArray(value), 'Coverage requires the complete v2 query frame sequence.');
  const frames = tables(value);
  const results = frames.filter(frame => frame.kind === 'PrimaryResult');
  requireThat(results.length === 1, 'Expected one whole-source extent result.');
  const result = results[0]!;
  requireThat(result.columns.length === 2 && result.columns.includes('Rows') && result.columns.includes('Extent'),
    'Expected the unfiltered whole-source extent-count query.');
  const extents = new Set<string>();
  let rows = 0, nativeRows = 0;
  for (const row of tableRows(result)) {
    const count = integer(row.Rows, 'Extent rows');
    requireThat(count > 0, 'A grouped extent must contain rows.');
    requireThat(typeof row.Extent === 'string' &&
      /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(row.Extent), 'Extent identity is absent or malformed.');
    const extent = row.Extent.toLowerCase();
    requireThat(!extents.has(extent), 'Duplicate extent identity in a grouped result.');
    extents.add(extent);
    rows = safeSum([rows, count]);
    if (extent !== UNACCELERATED_EXTENT) nativeRows = safeSum([nativeRows, count]);
  }
  const completions = frames.filter(frame => frame.kind === 'QueryCompletionInformation').flatMap(tableRows);
  requireThat(completions.length && completions.every(row => row.ClientRequestId === expectedRequestId),
    'Query completion is not bound to the observer request.');
  const warnings = completions.filter(row => row.LevelName === 'Warning' || row.StatusCode !== 0)
    .map(row => `QUERY_STATUS_${String(row.StatusCode)}`);
  const resourceRows = completions.filter(row => row.EventTypeName === 'QueryResourceConsumption' &&
    row.LevelName === 'Stats' && row.StatusCode === 0);
  requireThat(resourceRows.length === 1, 'Native query resource-consumption evidence is missing or ambiguous.');
  const statistics = dynamic(resourceRows[0]!.Payload);
  const input = object(statistics.input_dataset_statistics);
  const inputRows = object(input.rows), inputExtents = object(input.extents);
  const totalNativeRows = integer(inputRows.total, 'Native input rows');
  const totalNativeExtents = integer(inputExtents.total, 'Native input extents');
  const nativeExtentCount = extents.size - (extents.has(UNACCELERATED_EXTENT) ? 1 : 0);
  const exactNativeInput = totalNativeRows === rows && totalNativeExtents === nativeExtentCount && nativeExtentCount > 0;
  let externalDataRead: boolean;
  if (input.external_data !== undefined) {
    const external = object(input.external_data);
    externalDataRead = ['downloaded_items', 'downloaded_bytes', 'iterated_artifacts']
      .map(key => integer(external[key], `External ${key}`)).some(count => count > 0);
  } else {
    // Missing external counters are only admissible with positive, exact native-input provenance.
    requireThat(exactNativeInput, 'Absent external counters do not establish zero fallback.');
    externalDataRead = false;
  }
  if (!externalDataRead && !extents.has(UNACCELERATED_EXTENT))
    requireThat(exactNativeInput, 'Native input statistics do not reconcile with the whole-source extent result.');
  const cache = object(object(statistics.resource_usage).cache);
  const shards = object(cache.shards);
  if (cache.results_cache_origin || shards.results_cache_origin) warnings.push('QUERY_RESULTS_CACHE_ORIGIN');
  return { rows, rowsWithValidExtentId: nativeRows, distinctExtentCount: nativeExtentCount,
    fallbackDetected: extents.has(UNACCELERATED_EXTENT) || warnings.length > 0,
    externalDataRead, partial: false, warnings };
}

function coverageState(value: unknown): QapCoverageState | undefined {
  if (value === undefined) return undefined;
  const row = object(value);
  return {
    version: typeof row.version === 'number' && row.version >= 0 ? row.version : undefined,
    completionPercent: typeof row.completionPercent === 'number' && row.completionPercent >= 0 ? row.completionPercent : undefined,
    pendingFiles: typeof row.pendingFiles === 'number' && row.pendingFiles >= 0 ? row.pendingFiles : undefined,
    pendingBytes: typeof row.pendingBytes === 'number' && row.pendingBytes >= 0 ? row.pendingBytes : undefined,
    acceleratedRows: typeof row.acceleratedRows === 'number' && row.acceleratedRows >= 0 ? row.acceleratedRows : undefined,
    healthy: typeof row.healthy === 'boolean' ? row.healthy : undefined,
  };
}

export function coverageObservation(value: unknown): CoverageObservation {
  const row = object(value);
  const result: CoverageObservation = { startedAtMs: integer(row.startedAtMs, 'Observation start'),
    endedAtMs: integer(row.endedAtMs, 'Observation end'),
    before: coverageState(row.before), after: coverageState(row.after) };
  const errors: string[] = [];
  if (row.error) errors.push('RAW_OBSERVATION_FAILED');
  if (row.query) {
    try {
      const query = object(row.query);
      const started = integer(query.startedAtMs, 'Query start'), ended = integer(query.endedAtMs, 'Query end');
      const beforeAt = Date.parse(String(object(row.before).at)), afterAt = Date.parse(String(object(row.after).at));
      requireThat(result.startedAtMs <= beforeAt && beforeAt <= started && started <= ended &&
        ended <= afterAt && afterAt <= result.endedAtMs, 'Before/query/after envelope is not complete and ordered.');
      requireThat(typeof query.clientRequestId === 'string', 'Query request identity is missing.');
      result.provenance = queryProvenance(query.response, query.clientRequestId);
    } catch (error) { errors.push(error instanceof Error ? error.message : 'Query provenance unavailable.'); }
  }
  if (errors.length) result.telemetryErrors = errors;
  return result;
}

export function buildCoverageInput(sourceValue: unknown, observationValues: unknown[], visibilityValues: unknown[],
  sourceFinalized: boolean): CoverageInput {
  const receipt = object(sourceValue), source = object(receipt.source), reference = object(source.source_reference);
  const finalVersion = integer(source.delta_version, 'Final Delta version');
  requireThat(finalVersion < 20_000 && reference.observation === 'table' && reference.version === finalVersion,
    'Terminal source version is missing or outside the bounded replay contract.');
  const files = array(reference.active_files, 'Active files').map(object);
  const fileRows = files.map(file => integer(file.num_records, 'Active-file rows'));
  const fileBytes = files.map(file => integer(file.size, 'Active-file bytes'));
  requireThat(new Set(files.map(file => file.path)).size === files.length &&
    files.every(file => typeof file.path === 'string' && file.path.length > 0), 'Active file inventory is not unique.');
  requireThat(reference.active_file_count === files.length && reference.active_bytes === safeSum(fileBytes) &&
    reference.row_count === safeSum(fileRows) && source.active_rows === reference.row_count, 'Final file inventory does not reconcile.');
  const provenance = object(reference.provenance);
  const prefix = `${String(reference.table_root)}/_delta_log/`;
  const versions = new Set<number>();
  for (const value of array(provenance.source_objects, 'Replay source objects')) {
    const file = object(value);
    requireThat(typeof file.name === 'string' && typeof file.sha256 === 'string' && /^[a-f0-9]{64}$/.test(file.sha256) &&
      typeof file.etag === 'string' && file.etag.length > 0 && integer(file.bytes, 'Replay object bytes') > 0,
    'Replay object binding is incomplete.');
    if (!file.name.startsWith(prefix)) throw new Error('Replay object escaped its source log prefix.');
    const match = /^(\d{20})\.json$/.exec(file.name.slice(prefix.length));
    if (match) {
      const version = Number(match[1]);
      requireThat(!versions.has(version), 'Replay contains a duplicate JSON version.');
      versions.add(version);
    }
  }
  requireThat(versions.size === finalVersion + 1 &&
    [...versions].every(version => Number.isSafeInteger(version) && version >= 0 && version <= finalVersion),
  'Replay does not bind every cumulative version from zero.');
  const writers = new Map<number, Row>();
  const tableIds = new Set<string>();
  for (const value of array(receipt.writerRecords, 'Writer records')) {
    const row = object(value), version = integer(row.version, 'Writer version');
    requireThat(!writers.has(version) && typeof row.table_id === 'string', 'Writer identity/version is ambiguous.');
    writers.set(version, row); tableIds.add(row.table_id);
  }
  requireThat(tableIds.size === 1, 'Writer records span different logical tables.');
  const cohorts: CoverageCohort[] = array(source.commits, 'Delta Add commits').map(value => {
    const row = object(value), version = integer(row.version, 'Cohort version');
    const writer = writers.get(version);
    requireThat(writer && writer.rows === row.rows && writer.files === row.files, 'A Delta cohort lacks its matching durable writer record.');
    return { version, rows: integer(row.rows, 'Cohort rows'), files: integer(row.files, 'Cohort files'),
      bytes: integer(row.bytes, 'Cohort bytes'), finishedAtMs: integer(writer.finished_at_unix_ms, 'Writer finish') };
  }).sort((a, b) => a.version - b.version);
  const byVersion = new Map(cohorts.map(cohort => [cohort.version, cohort]));
  requireThat(byVersion.size === cohorts.length && cohorts.every(cohort => cohort.version <= finalVersion),
    'Duplicate or future source cohort.');
  const sourceSnapshots: SourceSnapshot[] = [];
  let rows = 0, count = 0, bytes = 0;
  const maxFileBytes = fileBytes.length ? Math.max(...fileBytes) : 0;
  for (let version = 0; version <= finalVersion; version++) {
    const cohort = byVersion.get(version);
    rows = safeSum([rows, cohort?.rows ?? 0]); count = safeSum([count, cohort?.files ?? 0]);
    bytes = safeSum([bytes, cohort?.bytes ?? 0]);
    // Neither the final file maximum nor the prefix's total bytes can understate its largest file.
    sourceSnapshots.push({ version, rows, files: count, bytes, maxFileBytes: Math.min(bytes, maxFileBytes), removeCount: 0,
      inventoryComplete: fileRows.every(value => value > 0) });
  }
  requireThat(rows === reference.row_count && count === files.length && bytes === reference.active_bytes,
    'Cumulative cohort inventory differs from the final active source.');
  const summaries = receipt.sourceVisibility === undefined ? [] : array(receipt.sourceVisibility, 'Visibility summaries');
  requireThat(summaries.length === visibilityValues.length, 'Raw visibility receipts and source summaries differ.');
  const validVisibility: { startedAtMs: number; endedAtMs: number; version: number }[] = [];
  let lastEnd = 0, lastVersion = -1;
  visibilityValues.forEach((value, index) => {
    const row = object(value), summary = object(summaries[index]);
    const { receipt: rawReceipt, ...recordedSummary } = row;
    requireThat(canonical_json(recordedSummary) === canonical_json(summary), 'Visibility summary lost its raw receipt binding.');
    const startedAtMs = integer(row.startedAtMs, 'Visibility start'), endedAtMs = integer(row.endedAtMs, 'Visibility end');
    requireThat(startedAtMs >= lastEnd && endedAtMs >= startedAtMs, 'Visibility requests are not ordered.');
    lastEnd = endedAtMs;
    if (row.error) return;
    const raw = object(rawReceipt);
    requireThat(typeof raw.bodyBase64 === 'string', 'Visibility HTTP body is missing.');
    const headers = object(raw.headers);
    requireThat(Object.values(headers).every(value => typeof value === 'string'), 'Invalid visibility headers.');
    const parsed = parseDeltaVisibility({ status: integer(raw.status, 'Visibility HTTP status'),
      bodyBase64: raw.bodyBase64, headers: headers as HttpReceipt['headers'] }, prefix);
    requireThat(parsed.version === row.version && parsed.logFiles === row.logFiles && parsed.requestId === row.requestId &&
      parsed.responseSha256 === row.responseSha256 && parsed.version! >= lastVersion && parsed.version! <= finalVersion,
    'Visibility receipt is inconsistent with the terminal source.');
    lastVersion = parsed.version!;
    validVisibility.push({ startedAtMs, endedAtMs, version: parsed.version! });
  });
  for (const cohort of cohorts) {
    const firstPresent = validVisibility.findIndex(observation => observation.version >= cohort.version);
    if (firstPresent < 0) continue;
    const absent = validVisibility.slice(0, firstPresent).filter(observation => observation.version < cohort.version).at(-1);
    if (absent) cohort.commitBounds = { lowerMs: absent.startedAtMs, upperMs: validVisibility[firstPresent]!.endedAtMs };
  }
  const measurementStartMs = integer(receipt.measurementStartMs, 'Measurement start');
  const measurementEndMs = integer(receipt.measurementEndMs, 'Measurement end');
  const warmupMs = integer(receipt.warmupMs, 'Warmup duration');
  requireThat(typeof source.append_only === 'boolean', 'Append-only attestation is missing.');
  return { cohorts, sourceSnapshots, observations: observationValues.map(coverageObservation),
    warmupStartMs: measurementStartMs - warmupMs, measurementStartMs, measurementEndMs,
    producerMeasurementWindow: { startMs: measurementStartMs, endMs: measurementEndMs },
    producerStoppedAtMs: measurementEndMs,
    clockErrorMs: null, sourceFinalized, sourceAppendOnly: source.append_only,
    terminalSource: { version: finalVersion, rows, files: count },
    policy: { steadyStateStartMs: integer(receipt.steadyStateStartMs, 'Steady-state start'),
      minCompleteWatermarks: 3, boundaryToleranceMs: 30_000, maxObservationGapMs: 45_000,
      maxSteadyStateDeficitRowsPerSecond: 0, maxBacklogGrowthRowsPerSecond: 0, maxBacklogIncreaseRows: 0 } };
}

export function resourceTrace(observations: unknown[], startMs: number, endMs: number, metricsValue?: unknown) {
  const reasons: string[] = [];
  const clusters: { startedAtMs: number; atMs: number; profile: ClusterProfile }[] = [];
  const capacity: { atMs: number; total: number; consumed: number }[] = [];
  const locations = new Set<string>();
  try {
    for (const value of observations) {
      const row = object(value);
      if (row.cluster) {
        locations.add(String(object(row.cluster).location).trim().toLowerCase().replace(/\s+/g, ''));
        clusters.push({ startedAtMs: integer(row.startedAtMs, 'Cluster sample start'),
          atMs: integer(row.endedAtMs, 'Cluster sample end'), profile: profileFromCluster(row.cluster) });
      }
      if (row.capacity) {
        const frame = tables(row.capacity)[0];
        const records = frame ? tableRows(frame).filter(item => item.Resource === 'QueryAcceleration') : [];
        requireThat(records.length === 1, 'QAP capacity resource is missing or duplicated.');
        capacity.push({ atMs: integer(row.endedAtMs, 'Capacity sample time'),
          total: integer(records[0]!.Total, 'QAP capacity'), consumed: integer(records[0]!.Consumed, 'QAP consumed capacity') });
      }
    }
  } catch (error) { reasons.push(error instanceof Error ? error.message : 'Resource trace is unavailable.'); }
  const covers = (times: number[], maxGap: number) => times.length >= 2 && times[0]! <= startMs + maxGap &&
    times.at(-1)! >= endMs - maxGap && times.every((at, index) => index === 0 ||
      at >= times[index - 1]! && at - times[index - 1]! <= maxGap);
  const scoped = <T extends { atMs: number }>(rows: T[]) => rows.filter(row => row.atMs >= startMs - 45_000 && row.atMs <= endMs + 45_000);
  if (!covers(scoped(clusters).map(row => row.atMs), 90_000)) reasons.push('CLUSTER_TRACE_INCOMPLETE');
  if (!covers(scoped(capacity).map(row => row.atMs), 45_000)) reasons.push('QAP_CAPACITY_TRACE_INCOMPLETE');
  const metricSummaries: { name: string; samples: number; average: number | null; maximum: number | null }[] = [];
  let engineCost = null;
  try {
    requireThat(metricsValue !== undefined, 'AZURE_METRICS_MISSING');
    const envelope = object(metricsValue), metrics = array(object(envelope.response).value, 'Azure metrics').map(object);
    for (const name of ['CPU', 'CacheUtilization', 'InstanceCount']) {
      const matching = metrics.filter(metric => object(metric.name).value === name);
      requireThat(matching.length === 1, `${name} metric is missing or duplicated.`);
      const points = array(matching[0]!.timeseries, 'Metric time series').flatMap(series => array(object(series).data, 'Metric points'))
        .map(object).filter(point => {
          const at = Date.parse(String(point.timeStamp));
          return Number.isFinite(at) && at >= Math.floor(startMs / 60_000) * 60_000 && at <= endMs;
        });
      const valid = points.filter(point => typeof point.average === 'number' && Number.isFinite(point.average) &&
        typeof point.maximum === 'number' && Number.isFinite(point.maximum));
      const times = valid.map(point => Date.parse(String(point.timeStamp)));
      if (valid.length < Math.max(1, Math.floor((endMs - startMs) / 60_000) - 1) || !covers(times, 120_000))
        reasons.push(`${name.toUpperCase()}_METRIC_WINDOW_INCOMPLETE`);
      metricSummaries.push({ name, samples: valid.length,
        average: valid.length ? valid.reduce((sum, point) => sum + Number(point.average), 0) / valid.length : null,
        maximum: valid.length ? Math.max(...valid.map(point => Number(point.maximum))) : null });
      if (name === 'InstanceCount' && locations.size === 1 && locations.has('canadacentral') &&
          !reasons.includes('CLUSTER_TRACE_INCOMPLETE')) {
        const profile = scoped(clusters)[0]?.profile;
        const sku = profile?.sku;
        const cores = /^Standard_E(4|8|16|32)ads_v5$/.exec(sku ?? '')?.[1];
        const fullStart = Math.ceil(startMs / 60_000) * 60_000, fullEnd = Math.floor(endMs / 60_000) * 60_000;
        const fullPoints = valid.filter(point => {
          const at = Date.parse(String(point.timeStamp));
          return at >= fullStart && at < fullEnd;
        });
        if (cores && profile && scoped(clusters).every(sample => sample.profile.sku === sku &&
            sample.profile.autoscale.maximum === profile.autoscale.maximum) &&
            fullPoints.length > 0 && fullPoints.length === (fullEnd - fullStart) / 60_000 &&
            fullPoints.every((point, index) => Date.parse(String(point.timeStamp)) === fullStart + index * 60_000 &&
              Number(point.average) >= 2 && Number(point.average) <= Number(point.maximum) &&
              Number(point.maximum) <= profile.autoscale.maximum)) {
          const nodeSeconds = fullPoints.reduce((sum, point) => sum + Number(point.average) * 60, 0);
          const usdPerVcpuHour = 0.182;
          engineCost = { startMs: fullStart, endMs: fullEnd, nodeSeconds, usdPerVcpuHour,
            estimatedUsd: nodeSeconds * Number(cores) * usdPerVcpuHour / 3600,
            basis: 'Canada Central Eadsv5 consumption VM plus standard ADX engine markup; full sampled minutes only, not a bill.' };
        }
      }
    }
  } catch (error) { reasons.push(error instanceof Error ? error.message : 'Azure metrics are unavailable.'); }
  const nodeTransitions = clusters.slice(1).flatMap((sample, index) => {
    const previous = clusters[index]!;
    return sample.profile.nodes === previous.profile.nodes && sample.profile.sku === previous.profile.sku ? [] : [{
      lowerMs: previous.startedAtMs, upperMs: sample.atMs, before: previous.profile, after: sample.profile,
    }];
  });
  return { complete: reasons.length === 0, reasons, clusters, capacity, nodeTransitions, engineCost,
    balancingEvidence: 'Per-node balancing is not exposed by the collected cluster metrics.', metricSummaries };
}

async function readOptional(path: string): Promise<string | undefined> {
  try { await lstat(path); }
  catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return undefined;
    throw error;
  }
  return readFile(path, 'utf8');
}
const jsonLines = (text: string): unknown[] => text.trim() ? text.trim().split('\n').map(line => JSON.parse(line)) : [];

async function main(args: string[]): Promise<void> {
  requireThat(args.length === 1, 'Usage: npx --no-install tsx projects/arrowcol-oat/aks-qap-score.ts RUN_DIRECTORY');
  const directory = resolve(args[0]!);
  const sourceText = await readFile(resolve(directory, 'qap-source.json'), 'utf8');
  const observationsText = await readFile(resolve(directory, 'qap-observations.jsonl'), 'utf8');
  const visibilityText = await readOptional(resolve(directory, 'qap-delta-visibility.jsonl'));
  const stopText = await readOptional(resolve(directory, 'qap-writer-stop.json'));
  const reportText = await readFile(resolve(directory, 'report.json'), 'utf8');
  const metricsText = await readOptional(resolve(directory, 'qap-azure-metrics.json'));
  const source = object(JSON.parse(sourceText)), report = object(JSON.parse(reportText));
  const stop = stopText ? object(JSON.parse(stopText)) : undefined;
  const finalized = !!stop && stop.writerUid === report.writer_pod_uid &&
    stop.writerStoppedAtMs === source.writerStoppedAtMs &&
    integer(stop.writerStoppedAtMs, 'Writer stop time') >= integer(source.measurementEndMs, 'Measurement end');
  const observationValues = jsonLines(observationsText);
  const input = buildCoverageInput(source, observationValues, jsonLines(visibilityText ?? ''), finalized);
  const coverage = analyzeCoverage(input);
  const resources = resourceTrace(observationValues, input.measurementStartMs, input.measurementEndMs,
    metricsText === undefined ? undefined : JSON.parse(metricsText));
  const revision = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  const producerHash = report.manifest_sha256;
  const clean = !execFileSync('git', ['status', '--porcelain', '--untracked-files=normal'], { encoding: 'utf8' }).trim();
  requireThat(clean, 'Scoring requires a committed, unchanged evaluator revision.');
  const cleanup = report.namespace_removed === true && report.run_fic_removed === true &&
    report.workload_pools_at_zero === true && object(report.qap).table_removed === true;
  let producerBinding = typeof report.benchmark_revision === 'string' && /^[a-f0-9]{40,64}$/.test(report.benchmark_revision) &&
    report.manifest_sha256 === PINNED_MANIFEST_SHA256 && source.manifestSha256 === PINNED_MANIFEST_SHA256 &&
    source.benchmarkRevision === report.benchmark_revision;
  if (producerBinding) {
    try {
      producerBinding = hash(execFileSync('git', ['show',
        `${String(report.benchmark_revision)}:experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml`],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })) === PINNED_MANIFEST_SHA256;
    } catch { producerBinding = false; }
  }
  const reasons = [...coverage.reasons, ...resources.reasons,
    ...(cleanup ? [] : ['CLEANUP_INCOMPLETE']), ...(producerBinding ? [] : ['PINNED_PRODUCER_REVISION_UNVERIFIED'])];
  const result = { schemaVersion: 1, qualified: coverage.qualified && cleanup && producerBinding && resources.complete, reasons,
    benchmarkRevision: report.benchmark_revision ?? null, scorerRevision: revision, manifestSha256: producerHash,
    receiptHashes: { source: hash(sourceText), observations: hash(observationsText), report: hash(reportText),
      visibility: visibilityText === undefined ? null : hash(visibilityText), writerStop: stopText ? hash(stopText) : null,
      metrics: metricsText === undefined ? null : hash(metricsText) },
    inputStoppedAtMs: input.measurementEndMs, writerStopVerifiedAtMs: source.writerStoppedAtMs ?? null,
    cleanupVerifiedByRunner: cleanup, producerBinding, resources, coverage };
  await writePrivateFile(resolve(directory, 'qap-score.json'), JSON.stringify(result, null, 2) + '\n');
  console.log(`QAP ${result.qualified ? 'QUALIFIED' : 'NOT QUALIFIED'}; durable source=${coverage.source.rowsPerSecond.toFixed(2)} rows/s; ` +
    `terminal parity=${coverage.terminalParity.matched}; censored cohorts=${coverage.censored.cohorts}.`);
  console.log('Reasons: ' + (reasons.join(', ') || 'none'));
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
