import { createHash } from "node:crypto";
import { lstat, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tableFromIPC } from "apache-arrow";
import { Compression, readParquet } from "parquet-wasm";
import { parseCompressionSpec, type CompressionSpec } from "./compression-policy.js";
import { reconcileStoppedSnapshot } from "./delta-reconcile.js";
import { verifyReconciledDelivery, type IdentityTools } from "./identity-reconcile.js";
import { adaptOatEvidence, CoverageUnavailableError, extractTimedReceipts, type OatMeasurement } from "./metrics-adapter.js";
import { assertParityEvidence } from "./parity-preflight.js";

type JsonObject = Record<string, unknown>;

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

function imageId(value: unknown): string {
  if (typeof value !== "string" || !/^sha256:[a-f0-9]{64}$/.test(value)) throw new Error("local image identity is unavailable");
  return value;
}

export function assertParityProcessRevision(
  process: JsonObject, producerRevision: string, collectorRevision = producerRevision,
): void {
  if (process.source_revision !== (process.component === "collector" ? collectorRevision : producerRevision)) {
    throw new Error("live process source revision differs from the parity producer");
  }
}

export type ClientDataProfile = "diagnostic" | "realistic";

export const DIAGNOSTIC_REALISTIC_RECONCILIATION_VERSION = 1;
export const DIAGNOSTIC_REALISTIC_DEFAULT_WRITER_VERSION = 1;

function assertDiagnosticDefaultWriter(evidence: JsonObject, collector: JsonObject): void {
  const hasOverride = (value: unknown): boolean => {
    if (value == null) return false;
    if (Array.isArray(value)) return value.length !== 0;
    if (typeof value === "object") return Object.values(value).some(hasOverride);
    return true;
  };
  const check = (value: unknown, label: string) => {
    const writer = object(value, label);
    object(writer.defaults, `${label} defaults`);
    object(writer.columns, `${label} columns`);
    if (hasOverride(writer)) throw new Error(`${label} contains an override`);
  };
  check(collector.writer_policy_overrides, "collector writer readback");
  const admission = object(evidence.parity_admission_readback, "pre-admission readback");
  const schedule = object(object(evidence.client_schedule, "client schedule").effective, "effective client schedule");
  if (admission.status !== "verified_before_schedule" || !Number.isSafeInteger(admission.observed_at_unix_ms)
    || !Number.isSafeInteger(schedule.run_start_unix_ms)
    || Number(admission.observed_at_unix_ms) >= Number(schedule.run_start_unix_ms)
    || typeof admission.collector_image_id !== "string" || admission.collector_image_id !== collector.image_id) {
    throw new Error("pre-admission collector identity or timing is unavailable");
  }
  check(object(admission.collector, "pre-admission collector").parquet_writer, "pre-admission writer readback");
}

export function wasmCompression(spec: CompressionSpec): Compression {
  switch (spec.codec) {
    case "uncompressed": return Compression.UNCOMPRESSED;
    case "snappy": return Compression.SNAPPY;
    case "gzip": return Compression.GZIP;
    case "brotli": return Compression.BROTLI;
    case "lz4": return Compression.LZ4;
    case "lz4_raw": return Compression.LZ4_RAW;
    case "zstd": return Compression.ZSTD;
  }
}

type DiagnosticTelemetry = {
  pressure_http_timings: {
    count: number | null;
    p50_ms: number | null;
    p95_ms: number | null;
    p99_ms: number | null;
    max_ms: number | null;
    samples: JsonObject[] | null;
    slow_or_timed_out_samples: JsonObject[] | null;
    dropped: number | null;
    quantile_method: "nearest_rank";
    unavailable_reason: string | null;
  };
  zero_progress_window_count: number | null;
  complete_windows: JsonObject[] | null;
  zero_progress_windows_unavailable_reason: string | null;
  collector_rss_samples: JsonObject[] | null;
  collector_rss_samples_dropped: number | null;
  collector_rss_unavailable_reason: string | null;
  pipeline_metric_samples: JsonObject[] | null;
  pipeline_metric_samples_unavailable_count: number | null;
  pipeline_metric_samples_dropped: number | null;
  pipeline_metrics_unavailable_reason: string | null;
  client_send_stalls: null;
  client_send_stalls_unavailable_reason: string;
};

function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(label + " must be a non-negative safe integer");
  }
  return value;
}

function extractDiagnosticTelemetry(evidence: JsonObject, measurement: OatMeasurement): DiagnosticTelemetry {
  const unavailable = "overload guard evidence is missing";
  const result: DiagnosticTelemetry = {
    pressure_http_timings: { count: null, p50_ms: null, p95_ms: null, p99_ms: null, max_ms: null,
      samples: null, slow_or_timed_out_samples: null, dropped: null, quantile_method: "nearest_rank",
      unavailable_reason: unavailable },
    zero_progress_window_count: null, complete_windows: null,
    zero_progress_windows_unavailable_reason: unavailable,
    collector_rss_samples: null, collector_rss_samples_dropped: null,
    collector_rss_unavailable_reason: unavailable,
    pipeline_metric_samples: null, pipeline_metric_samples_unavailable_count: null, pipeline_metric_samples_dropped: null,
    pipeline_metrics_unavailable_reason: unavailable,
    client_send_stalls: null, client_send_stalls_unavailable_reason: "OAT client evidence does not expose send-stall measurements; these are owned by the load generator.",
  };
  if (evidence.overload_guard == null) return result;
  const guard = object(evidence.overload_guard, "overload guard evidence");
  const admission = guard.admission_diagnostics == null
    ? null : object(guard.admission_diagnostics, "guard admission diagnostics");
  const rawTimings = admission?.pressure_http_timings;
  if (admission && Array.isArray(rawTimings)) {
    const samples = rawTimings.map((sample, index) => object(sample, "pressure HTTP timing sample " + index));
    const elapsed = samples.map((sample, index) => nonNegativeInteger(sample.elapsed_ms, "pressure HTTP elapsed time " + index));
    const sorted = [...elapsed].sort((a, b) => a - b);
    const percentile = (fraction: number) => sorted.length ? sorted[Math.ceil(fraction * sorted.length) - 1]! : null;
    const dropped = admission.pressure_http_timing_dropped;
    result.pressure_http_timings = {
      count: samples.length, p50_ms: percentile(0.5), p95_ms: percentile(0.95), p99_ms: percentile(0.99),
      max_ms: sorted.length ? sorted[sorted.length - 1]! : null, samples,
      slow_or_timed_out_samples: samples.filter((sample, index) => elapsed[index]! >= 1500 || sample.status === "timeout"),
      dropped: typeof dropped === "number" ? nonNegativeInteger(dropped, "dropped pressure HTTP timing count") : null,
      quantile_method: "nearest_rank",
      unavailable_reason: samples.length === 0 ? "no pressure HTTP timing samples were recorded" : null,
    };
  } else {
    result.pressure_http_timings.unavailable_reason = "pressure HTTP timings are missing from guard admission diagnostics";
  }
  if (Array.isArray(guard.complete_windows)) {
    const windows = guard.complete_windows.map((window, index) => object(window, "guard complete window " + index));
    result.complete_windows = windows;
    result.zero_progress_window_count = windows.filter((window, index) => {
      const commits = nonNegativeInteger(window.ingestion_commits, "window ingestion commits " + index);
      return window.covered === true && window.outstanding === true && commits === 0;
    }).length;
    result.zero_progress_windows_unavailable_reason = windows.length ? null : "guard recorded no complete progress windows";
  } else {
    result.zero_progress_windows_unavailable_reason = "complete progress windows are missing from guard evidence";
  }
  if (Array.isArray(guard.collector_rss_samples)) {
    result.collector_rss_samples = guard.collector_rss_samples.map((sample, index) => {
      const parsed = object(sample, "collector RSS sample " + index);
      return { sampled_at_unix_ms: nonNegativeInteger(parsed.sampled_at_unix_ms, "collector RSS sample timestamp"),
        rss_bytes: nonNegativeInteger(parsed.rss_bytes, "collector RSS bytes"),
        cap_bytes: nonNegativeInteger(parsed.cap_bytes, "collector RSS cap bytes") };
    });
    const dropped = guard.collector_rss_samples_dropped;
    result.collector_rss_samples_dropped = typeof dropped === "number"
      ? nonNegativeInteger(dropped, "dropped collector RSS sample count") : null;
    result.collector_rss_unavailable_reason = result.collector_rss_samples.length
      ? null : typeof guard.collector_rss_unavailable_reason === "string"
        ? guard.collector_rss_unavailable_reason : "no collector RSS samples were recorded";
  } else {
    result.collector_rss_unavailable_reason = "collector RSS samples are missing from guard evidence";
  }
  const backlog = evidence.backlog == null ? null : object(evidence.backlog, "publication backlog evidence");
  if (Array.isArray(backlog?.samples)) {
    const matching = backlog.samples.map((sample, index) => object(sample, "backlog sample " + index)).filter(sample => {
      const started = nonNegativeInteger(sample.request_start_unix_ms, "backlog sample start");
      return started >= measurement.startUnixMs && started < measurement.endUnixMs;
    });
    const pipelineSamples: JsonObject[] = [];
    const unavailableReasons: string[] = [];
    for (const sample of matching) {
      if (sample.pipeline_stage_metrics != null) {
        pipelineSamples.push({
          request_start_unix_ms: sample.request_start_unix_ms,
          request_end_unix_ms: sample.request_end_unix_ms,
          metrics: object(sample.pipeline_stage_metrics, "pipeline stage metrics"),
        });
      } else if (typeof sample.pipeline_stage_metrics_unavailable_reason === "string") {
        unavailableReasons.push(sample.pipeline_stage_metrics_unavailable_reason);
      }
    }
    result.pipeline_metric_samples = pipelineSamples.length ? pipelineSamples : null;
    result.pipeline_metric_samples_unavailable_count = matching.length - pipelineSamples.length;
    result.pipeline_metric_samples_dropped = typeof backlog.samples_dropped === "number"
      ? nonNegativeInteger(backlog.samples_dropped, "dropped backlog sample count") : null;
    result.pipeline_metrics_unavailable_reason = pipelineSamples.length
      ? null : unavailableReasons[0] ?? "no exporter stage histogram samples in the measurement window";
  } else {
    result.pipeline_metrics_unavailable_reason = "publication backlog samples are missing";
  }
  return result;
}

export interface ClientEvidence {
  profile: ClientDataProfile;
  identityEligibleForReconciliation: boolean;
  payloadCorpusSha256: string;
  cpu: { coreEquivalents: number | null; sampleStartUnixMs: number | null; sampleEndUnixMs: number | null; unavailableReason: string | null };
}

export function extractStoppedReceipts(
  input: unknown, stoppedInput: unknown, runId: string, table: string, measurement: OatMeasurement,
) {
  const evidence = object(input, "OAT evidence");
  const stopped = object(stoppedInput, "stopped snapshot");
  if (stopped.schema_version !== 1 || stopped.run_id !== runId || !Array.isArray(stopped.writers)) {
    throw new CoverageUnavailableError("stopped receipt snapshot identity is unavailable");
  }
  const collectors = stopped.writers.map(writer => object(writer, "stopped writer"))
    .filter(writer => typeof writer.service === "string" && /^dataflow-\d+$/.test(writer.service));
  if (collectors.length !== 1 || collectors[0]!.was_running !== true || collectors[0]!.oom_killed !== false
    || collectors[0]!.exit_code !== 0 || typeof collectors[0]!.finished_at !== "string"
    || !(Date.parse(collectors[0]!.finished_at) > 0)) {
    throw new CoverageUnavailableError("stopped collector identity or clean exit is unavailable");
  }
  const output = object(collectors[0]!.stopped_commit_output, "stopped commit output");
  if (output.source !== "complete_stopped_collector_logs") {
    throw new CoverageUnavailableError("complete stopped collector receipts are unavailable");
  }
  const tables = object(output.tables, "stopped receipt tables");
  const before = extractTimedReceipts(evidence, table, measurement);
  const commits = object(evidence.durable_commits, "durable commits");
  const after = extractTimedReceipts({
    ...evidence, durable_commits: { ...commits, run_cumulative_output: tables[table] },
  }, table, measurement);
  const byVersion = new Map(after.all.map(receipt => [`${receipt.tableId}\0${receipt.version}`, receipt]));
  for (const receipt of before.all) {
    if (JSON.stringify(byVersion.get(`${receipt.tableId}\0${receipt.version}`)) !== JSON.stringify(receipt)) {
      throw new CoverageUnavailableError("stopped receipts disagree with pre-stop receipt evidence");
    }
  }
  if (JSON.stringify(before.selected) !== JSON.stringify(after.selected)) {
    throw new CoverageUnavailableError("stopped receipts disagree with measured-window receipts");
  }
  return after;
}

export function extractClientEvidence(raw: unknown, requestedProfile: ClientDataProfile): ClientEvidence {
  if (requestedProfile !== "diagnostic" && requestedProfile !== "realistic") throw new Error("requested client data profile is invalid");
  const report = object(raw, "client report");
  const phase = object(report.phase_counters, "client phase counters");
  const preparation = object(phase.preparation, "client preparation");
  const manifest = object(phase.payload_manifest, "payload manifest");
  const hash = preparation.payload_corpus_sha256;
  const expectedScope = requestedProfile === "diagnostic"
    ? "arrow-logical-v2-all-columns"
    : "arrow-logical-v2-excluding-endpoint_url";
  const identityEligible = requestedProfile === "realistic";
  const expectedStatus = identityEligible ? "identity_bearing" : "diagnostic_only";
  if (typeof hash !== "string" || !/^[a-f0-9]{64}$/.test(hash)
    || preparation.data_profile !== requestedProfile || preparation.steady_state_generation !== identityEligible
    || preparation.payload_corpus_hash_version !== 2 || preparation.payload_corpus_hash_scope !== expectedScope
    || manifest.payload_corpus_sha256 !== hash || manifest.data_profile !== requestedProfile
    || manifest.payload_corpus_hash_version !== 2 || manifest.payload_corpus_hash_scope !== expectedScope
    || manifest.identity_status !== expectedStatus || manifest.eligible_for_identity_reconciliation !== identityEligible
    || manifest.complete !== identityEligible || manifest.truncated !== false) {
    throw new Error("client payload corpus evidence is unavailable or inconsistent");
  }
  const clientEvidence = {
    profile: requestedProfile,
    identityEligibleForReconciliation: identityEligible,
    payloadCorpusSha256: hash,
  };
  const window = object(phase.measurement_window, "client measurement window");
  if (window.client_resources == null) {
    return { ...clientEvidence, cpu: { coreEquivalents: null, sampleStartUnixMs: null, sampleEndUnixMs: null, unavailableReason: "client CPU resource evidence unavailable" } };
  }
  const resources = object(window.client_resources, "client resources");
  const cpu = resources.cpu_core_equivalents;
  if (cpu == null) {
    return { ...clientEvidence, cpu: { coreEquivalents: null, sampleStartUnixMs: null, sampleEndUnixMs: null, unavailableReason: "client process CPU samples unavailable" } };
  }
  if (typeof cpu !== "number" || !Number.isFinite(cpu) || cpu < 0
    || !Number.isSafeInteger(resources.start_sample_unix_ms) || !Number.isSafeInteger(resources.end_sample_unix_ms)
    || Number(resources.start_sample_unix_ms) >= Number(resources.end_sample_unix_ms)) {
    throw new Error("client CPU evidence is invalid");
  }
  return { ...clientEvidence, cpu: {
    coreEquivalents: cpu, sampleStartUnixMs: Number(resources.start_sample_unix_ms),
    sampleEndUnixMs: Number(resources.end_sample_unix_ms), unavailableReason: null,
  } };
}

export function extractClientReports(raw: unknown, profile: ClientDataProfile) {
  if (!Array.isArray(raw) || raw.length === 0 || raw.length > 1024) throw new Error("expected 1 to 1024 client reports");
  let totalCompletedRows = 0;
  let sharedImage: string | undefined;
  const reports = raw.map(item => {
    const report = object(item, "client report");
    const image = object(report.execution_image, "client image");
    if (image.source !== "docker_container_inspect") throw new Error("client image provenance is unavailable");
    const id = imageId(image.image_id);
    if (sharedImage !== undefined && sharedImage !== id) throw new Error("client images differ");
    sharedImage = id;
    const config = object(report.consumer_effective_config, "client readback");
    const counters = object(report.phase_counters, "client phase counters");
    const cumulative = object(counters.process_cumulative, "client cumulative counters");
    if (!Number.isSafeInteger(cumulative.completed_rows) || Number(cumulative.completed_rows) < 0
      || cumulative.ambiguous_rows !== 0) throw new CoverageUnavailableError("client cumulative completions are unavailable or ambiguous");
    totalCompletedRows += Number(cumulative.completed_rows);
    if (!Number.isSafeInteger(totalCompletedRows)) throw new CoverageUnavailableError("client cumulative completions overflow");
    return { image, config, evidence: extractClientEvidence(item, profile) };
  });
  const first = reports[0]!;
  if (reports.some(report => report.evidence.payloadCorpusSha256 !== first.evidence.payloadCorpusSha256)) {
    throw new Error("client business corpus hashes differ");
  }
  const cpus = reports.map(report => report.evidence.cpu);
  const start = cpus[0]!.sampleStartUnixMs;
  const end = cpus[0]!.sampleEndUnixMs;
  const allComparable = cpus.every(cpu => cpu.coreEquivalents !== null
    && cpu.sampleStartUnixMs === start && cpu.sampleEndUnixMs === end);
  const totalCpu = allComparable ? cpus.reduce((sum, cpu) => sum + cpu.coreEquivalents!, 0) : null;
  const clientEvidence: ClientEvidence = {
    ...first.evidence,
    cpu: totalCpu !== null && Number.isFinite(totalCpu)
      ? { coreEquivalents: totalCpu, sampleStartUnixMs: start, sampleEndUnixMs: end, unavailableReason: null }
      : { coreEquivalents: null, sampleStartUnixMs: null, sampleEndUnixMs: null, unavailableReason: "client CPU samples unavailable or not aligned" },
  };
  return { image: sharedImage!, reports, clientEvidence, totalCompletedRows };
}

export async function readReconciledOatResult(
  resultsPath: string, snapshotRoot: string, table: string, sourceRevision: string,
  compressionLevel: number | null | CompressionSpec,
  requestedProfile: ClientDataProfile, parity?: {
    config: unknown; decodeFile: (path: string, active: boolean) => Promise<number>; collectorRevision?: string;
    allowWriterOverrides?: boolean;
  },
  identity?: { tools: IdentityTools; outputDir: string; collectorRevision?: string },
  diagnostic?: {
    diagnosticOnlyRealistic: true; decodeFile: (path: string, active: boolean) => Promise<number>;
    expectedClientConfig: { workers: number; streams_per_worker: number }; collectorRevision?: string;
  },
): Promise<{
  measurement: OatMeasurement; images: string[]; clientEvidence: ClientEvidence; clientEvidences: ClientEvidence[];
  clientEffectiveConfigs: JsonObject[]; activeRows: number; snapshotVersion: number;
  activeFileRows: Awaited<ReturnType<typeof reconcileStoppedSnapshot>>["activeFileRows"];
  windowFileRows: Awaited<ReturnType<typeof reconcileStoppedSnapshot>>["windowFileRows"];
  parityTelemetry: JsonObject | null;
  diagnosticTelemetry: DiagnosticTelemetry;
  clientImageSourceRevision: string | null;
  clientImageSourceRevisionUnavailableReason: string | null;
  deliveryIdentity: JsonObject | null;
  diagnosticOnlyRealistic: boolean; diagnosticRealisticReconciliationVersion: number | null;
  kind?: "diagnostic-only";
  cellRunId: string; snapshotReconciled: true;
}> {
  if (diagnostic && (diagnostic.diagnosticOnlyRealistic !== true || requestedProfile !== "realistic"
    || typeof diagnostic.decodeFile !== "function" || parity || !identity?.tools
    || typeof identity.outputDir !== "string" || identity.outputDir.length === 0)) {
    throw new Error("diagnostic-only Realistic reconciliation requires a physical decoder and identity tools, without parity qualification");
  }
  if (diagnostic && (!Number.isSafeInteger(diagnostic.expectedClientConfig?.workers)
    || diagnostic.expectedClientConfig.workers < 1 || diagnostic.expectedClientConfig.workers > 1024
    || !Number.isSafeInteger(diagnostic.expectedClientConfig.streams_per_worker)
    || diagnostic.expectedClientConfig.streams_per_worker < 1)) {
    throw new Error("invalid diagnostic client worker/stream binding");
  }
  const typedCompression = compressionLevel !== null && typeof compressionLevel === "object";
  const compression = compressionLevel === null ? null : parseCompressionSpec(
    typeof compressionLevel === "number" ? { codec: "zstd", level: compressionLevel } : compressionLevel,
  );
  if (diagnostic && compression !== null) throw new Error("diagnostic Realistic requires the unchanged default writer");
  if (typedCompression && (requestedProfile !== "realistic" || (!diagnostic && (!parity?.allowWriterOverrides
    || typeof parity.decodeFile !== "function" || !identity)))) {
    throw new Error("typed compression requires qualified preflight and delivery identity");
  }
  if (requestedProfile === "diagnostic") {
    if (compression !== null || !parity || typeof parity.decodeFile !== "function") {
      throw new Error("diagnostic parity requires a physical footer inspector and no codec override");
    }
  } else if ((!diagnostic && compression === null) || (parity && (!parity.allowWriterOverrides || typeof parity.decodeFile !== "function"))) {
    throw new Error("realistic compression requires its Zstd level and an explicitly qualified parity gate");
  }
  if (requestedProfile === "realistic" && !identity) {
    throw new Error("realistic replay requires full delivery identity reconciliation");
  }
  if (requestedProfile === "diagnostic" && identity) {
    throw new Error("diagnostic payloads cannot qualify delivery identities");
  }
  const bytes = await readFile(resultsPath);
  if (!bytes.length || bytes.length > 16 * 1024 * 1024) throw new Error("OAT result is empty or too large");
  const arrow = tableFromIPC(readParquet(bytes).intoIPCStream());
  if (arrow.numRows !== 1) throw new Error("expected exactly one OAT result row");
  const arrowRow = arrow.toArray()[0];
  const row = Object.fromEntries(arrow.schema.fields.map(field => [field.name, arrowRow[field.name]]));
  if (row.schema_version !== 3 || row.mode !== "local" || row.table !== table || row.status !== "succeeded"
    || row.git_sha !== sourceRevision || row.pinning_ok !== true || Number(row.replicas) !== 1
    || typeof row.run_id !== "string" || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(row.run_id)
    || typeof row.evidence_json !== "string") {
    throw new Error("OAT result identity, source, pinning, or status is invalid");
  }
  const evidenceJson = row.evidence_json;
  const cellRunId = row.run_id;
  const evidence = object(JSON.parse(evidenceJson), "OAT evidence");
  if (evidence.version !== 1 || !Array.isArray(evidence.processes)) throw new Error("OAT evidence version or processes are invalid");
  const processes = evidence.processes.map(process => object(process, "OAT process"));
  const collectors = processes.filter(process => process.component === "collector");
  if (collectors.length !== 1) throw new Error("expected exactly one collector");
  const collector = collectors[0]!;
  const effective = object(collector.effective_config, "collector effective config");
  const capability = object(effective.commit_telemetry, "collector commit capability");
  if (collector.log_complete !== true || collector.effective_config_source !== "consumer_startup_readback"
    || capability.schema_version !== 1 || capability.scope !== "new_ingestion_addfiles_after_sync"
    || capability.clock !== "system_unix_ms") {
    throw new Error("collector capability does not match the run");
  }
  if (parity) {
    assertParityEvidence(parity.config, evidence, parity.allowWriterOverrides === true);
  }
  if (diagnostic) {
    assertDiagnosticDefaultWriter(evidence, collector);
  } else if (requestedProfile === "realistic") {
    const policy = object(object(collector.writer_policy_overrides, "writer policy").defaults, "writer defaults");
    if (!compression || policy.compression !== compression.codec || (policy.compression_level ?? null) !== compression.level) {
      throw new Error("collector configured compression does not match the run");
    }
  }
  const images = processes.map(process => {
    if (typeof process.component !== "string" || process.log_complete !== true) throw new Error("incomplete OAT process evidence");
    if (parity || identity?.collectorRevision || diagnostic) {
      assertParityProcessRevision(process, sourceRevision, parity?.collectorRevision ?? identity?.collectorRevision ?? diagnostic?.collectorRevision);
    }
    return `${process.component}:${imageId(process.image_id)}`;
  });
  const clients = extractClientReports(evidence.client_phase_reports, requestedProfile);
  if (diagnostic && clients.reports.some(report =>
    report.config.workers !== diagnostic.expectedClientConfig.workers
      || report.config.streams_per_worker !== diagnostic.expectedClientConfig.streams_per_worker)) {
    throw new Error("diagnostic client worker/stream readback differs from requested binding");
  }
  const client = clients.reports[0]!.image;
  const clientEvidence = clients.clientEvidence;
  images.push(`client:${clients.image}`);
  images.sort();
  const measurement = adaptOatEvidence(evidence);
  const diagnosticTelemetry = extractDiagnosticTelemetry(evidence, measurement);
  if (diagnosticTelemetry.collector_rss_samples?.length) {
    measurement.rssBytesMaxObserved = diagnosticTelemetry.collector_rss_samples
      .map(sample => nonNegativeInteger(sample.rss_bytes, "collector RSS bytes"))
      .reduce((maximum, value) => Math.max(maximum, value), 0);
    measurement.rssUnavailableReason = null;
  } else {
    measurement.rssUnavailableReason = diagnosticTelemetry.collector_rss_unavailable_reason;
  }
  const canonicalTable = `telemetry.sqlserver.${table}`;
  const stoppedPath = join(snapshotRoot, row.run_id, "stopped.json");
  const stoppedStat = await lstat(stoppedPath);
  if (!stoppedStat.isFile() || stoppedStat.isSymbolicLink() || stoppedStat.size > 64 * 1024 * 1024) {
    throw new CoverageUnavailableError("invalid stopped receipt evidence file");
  }
  const stopped = JSON.parse(await readFile(stoppedPath, "utf8"));
  const receipts = extractStoppedReceipts(evidence, stopped, row.run_id, canonicalTable, measurement);
  const stoppedRows = receipts.all.reduce((sum, receipt) => sum + receipt.rows, 0);
  if (!Number.isSafeInteger(stoppedRows) || stoppedRows !== clients.totalCompletedRows) {
    throw new CoverageUnavailableError("stopped Delta rows do not match unambiguous cumulative client completions");
  }
  if (typeof evidence.preload_delta !== "object" || evidence.preload_delta === null || Array.isArray(evidence.preload_delta)) {
    throw new CoverageUnavailableError("pre-load Delta evidence is unavailable");
  }
  const preload = object(evidence.preload_delta, "pre-load Delta evidence");
  const schedule = object(object(evidence.client_schedule, "client schedule").effective, "effective client schedule");
  if (!Number.isSafeInteger(schedule.run_start_unix_ms) || typeof preload.observed_at_unix_ms !== "number"
    || preload.observed_at_unix_ms >= Number(schedule.run_start_unix_ms)) {
    throw new CoverageUnavailableError("pre-load Delta evidence was not captured before client admission");
  }
  const expectedTableId = createHash("sha256").update(`file:///delta/dataflow/telemetry/sqlserver/${table}/`).digest("hex");
  const identityResult: { value: JsonObject | null } = { value: null };
  const identityOutputDir = identity ? (diagnostic ? join(identity.outputDir, "diagnostic-only") : identity.outputDir) : undefined;
  const verifyActive = identity ? async (paths: string[]) => {
    if (diagnostic) await mkdir(identityOutputDir!);
    identityResult.value = await verifyReconciledDelivery(identity.tools, identityOutputDir!, resultsPath,
      createHash("sha256").update(bytes).digest("hex"), evidenceJson, table, paths);
    if (diagnostic) {
      identityResult.value.kind = "diagnostic-only";
      identityResult.value.diagnosticRealisticReconciliationVersion = DIAGNOSTIC_REALISTIC_RECONCILIATION_VERSION;
      identityResult.value.diagnosticRealisticDefaultWriterVersion = DIAGNOSTIC_REALISTIC_DEFAULT_WRITER_VERSION;
    }
    const root = join(snapshotRoot, cellRunId);
    const snapshotHashes: Record<string, string> = {};
    for (const path of ["complete.json", "stopped.json", `${table}/manifest.json`]) {
      snapshotHashes[path] = createHash("sha256").update(await readFile(join(root, path))).digest("hex");
    }
    identityResult.value.snapshotHashes = snapshotHashes;
  } : undefined;
  const proof = await reconcileStoppedSnapshot(
    join(snapshotRoot, row.run_id), row.run_id, table, canonicalTable, expectedTableId, measurement, receipts.selected, receipts.all,
    compression === null ? Compression.UNCOMPRESSED : wasmCompression(compression), preload,
    parity?.decodeFile ?? diagnostic?.decodeFile, verifyActive, diagnostic !== undefined,
  );
  if (identity && (!identityResult.value || identityResult.value.activeFileRows !== proof.activeRows)) {
    throw new Error("identity verdict does not match reconciled active rows");
  }
  if (identity) await writeFile(join(identityOutputDir!, "identity-result.json"),
    `${JSON.stringify(identityResult.value)}\n`, { flag: "wx", mode: 0o400 });
  return {
    measurement, images, clientEvidence, clientEvidences: clients.reports.map(report => report.evidence),
    diagnosticTelemetry,
    clientImageSourceRevision: typeof client.source_revision === "string" ? client.source_revision : null,
    clientImageSourceRevisionUnavailableReason: typeof client.source_revision === "string" ? null : "client image has no source revision label",
    clientEffectiveConfigs: clients.reports.map(report => report.config), activeRows: proof.activeRows, snapshotVersion: proof.latestVersion,
    activeFileRows: proof.activeFileRows, windowFileRows: proof.windowFileRows, deliveryIdentity: identityResult.value,
    ...(diagnostic ? { kind: "diagnostic-only" as const } : {}), diagnosticOnlyRealistic: diagnostic !== undefined,
    diagnosticRealisticReconciliationVersion: diagnostic ? DIAGNOSTIC_REALISTIC_RECONCILIATION_VERSION : null,
    parityTelemetry: parity ? {
      backlog: evidence.backlog ?? null,
      backlog_budgets: object(object(object(parity.config, "parity config").tunables, "tunables").collector, "collector").batching,
      effective_readback: { before_admission: evidence.parity_admission_readback,
        collector: collector.effective_config, writer: collector.writer_policy_overrides,
        table_layout: collector.layout_table_policies, client: clients.reports[0]!.config,
        clients: clients.reports.map(report => report.config) },
      admission_waits: null, admission_waits_unavailable_reason: "stage telemetry not integrated",
      commit_substages: null, commit_substages_unavailable_reason: "stage telemetry not integrated",
      client_send_stalls: null, client_send_stalls_unavailable_reason: "stage telemetry not integrated",
      client_image_source_revision: client.source_revision ?? null,
      client_image_source_revision_unavailable_reason: client.source_revision == null ? "client image has no source revision label" : null,
    } : null,
    cellRunId: row.run_id, snapshotReconciled: true,
  };
}
