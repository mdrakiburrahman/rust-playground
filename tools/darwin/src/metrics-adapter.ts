import type { Measurement } from "./score.js";

type JsonObject = Record<string, unknown>;

export interface OatMeasurement extends Measurement {
  startUnixMs: number;
  endUnixMs: number;
  rows: number;
  parquetBytes: number;
  files: number;
  commits: number;
  commitLatencySeconds: { p50: number; p99: number; samples: number };
  cpuMillicoresMean: number | null;
  memoryBytesMaxObserved: number | null;
  downstreamUnavailableReason: string | null;
}

export interface TimedReceipt {
  tableId: string;
  tableName: string;
  version: number;
  rows: number;
  files: number;
  inputBytes: number;
  parquetBytes: number;
  minIngestedAtMs: number;
  maxIngestedAtMs: number;
  finishedAtUnixMs: number;
  elapsedSeconds: number;
}

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as JsonObject;
}

function number(value: unknown, label: string, minimum = 0): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < minimum) {
    throw new Error(`${label} is missing or invalid`);
  }
  return value;
}

function integer(value: unknown, label: string, minimum = 0): number {
  const result = number(value, label, minimum);
  if (!Number.isSafeInteger(result)) throw new Error(`${label} must be a safe integer`);
  return result;
}

function nearlyEqual(actual: number, expected: number): boolean {
  return Math.abs(actual - expected) <= 1e-8 * Math.max(1, Math.abs(expected));
}

function text(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new Error(`${label} is missing`);
  return value;
}

export function extractTimedReceipts(input: unknown, tableName: string, measurement: OatMeasurement): TimedReceipt[] {
  const evidence = object(input, "evidence");
  const commits = object(evidence.durable_commits, "durable commits");
  const cumulative = object(commits.run_cumulative_output, "cumulative output");
  if (cumulative.scope !== "all_new_ingestion_commits_from_this_cell_including_warmup"
    || cumulative.unavailable_reason != null || !Array.isArray(cumulative.commits_by_version)) {
    throw new Error("complete timed receipts are unavailable");
  }
  const cumulativeRows = integer(cumulative.rows, "cumulative receipt rows", 1);
  const seen = new Map<string, TimedReceipt>();
  for (const raw of cumulative.commits_by_version) {
    const receipt = object(raw, "commit receipt");
    const parsed: TimedReceipt = {
      tableId: text(receipt.table_id, "receipt table id"),
      tableName: text(receipt.table_name, "receipt table name"),
      version: integer(receipt.delta_version, "receipt Delta version"),
      rows: integer(receipt.rows, "receipt rows", 1),
      files: integer(receipt.files, "receipt files", 1),
      inputBytes: integer(receipt.input_bytes, "receipt input bytes"),
      parquetBytes: integer(receipt.parquet_bytes, "receipt Parquet bytes", 1),
      minIngestedAtMs: integer(receipt.min_ingested_at_ms, "receipt minimum ingest time"),
      maxIngestedAtMs: integer(receipt.max_ingested_at_ms, "receipt maximum ingest time"),
      finishedAtUnixMs: integer(receipt.finished_at_unix_ms, "receipt finish time"),
      elapsedSeconds: number(receipt.elapsed_seconds, "receipt elapsed seconds"),
    };
    if (parsed.tableName !== tableName || parsed.minIngestedAtMs > parsed.maxIngestedAtMs) {
      throw new Error("commit receipt table or ingest times are inconsistent");
    }
    const key = `${parsed.tableId}\0${parsed.version}`;
    const prior = seen.get(key);
    if (prior && JSON.stringify(prior) !== JSON.stringify(parsed)) throw new Error("conflicting commit receipts");
    seen.set(key, parsed);
  }
  const receipts = [...seen.values()];
  if (new Set(receipts.map(receipt => receipt.tableId)).size !== 1) {
    throw new Error("expected exactly one receipt table identity");
  }
  const selected = receipts.filter(receipt =>
    receipt.finishedAtUnixMs >= measurement.startUnixMs && receipt.finishedAtUnixMs < measurement.endUnixMs);
  const sum = (items: TimedReceipt[], field: "rows" | "files" | "parquetBytes") =>
    items.reduce((total, receipt) => integer(total + receipt[field], "receipt total"), 0);
  if (integer(cumulative.commits, "cumulative commit count") !== receipts.length || sum(receipts, "rows") !== cumulativeRows
    || sum(selected, "rows") !== measurement.rows || sum(selected, "parquetBytes") !== measurement.parquetBytes
    || sum(selected, "files") !== measurement.files || selected.length !== measurement.commits) {
    throw new Error("timed receipts do not match commit-window totals");
  }
  return selected;
}

/**
 * Extracts the current OAT table evidence. Snapshot reconciliation and the
 * identity of this evidence must be verified by the trusted caller separately.
 */
export function adaptOatEvidence(input: unknown): OatMeasurement {
  const evidence = object(input, "evidence");
  const commits = object(evidence.durable_commits, "durable commits");
  const output = object(commits.output_throughput, "commit window output");
  if (output.unavailable_reason != null
    || output.scope !== "new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort"
    || output.clock !== "collector_system_unix_ms_shared_schedule_clock_skew_not_measured"
    || output.warmup_backlog !== "included_when_commit_finishes_in_window"
    || output.rewrite_commits !== "excluded_by_ingestion_only_producer_capability") {
    throw new Error("commit output is unavailable or has invalid provenance");
  }
  const start = integer(output.measurement_start_unix_ms, "window start");
  const end = integer(output.measurement_end_unix_ms, "window end");
  const durationMs = end - start;
  if (durationMs < 150_000) throw new Error("measurement window is shorter than 150 seconds");
  const seconds = number(output.window_seconds, "window seconds");
  if (!nearlyEqual(seconds * 1000, durationMs)) throw new Error("window duration is inconsistent");
  const rows = integer(output.rows, "durable rows", 1);
  const bytes = integer(output.parquet_bytes, "Parquet bytes", 1);
  const files = integer(output.files, "committed files", 1);
  const commitCount = integer(output.commits, "commit count", 1);
  const rate = number(output.rows_per_second, "durable rows/s", Number.MIN_VALUE);
  const bytesPerRow = number(output.bytes_per_row, "Parquet bytes/row", Number.MIN_VALUE);
  if (!nearlyEqual(rate, rows / seconds) || !nearlyEqual(bytesPerRow, bytes / rows)) {
    throw new Error("commit window totals are inconsistent");
  }
  if (!Array.isArray(commits.complete_30_second_commit_windows)
    || commits.complete_30_second_commit_windows.length < 5
    || commits.complete_30_second_commit_windows.length !== Math.floor(durationMs / 30_000)) {
    throw new Error("missing complete 30-second buckets");
  }
  let bucketRows = 0;
  let bucketBytes = 0;
  for (const [index, raw] of commits.complete_30_second_commit_windows.entries()) {
    const bucket = object(raw, "bucket");
    if (bucket.unavailable_reason != null
      || bucket.measurement_start_unix_ms !== start + index * 30_000
      || bucket.measurement_end_unix_ms !== start + (index + 1) * 30_000
      || bucket.window_seconds !== 30) {
      throw new Error("invalid or noncontiguous 30-second bucket");
    }
    bucketRows += integer(bucket.rows, "bucket rows");
    bucketBytes += integer(bucket.parquet_bytes, "bucket Parquet bytes");
    if (!Number.isSafeInteger(bucketRows) || !Number.isSafeInteger(bucketBytes)) {
      throw new Error("bucket totals overflow");
    }
  }
  if (bucketRows > rows || bucketBytes > bytes
    || (durationMs % 30_000 === 0 && (bucketRows !== rows || bucketBytes !== bytes))) {
    throw new Error("bucket totals do not match the commit window");
  }
  const latency = object(output.commit_latency_seconds, "commit latency");
  const p50 = number(latency.p50, "commit p50");
  const p99 = number(latency.p99, "commit p99");
  const latencySamples = integer(latency.samples, "commit latency samples", 1);
  if (p99 < p50 || latencySamples !== commitCount
    || latency.scope !== "commit_retry_and_sync_not_encoding_or_admission") {
    throw new Error("invalid commit latency distribution");
  }
  const resources = object(evidence.resources, "resources");
  if (!Array.isArray(resources.samples)) throw new Error("resource samples are missing");
  let cpuTotal = 0;
  let memoryMax = 0;
  for (const raw of resources.samples) {
    const sample = object(raw, "resource sample");
    const time = integer(sample.sampled_at_unix_ms, "resource sample time");
    if (time < start || time >= end) throw new Error("resource sample is outside the window");
    const metrics = object(sample.metrics, "resource metrics");
    cpuTotal += number(metrics.cpu_millicores, "CPU millicores");
    memoryMax = Math.max(memoryMax, number(metrics.memory_bytes, "memory bytes"));
  }
  const downstreamUnavailableReason = evidence.downstream_status === "unavailable"
    ? typeof evidence.downstream_comment === "string" && evidence.downstream_comment.trim()
      ? evidence.downstream_comment
      : (() => { throw new Error("downstream unavailability has no reason"); })()
    : null;
  return {
    startUnixMs: start,
    endUnixMs: end,
    rows,
    parquetBytes: bytes,
    files,
    commits: commitCount,
    durableRowsPerSecond: rate,
    parquetBytesPerRow: bytesPerRow,
    commitLatencySeconds: { p50, p99, samples: latencySamples },
    cpuMillicoresMean: resources.samples.length ? cpuTotal / resources.samples.length : null,
    memoryBytesMaxObserved: resources.samples.length ? memoryMax : null,
    downstreamUnavailableReason,
  };
}
