import { constants } from "node:fs";
import { lstat, mkdir, mkdtemp, open, realpath } from "node:fs/promises";
import path from "node:path";

export type CommitArm = "control" | "candidate";

export interface AdmittedCommitPin {
  revision: string;
  binarySha256: string;
  fixtureSha256: string;
  entrySha256: string;
}

export interface CommitCellRequest {
  arm: CommitArm;
  pair: number;
  resultPath: string;
}

export interface CommitCellReceipt {
  revision: string;
  binarySha256: string;
  fixtureSha256: string;
  entrySha256: string;
  exitCode: number | null;
  settled: boolean;
}

export interface CommitComponentHost {
  scratchRoot: string;
  control: AdmittedCommitPin;
  candidate: AdmittedCommitPin;
  runAdmittedCell(request: CommitCellRequest): Promise<CommitCellReceipt>;
}

export interface CommitComponentResult {
  version: 1;
  experiment: "commit-pipeline";
  tier: "component-diagnostic";
  objective: "proxy-rate";
  valid: 0;
  combined_score: 0;
  accepted: false;
  reason: string;
  feedback: string;
  evidenceDirectory: string;
  calibration: null | {
    wallSeconds: { control: number[]; candidate: number[] };
    pairedControlOverCandidate: number[];
    geometricMean: number;
  };
}

const ORDER: ReadonlyArray<{ arm: CommitArm; pair: number }> = [
  { arm: "control", pair: 1 },
  { arm: "candidate", pair: 1 },
  { arm: "candidate", pair: 2 },
  { arm: "control", pair: 2 },
  { arm: "control", pair: 3 },
  { arm: "candidate", pair: 3 },
];
const MAX_RESULT_BYTES = 64 * 1024;
const INPUT_CAP = 16 * 1024 * 1024;

function assertPin(pin: AdmittedCommitPin): void {
  if (!/^[a-f0-9]{40}$/.test(pin.revision)
    || !/^[a-f0-9]{64}$/.test(pin.binarySha256)
    || !/^[a-f0-9]{64}$/.test(pin.fixtureSha256)
    || !/^[a-f0-9]{64}$/.test(pin.entrySha256)) {
    throw new Error("pre-admitted source and executable pins are invalid");
  }
}

function samePin(a: AdmittedCommitPin, b: AdmittedCommitPin): boolean {
  return a.revision === b.revision && a.binarySha256 === b.binarySha256
    && a.fixtureSha256 === b.fixtureSha256 && a.entrySha256 === b.entrySha256;
}

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("component result must be an object");
  }
  return value as Record<string, unknown>;
}

function number(value: unknown, label: string, minimum: number, integer = false): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < minimum
    || (integer && !Number.isSafeInteger(value))) {
    throw new Error(`component result has invalid ${label}`);
  }
  return value;
}

function fixed(value: unknown, expected: unknown, label: string): void {
  if (value !== expected) throw new Error(`component result has invalid ${label}`);
}

function validateResult(input: unknown): { wallSeconds: number } {
  const result = record(input);
  fixed(result.schema, "arrowcol-commit-component-v1", "schema");
  fixed(result.scope, "local_component_not_dfe_ack_or_azure", "scope");
  fixed(result.storage, "local_filesystem", "storage");
  fixed(result.workload, "public_synthetic_id_payload_v1", "workload");
  fixed(result.timing_boundary, "generate_input_through_nonforced_durable_receipts", "timing boundary");
  fixed(result.batch_window_ms, 2000, "window");
  fixed(result.max_files_per_commit, 128, "commit file limit");
  fixed(result.max_pending_files, 512, "pending file limit");
  fixed(result.max_in_flight_groups, 8, "group limit");
  fixed(result.max_in_flight_input_bytes, INPUT_CAP, "input byte limit");
  fixed(result.timed_groups, 64, "timed group count");
  fixed(result.timed_receipts, 64, "timed receipt count");
  fixed(result.timed_durable_rows, 1_048_576, "timed row count");
  fixed(result.active_files_including_warmup, 65, "active file count");
  fixed(result.active_rows_including_warmup, 1_064_960, "active row count");
  fixed(result.upload_seconds, null, "upload measurement");
  fixed(result.replay, "deduplicated", "replay outcome");
  fixed(result.verification, "stopped_writer_delta_log_kernel_snapshot_and_parquet", "self-check status");
  for (const field of ["peak_in_flight_groups", "peak_staging_groups"] as const) {
    const peak = number(result[field], field, 1, true);
    if (peak > 8) throw new Error(`component result exceeds ${field}`);
  }
  const outstanding = number(result.peak_submitted_unconsumed_receipts, "outstanding receipts", 2, true);
  if (outstanding > 8) throw new Error("component result exceeds the outstanding group bound");
  const inputBytes = number(result.peak_in_flight_input_bytes, "in-flight input bytes", 1, true);
  if (inputBytes > INPUT_CAP) throw new Error("component result exceeds the input byte bound");
  const timedBytes = number(result.timed_physical_bytes, "timed physical bytes", 1, true);
  const activeBytes = number(result.active_physical_bytes_including_warmup, "active physical bytes", 1, true);
  if (activeBytes <= timedBytes) throw new Error("component physical byte totals are inconsistent");
  const versions = number(result.delta_versions, "Delta versions", 1, true);
  if (!Array.isArray(result.cohort_sizes) || result.cohort_sizes.length !== versions
    || result.cohort_sizes[0] !== 1
    || result.cohort_sizes.some((size: unknown) =>
      typeof size !== "number" || !Number.isSafeInteger(size) || size < 1 || size > 8)
    || result.cohort_sizes.reduce((sum: number, size: number) => sum + size, 0) !== 65) {
    throw new Error("component result has incomplete cohorts");
  }
  for (const field of [
    "process_cpu_seconds", "stage_wall_sum_seconds", "preparation_build_sum_seconds",
    "preparation_sort_sum_seconds", "encode_sum_seconds", "submit_sum_seconds",
    "receipt_wait_sum_seconds", "commit_service_sum_seconds",
  ] as const) number(result[field], field, 0);
  return { wallSeconds: number(result.wall_seconds, "wall time", Number.MIN_VALUE) };
}

async function readResult(file: string): Promise<unknown> {
  const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = await handle.stat();
    if (!before.isFile() || before.nlink !== 1 || before.size === 0 || before.size > MAX_RESULT_BYTES) {
      throw new Error("component result must be a bounded regular file");
    }
    const bytes = await handle.readFile();
    const after = await handle.stat();
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || bytes.length !== before.size) {
      throw new Error("component result changed during reading");
    }
    try {
      return JSON.parse(bytes.toString("utf8")) as unknown;
    } catch {
      throw new Error("component result is not valid JSON");
    }
  } finally {
    await handle.close();
  }
}

async function privateScratch(root: string): Promise<string> {
  if (!path.isAbsolute(root) || path.resolve(root) !== root || await realpath(root) !== root) {
    throw new Error("scratch root must be an absolute canonical directory");
  }
  const stat = await lstat(root);
  const uid = process.getuid?.();
  if (!stat.isDirectory() || stat.isSymbolicLink() || uid === undefined
    || (stat.uid !== uid && stat.uid !== 0) || (stat.mode & 0o022) !== 0) {
    throw new Error("scratch root must be owned and not group- or world-writable");
  }
  return mkdtemp(path.join(root, "commit-component-"));
}

/**
 * Run three alternating component pairs through a trusted, pre-admitted host.
 * The host owns isolation, immutable source/executable admission and process-tree
 * settlement; it must not forward candidate-controlled paths or stdout here.
 * Results are diagnostic only until independent stopped files are available.
 */
export async function evaluateCommitComponent(host: CommitComponentHost): Promise<CommitComponentResult> {
  assertPin(host.control);
  assertPin(host.candidate);
  if (host.control.fixtureSha256 !== host.candidate.fixtureSha256
    || host.control.entrySha256 !== host.candidate.entrySha256) {
    throw new Error("control and candidate must use the same protected fixture and entry point");
  }
  const evidenceDirectory = await privateScratch(host.scratchRoot);
  const times: Record<CommitArm, number[]> = { control: [], candidate: [] };
  for (const [index, cell] of ORDER.entries()) {
    const directory = path.join(evidenceDirectory, `${index + 1}-${cell.arm}-${cell.pair}`);
    await mkdir(directory, { mode: 0o700 });
    const resultPath = path.join(directory, "result.json");
    const receipt = await host.runAdmittedCell({ ...cell, resultPath });
    const pin = host[cell.arm];
    if (!samePin(receipt, pin) || receipt.exitCode !== 0 || receipt.settled !== true) {
      throw new Error(`${cell.arm} cell did not exit zero and settle with its admitted pin`);
    }
    const result = validateResult(await readResult(resultPath));
    times[cell.arm][cell.pair - 1] = result.wallSeconds;
  }
  let calibration: CommitComponentResult["calibration"] = null;
  if (samePin(host.control, host.candidate)) {
    const ratios = times.control.map((value, index) => value / times.candidate[index]!);
    const geometricMean = Math.exp(ratios.reduce((sum, ratio) => sum + Math.log(ratio), 0) / ratios.length);
    if (!Number.isFinite(geometricMean) || geometricMean <= 0) throw new Error("invalid calibration ratio");
    calibration = { wallSeconds: times, pairedControlOverCandidate: ratios, geometricMean };
  }
  return {
    version: 1,
    experiment: "commit-pipeline",
    tier: "component-diagnostic",
    objective: "proxy-rate",
    valid: 0,
    combined_score: 0,
    accepted: false,
    reason: "independent stopped Delta and Parquet files are unavailable outside candidate control",
    feedback: "Component-only diagnostic; the fixed 2s commit window dominates wall time and limits sensitivity.",
    evidenceDirectory,
    calibration,
  };
}
