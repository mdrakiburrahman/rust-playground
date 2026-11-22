import { execFileSync } from "node:child_process";
import path from "node:path";

export const DISPATCH_LANE = "compression-config" as const;
export const MAX_REQUEST_BYTES = 4096;
export const MAX_CONFIG_BYTES = 16_384;
export const MIN_COMPRESSION_LEVEL = 1;
export const MAX_COMPRESSION_LEVEL = 22;

export interface DispatchRequest {
  version: 1;
  lane: typeof DISPATCH_LANE;
  candidateCommit: string;
}

export interface ValidatedDispatch {
  lane: typeof DISPATCH_LANE;
  candidateCommit: string;
  compressionLevel: number;
}

export type ConfigValidator = (baseline: Buffer, candidate: Buffer) => number;
export type Workload = (dispatch: ValidatedDispatch) => Promise<unknown> | unknown;

function runGit(repoRoot: string, args: string[], binary = false, maxBuffer = 1024 * 1024): Buffer {
  const output = execFileSync("git", ["-C", repoRoot, ...args], {
    encoding: binary ? undefined : "utf8",
    maxBuffer,
  });
  return Buffer.isBuffer(output) ? output : Buffer.from(output);
}

function assertCommit(repoRoot: string, commit: string): void {
  if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error("candidateCommit must be a full lowercase Git SHA");
  if (runGit(repoRoot, ["cat-file", "-t", commit]).toString("utf8").trim() !== "commit") {
    throw new Error("candidateCommit does not identify a Git commit");
  }
}

export function parseDispatchRequest(payload: Uint8Array): DispatchRequest {
  const bytes = Buffer.from(payload);
  if (bytes.length === 0 || bytes.length > MAX_REQUEST_BYTES) throw new Error("dispatch request size is invalid");
  let value: unknown;
  try { value = JSON.parse(bytes.toString("utf8")); } catch { throw new Error("dispatch request must be JSON"); }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("dispatch request must be an object");
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  if (keys.join(",") !== "candidateCommit,lane,version"
    || record.version !== 1 || record.lane !== DISPATCH_LANE
    || typeof record.candidateCommit !== "string") {
    throw new Error("dispatch request does not match the fixed compression-config contract");
  }
  return { version: 1, lane: DISPATCH_LANE, candidateCommit: record.candidateCommit };
}

export function validateDispatch(
  repoRoot: string,
  producerCommit: string,
  configPath: string,
  payload: Uint8Array,
  validateConfig: ConfigValidator,
): ValidatedDispatch {
  const request = parseDispatchRequest(payload);
  if (path.isAbsolute(configPath) || configPath.split(/[\\/]/).includes("..")
    || configPath.replaceAll("\\", "/") !== configPath) {
    throw new Error("trusted config path must be a normalized repository-relative path");
  }
  assertCommit(repoRoot, producerCommit);
  assertCommit(repoRoot, request.candidateCommit);
  try {
    runGit(repoRoot, ["merge-base", "--is-ancestor", producerCommit, request.candidateCommit]);
  } catch {
    throw new Error("candidate commit must descend from the pinned producer commit");
  }

  const changedPaths = runGit(repoRoot, ["diff", "--name-only", "--no-renames", "-z", producerCommit, request.candidateCommit, "--"], true)
    .toString("utf8").split("\0").filter(Boolean);
  if (changedPaths.length !== 1 || changedPaths[0] !== configPath) {
    throw new Error("candidate commit may change only the trusted compression config file");
  }

  const configReadLimit = MAX_CONFIG_BYTES * 4;
  const baseline = runGit(repoRoot, ["show", `${producerCommit}:${configPath}`], true, configReadLimit);
  const candidate = runGit(repoRoot, ["show", `${request.candidateCommit}:${configPath}`], true, configReadLimit);
  if (baseline.length > MAX_CONFIG_BYTES || candidate.length > MAX_CONFIG_BYTES) {
    throw new Error("trusted compression config exceeds the size limit");
  }
  const compressionLevel = validateConfig(baseline, candidate);
  if (!Number.isInteger(compressionLevel)
    || compressionLevel < MIN_COMPRESSION_LEVEL || compressionLevel > MAX_COMPRESSION_LEVEL) {
    throw new Error("validated compression level is outside the supported range");
  }
  return { lane: DISPATCH_LANE, candidateCommit: request.candidateCommit, compressionLevel };
}

export async function dispatchRequest(
  repoRoot: string,
  producerCommit: string,
  configPath: string,
  payload: Uint8Array,
  validateConfig: ConfigValidator,
  startWorkload: Workload,
): Promise<unknown> {
  const validated = validateDispatch(repoRoot, producerCommit, configPath, payload, validateConfig);
  return startWorkload(validated);
}

export function encodeDispatchRequest(candidateCommit: string): Buffer {
  return Buffer.from(JSON.stringify({ version: 1, lane: DISPATCH_LANE, candidateCommit }));
}
