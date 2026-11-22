import { randomUUID } from "node:crypto";
import { closeSync, existsSync, fsyncSync, linkSync, lstatSync, mkdirSync, openSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";

export interface DeadlineRecord {
  version: 1;
  runId: string;
  deadlineUtc: string;
}

export const DEFAULT_CLEANUP_RESERVE_MS = 30_000;

function canonicalUtc(value: string): number {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) {
    throw new Error("deadline must be an RFC 3339 UTC timestamp with milliseconds");
  }
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed) || new Date(parsed).toISOString() !== value) {
    throw new Error("deadline timestamp is invalid");
  }
  return parsed;
}

function isInside(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
}

function resolveStatePath(statePath: string, repoRoot: string, runId: string): string {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{7,79}$/.test(runId)) throw new Error("run id is invalid");
  const absolute = path.resolve(statePath);
  if (path.basename(absolute) !== `${runId}.json`) throw new Error("deadline state path must be named for the run id");
  const repo = realpathSync(repoRoot);
  if (isInside(repo, absolute)) throw new Error("deadline state must be outside the repository");
  mkdirSync(path.dirname(absolute), { recursive: true, mode: 0o700 });
  const realDirectory = realpathSync(path.dirname(absolute));
  const resolved = path.join(realDirectory, path.basename(absolute));
  if (isInside(repo, resolved)) throw new Error("deadline state must resolve outside the repository");
  return resolved;
}

function readRecord(statePath: string): DeadlineRecord {
  const info = lstatSync(statePath);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) {
    throw new Error("deadline state must be a private regular file");
  }
  const parsed: unknown = JSON.parse(readFileSync(statePath, "utf8"));
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("deadline state is malformed");
  }
  const record = parsed as Record<string, unknown>;
  if (Object.keys(record).sort().join(",") !== "deadlineUtc,runId,version"
    || record.version !== 1 || typeof record.runId !== "string" || typeof record.deadlineUtc !== "string") {
    throw new Error("deadline state is malformed");
  }
  canonicalUtc(record.deadlineUtc);
  return { version: 1, runId: record.runId, deadlineUtc: record.deadlineUtc };
}

export function loadOrCreateDeadline(options: {
  statePath: string;
  repoRoot: string;
  runId: string;
  deadlineUtc: string;
  now?: () => number;
  cleanupReserveMs?: number;
}): DeadlineRecord {
  const { statePath, repoRoot, runId, deadlineUtc } = options;
  const now = options.now ?? Date.now;
  const cleanupReserveMs = options.cleanupReserveMs ?? DEFAULT_CLEANUP_RESERVE_MS;
  const deadlineMs = canonicalUtc(deadlineUtc);
  if (!Number.isSafeInteger(cleanupReserveMs) || cleanupReserveMs < 0) throw new Error("cleanup reserve is invalid");
  const resolvedStatePath = resolveStatePath(statePath, repoRoot, runId);

  const verifyLive = (record: DeadlineRecord): DeadlineRecord => {
    if (record.runId !== runId) throw new Error("deadline state belongs to another run");
    if (record.deadlineUtc !== deadlineUtc) throw new Error("deadline is immutable and cannot be reset on resume");
    remainingBudgetMs(record, now(), cleanupReserveMs);
    return record;
  };

  if (existsSync(resolvedStatePath)) return verifyLive(readRecord(resolvedStatePath));

  const record: DeadlineRecord = { version: 1, runId, deadlineUtc };
  remainingBudgetMs(record, now(), cleanupReserveMs);
  const temporary = `${resolvedStatePath}.${process.pid}.${randomUUID()}.tmp`;
  let descriptor: number | undefined;
  try {
    descriptor = openSync(temporary, "wx", 0o600);
    writeFileSync(descriptor, `${JSON.stringify(record)}\n`, "utf8");
    fsyncSync(descriptor);
    closeSync(descriptor);
    descriptor = undefined;
    try { linkSync(temporary, resolvedStatePath); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
    if (existsSync(temporary)) rmSync(temporary);
  }
  return verifyLive(readRecord(resolvedStatePath));
}

export function remainingBudgetMs(
  deadline: DeadlineRecord | string,
  nowMs = Date.now(),
  cleanupReserveMs = DEFAULT_CLEANUP_RESERVE_MS,
): number {
  const deadlineUtc = typeof deadline === "string" ? deadline : deadline.deadlineUtc;
  const available = canonicalUtc(deadlineUtc) - nowMs - cleanupReserveMs;
  if (available <= 0) throw new Error("deadline is expired or infeasible after cleanup reserve");
  return available;
}
