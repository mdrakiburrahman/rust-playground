import { constants } from "node:fs";
import { lstat, mkdtemp, open, realpath } from "node:fs/promises";
import path from "node:path";
import type { CommitPhysicalSummary } from "./commit-component-adapter.js";
import { reconcileStoppedCommitTable } from "./commit-component-reconciler.js";

const MAX_STDOUT_BYTES = 256 * 1024 * 1024;
const MAX_STDERR_BYTES = 1024 * 1024;
const MEMORY_BYTES = 4 * 1024 * 1024 * 1024;
const TIMEOUT_MS = 240_000;

export interface CommitOracleRequest {
  oracleSha256: string;
  args: readonly [string, string, string, string];
  stdoutPath: string;
  timeoutMs: number;
  maxStdoutBytes: number;
  maxStderrBytes: number;
  memoryBytes: number;
}

export interface CommitOracleReceipt {
  oracleSha256: string;
  exitCode: number | null;
  outcome: "exited" | "timed_out" | "cancelled" | "resource_limit" | "launch_failed";
  settlement: "verified" | "unproved";
}

export interface CommitOracleHost {
  scratchRoot: string;
  oracleSha256: string;
  runAdmittedOracle(request: CommitOracleRequest): Promise<CommitOracleReceipt>;
}

async function readBoundedSnapshot(file: string): Promise<unknown> {
  const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = await handle.stat();
    if (!before.isFile() || before.nlink !== 1 || before.size === 0 || before.size > MAX_STDOUT_BYTES) {
      throw new Error("oracle output must be a bounded regular file");
    }
    const bytes = Buffer.alloc(before.size + 1);
    let length = 0;
    while (length < bytes.length) {
      const { bytesRead } = await handle.read(bytes, length, bytes.length - length, length);
      if (bytesRead === 0) break;
      length += bytesRead;
    }
    const after = await handle.stat();
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || length !== before.size) {
      throw new Error("oracle output changed during reading");
    }
    try {
      return JSON.parse(bytes.toString("utf8", 0, length)) as unknown;
    } catch {
      throw new Error("oracle output is not valid JSON");
    }
  } finally {
    await handle.close();
  }
}

/**
 * The trusted host must independently admit the oracle and enforce every
 * requested bound and whole-tree settlement outside candidate control.
 */
export async function reconcileStoppedCommitWithOracle(
  host: CommitOracleHost,
  tablePath: string,
): Promise<CommitPhysicalSummary> {
  if (!/^[a-f0-9]{64}$/.test(host.oracleSha256)) throw new Error("invalid admitted oracle pin");
  if (!path.isAbsolute(tablePath) || path.resolve(tablePath) !== tablePath
    || path.basename(tablePath) !== "table" || await realpath(tablePath) !== tablePath) {
    throw new Error("table path must be canonical and named table");
  }
  if (!path.isAbsolute(host.scratchRoot) || path.resolve(host.scratchRoot) !== host.scratchRoot
    || await realpath(host.scratchRoot) !== host.scratchRoot) {
    throw new Error("oracle scratch root must be canonical");
  }
  const stat = await lstat(host.scratchRoot);
  const uid = process.getuid?.();
  if (!stat.isDirectory() || stat.isSymbolicLink() || uid === undefined
    || (stat.uid !== uid && stat.uid !== 0) || (stat.mode & 0o022) !== 0) {
    throw new Error("oracle scratch root must be owned and not group- or world-writable");
  }
  const directory = await mkdtemp(path.join(host.scratchRoot, "commit-oracle-"));
  const stdoutPath = path.join(directory, "snapshot.json");
  const receipt = await host.runAdmittedOracle({
    oracleSha256: host.oracleSha256,
    args: ["summarize", path.dirname(tablePath), "--table", "table"],
    stdoutPath,
    timeoutMs: TIMEOUT_MS,
    maxStdoutBytes: MAX_STDOUT_BYTES,
    maxStderrBytes: MAX_STDERR_BYTES,
    memoryBytes: MEMORY_BYTES,
  });
  if (receipt.oracleSha256 !== host.oracleSha256 || receipt.outcome !== "exited"
    || receipt.exitCode !== 0 || receipt.settlement !== "verified") {
    throw new Error("admitted oracle did not exit zero and settle with its admitted pin");
  }
  return reconcileStoppedCommitTable(tablePath, await readBoundedSnapshot(stdoutPath));
}
