import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, symlink, truncate, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import {
  reconcileStoppedCommitWithOracle,
  type CommitOracleHost,
  type CommitOracleReceipt,
  type CommitOracleRequest,
} from "./commit-component-oracle.js";

const hash = "a".repeat(64);
const success: CommitOracleReceipt = {
  oracleSha256: hash,
  exitCode: 0,
  outcome: "exited",
  settlement: "verified",
};

async function setup() {
  const root = await mkdtemp(path.join(tmpdir(), "commit-oracle-test-"));
  const table = path.join(root, "cell", "table");
  await mkdir(table, { recursive: true });
  return { root, table };
}

function host(root: string, runAdmittedOracle: CommitOracleHost["runAdmittedOracle"]): CommitOracleHost {
  return { scratchRoot: root, oracleSha256: hash, runAdmittedOracle };
}

test("passes fixed bounded oracle invocation and reconciles only after verified settlement", async (t) => {
  const { root, table } = await setup();
  t.after(() => rm(root, { recursive: true, force: true }));
  let request: CommitOracleRequest | undefined;
  await assert.rejects(reconcileStoppedCommitWithOracle(host(root, async (value) => {
    request = value;
    await writeFile(value.stdoutPath, JSON.stringify({ tables: {} }), { flag: "wx" });
    return success;
  }), table), /wrong oracle table set/);
  assert.ok(request);
  assert.deepEqual(request.args, ["summarize", path.dirname(table), "--table", "table"]);
  assert.equal(request.oracleSha256, hash);
  assert.equal(request.timeoutMs, 240_000);
  assert.equal(request.maxStdoutBytes, 256 * 1024 * 1024);
  assert.equal(request.maxStderrBytes, 1024 * 1024);
  assert.equal(request.memoryBytes, 4 * 1024 * 1024 * 1024);
  assert.notEqual(path.dirname(request.stdoutPath), path.dirname(table));
});

test("rejects unsuccessful or unproved oracle executions without reading output", async (t) => {
  const { root, table } = await setup();
  t.after(() => rm(root, { recursive: true, force: true }));
  const failures: CommitOracleReceipt[] = [
    { ...success, oracleSha256: "b".repeat(64) },
    { ...success, exitCode: 1 },
    { ...success, outcome: "timed_out", exitCode: null },
    { ...success, outcome: "cancelled", exitCode: null },
    { ...success, outcome: "resource_limit", exitCode: null },
    { ...success, outcome: "launch_failed", exitCode: null },
    { ...success, settlement: "unproved" },
  ];
  for (const receipt of failures) {
    await assert.rejects(reconcileStoppedCommitWithOracle(host(root, async () => receipt), table),
      /did not exit zero and settle/);
  }
});

test("rejects missing, symlinked, malformed and oversized oracle output", async (t) => {
  const { root, table } = await setup();
  t.after(() => rm(root, { recursive: true, force: true }));
  await assert.rejects(reconcileStoppedCommitWithOracle(host(root, async () => success), table), /ENOENT/);
  await assert.rejects(reconcileStoppedCommitWithOracle(host(root, async ({ stdoutPath }) => {
    await symlink(table, stdoutPath);
    return success;
  }), table), /ELOOP/);
  await assert.rejects(reconcileStoppedCommitWithOracle(host(root, async ({ stdoutPath }) => {
    await writeFile(stdoutPath, "{", { flag: "wx" });
    return success;
  }), table), /not valid JSON/);
  await assert.rejects(reconcileStoppedCommitWithOracle(host(root, async ({ stdoutPath }) => {
    await writeFile(stdoutPath, "", { flag: "wx" });
    await truncate(stdoutPath, 256 * 1024 * 1024 + 1);
    return success;
  }), table), /bounded regular file/);
});
