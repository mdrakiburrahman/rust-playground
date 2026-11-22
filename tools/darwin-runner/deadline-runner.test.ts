import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { runDeadlinedCommand } from "./deadline-runner.js";
import { loadOrCreateDeadline } from "./deadline-state.js";

test("terminates a command before the fixed deadline and returns timeout status", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "darwin-deadline-runner-"));
  try {
    const runId = "run-000002";
    const repo = path.join(root, "repo");
    mkdirSync(repo);
    const deadlineUtc = new Date(Date.now() + 1_000).toISOString();
    const deadline = loadOrCreateDeadline({
      statePath: path.join(root, `${runId}.json`), repoRoot: repo,
      runId, deadlineUtc, cleanupReserveMs: 100,
    });
    const status = await runDeadlinedCommand({
      deadline, command: process.execPath, args: ["-e", "setTimeout(() => {}, 5000)"],
      cleanupReserveMs: 100,
    });
    assert.equal(status, 124);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
