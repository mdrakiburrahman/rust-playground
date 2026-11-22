import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { loadOrCreateDeadline, remainingBudgetMs } from "./deadline-state.js";

const runId = "run-000001";
const deadlineUtc = "2030-01-01T00:00:00.000Z";
const nowMs = Date.parse("2029-12-31T23:00:00.000Z");

function layout(run: (repo: string, state: string, root: string) => void): void {
  const root = mkdtempSync(path.join(tmpdir(), "darwin-deadline-test-"));
  const repo = path.join(root, "repo");
  const stateDir = path.join(root, "state");
  mkdirSync(repo);
  mkdirSync(stateDir);
  try { run(repo, path.join(stateDir, `${runId}.json`), root); }
  finally { rmSync(root, { recursive: true, force: true }); }
}

test("persists a private immutable deadline outside the repository", () => {
  layout((repo, state) => {
    const options = { statePath: state, repoRoot: repo, runId, deadlineUtc, now: () => nowMs };
    const first = loadOrCreateDeadline(options);
    const resumed = loadOrCreateDeadline(options);
    assert.deepEqual(resumed, first);
    assert.equal(readFileSync(state, "utf8"), '{"version":1,"runId":"run-000001","deadlineUtc":"2030-01-01T00:00:00.000Z"}\n');
  });
});

test("refuses a changed or expired deadline without rewriting state", () => {
  layout((repo, state) => {
    const options = { statePath: state, repoRoot: repo, runId, deadlineUtc, now: () => nowMs };
    loadOrCreateDeadline(options);
    const original = readFileSync(state, "utf8");
    assert.throws(() => loadOrCreateDeadline({ ...options, deadlineUtc: "2030-01-02T00:00:00.000Z" }), /cannot be reset/);
    assert.throws(() => loadOrCreateDeadline({ ...options, now: () => Date.parse(deadlineUtc) }), /expired or infeasible/);
    assert.equal(readFileSync(state, "utf8"), original);
  });
});

test("reserves bounded cleanup time from every stage budget", () => {
  assert.equal(remainingBudgetMs(deadlineUtc, nowMs, 30_000), 3_570_000);
  assert.throws(() => remainingBudgetMs(deadlineUtc, Date.parse(deadlineUtc) - 30_000, 30_000), /expired or infeasible/);
});

test("rejects repository-local and symlinked state locations", () => {
  layout((repo, state, root) => {
    assert.throws(() => loadOrCreateDeadline({
      statePath: path.join(repo, `${runId}.json`), repoRoot: repo, runId, deadlineUtc, now: () => nowMs,
    }), /outside the repository/);
    const linkPath = path.join(root, "inside-repo");
    symlinkSync(repo, linkPath, "dir");
    assert.throws(() => loadOrCreateDeadline({
      statePath: path.join(linkPath, `${runId}.json`), repoRoot: repo, runId, deadlineUtc, now: () => nowMs,
    }), /outside the repository/);
  });
});
