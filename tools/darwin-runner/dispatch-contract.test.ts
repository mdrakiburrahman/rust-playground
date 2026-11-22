import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { dispatchRequest, encodeDispatchRequest, type ConfigValidator, type ValidatedDispatch } from "./dispatch-contract.js";

const CONFIG_PATH = "fixtures/compression.json";

function git(root: string, ...args: string[]): string {
  return execFileSync("git", ["-C", root, ...args], { encoding: "utf8" }).trim();
}

function commit(root: string, message: string): string {
  git(root, "add", "--all");
  git(root, "commit", "-qm", message);
  return git(root, "rev-parse", "HEAD");
}

function createFixture(): { root: string; base: string; level3: string; level7: string; extra: string; invalid: string; large: string } {
  const root = mkdtempSync(path.join(tmpdir(), "darwin-dispatch-contract-"));
  git(root, "init", "-q");
  git(root, "config", "user.name", "Darwin contract test");
  git(root, "config", "user.email", "darwin-contract@example.invalid");
  mkdirSync(path.join(root, "fixtures"), { recursive: true });
  writeFileSync(path.join(root, CONFIG_PATH), '{"compression_level":1,"other":"fixed"}\n');
  writeFileSync(path.join(root, "README.md"), "trusted fixture\n");
  const base = commit(root, "pinned producer");

  writeFileSync(path.join(root, CONFIG_PATH), '{"compression_level":3,"other":"fixed"}\n');
  const level3 = commit(root, "level three");
  git(root, "checkout", "-q", "--detach", base);
  writeFileSync(path.join(root, CONFIG_PATH), '{"compression_level":7,"other":"fixed"}\n');
  const level7 = commit(root, "level seven");
  git(root, "checkout", "-q", "--detach", base);
  writeFileSync(path.join(root, CONFIG_PATH), '{"compression_level":5,"other":"fixed"}\n');
  writeFileSync(path.join(root, "README.md"), "unapproved change\n");
  const extra = commit(root, "protected file change");
  git(root, "checkout", "-q", "--detach", base);
  writeFileSync(path.join(root, CONFIG_PATH), '{"compression_level":0,"other":"fixed"}\n');
  const invalid = commit(root, "invalid level");
  git(root, "checkout", "-q", "--detach", base);
  writeFileSync(path.join(root, CONFIG_PATH), `${JSON.stringify({ compression_level: 5, other: "x".repeat(20_000) })}\n`);
  const large = commit(root, "oversized config");
  return { root, base, level3, level7, extra, invalid, large };
}

const validateConfig: ConfigValidator = (baseline, candidate) => {
  const before = JSON.parse(baseline.toString("utf8")) as Record<string, unknown>;
  const after = JSON.parse(candidate.toString("utf8")) as Record<string, unknown>;
  if (Object.keys(before).sort().join() !== Object.keys(after).sort().join()
    || before.other !== after.other || before.compression_level === after.compression_level
    || !Number.isInteger(after.compression_level)
    || (after.compression_level as number) < 1 || (after.compression_level as number) > 22) {
    throw new Error("candidate config is outside the inert compression-only fixture policy");
  }
  return after.compression_level as number;
};

test("dispatches two committed refs with their distinct validated levels", async () => {
  const fixture = createFixture();
  try {
    const started: ValidatedDispatch[] = [];
    for (const [commitSha, expected] of [[fixture.level3, 3], [fixture.level7, 7]] as const) {
      const result = await dispatchRequest(
        fixture.root, fixture.base, CONFIG_PATH, encodeDispatchRequest(commitSha), validateConfig,
        (request) => { started.push(request); return request.compressionLevel; },
      );
      assert.equal(result, expected);
    }
    assert.deepEqual(started.map((entry) => entry.compressionLevel), [3, 7]);
    assert.deepEqual(started.map((entry) => entry.candidateCommit), [fixture.level3, fixture.level7]);
  } finally { rmSync(fixture.root, { recursive: true, force: true }); }
});

test("rejects protected changes, invalid levels and oversized configs before workload start", async () => {
  const fixture = createFixture();
  try {
    let starts = 0;
    const start = () => { starts++; };
    await assert.rejects(dispatchRequest(
      fixture.root, fixture.base, CONFIG_PATH, encodeDispatchRequest(fixture.extra), validateConfig, start,
    ), /may change only/);
    await assert.rejects(dispatchRequest(
      fixture.root, fixture.base, CONFIG_PATH, encodeDispatchRequest(fixture.invalid), validateConfig, start,
    ), /outside the inert compression-only/);
    await assert.rejects(dispatchRequest(
      fixture.root, fixture.base, CONFIG_PATH, encodeDispatchRequest(fixture.large), validateConfig, start,
    ), /exceeds the size limit/);
    assert.equal(starts, 0);
  } finally { rmSync(fixture.root, { recursive: true, force: true }); }
});

test("rejects malformed, widened, unsupported and non-descendant requests before workload start", async () => {
  const fixture = createFixture();
  try {
    let starts = 0;
    const start = () => { starts++; };
    const valid = JSON.parse(encodeDispatchRequest(fixture.level3).toString("utf8")) as Record<string, unknown>;
    for (const request of [
      { ...valid, extra: "ignored fields are forbidden" },
      { ...valid, lane: "throughput-config" },
      { ...valid, candidateCommit: "deadbeef" },
      { ...valid, candidateCommit: fixture.base },
    ]) {
      await assert.rejects(dispatchRequest(
        fixture.root, fixture.base, CONFIG_PATH, Buffer.from(JSON.stringify(request)), validateConfig, start,
      ));
    }
    await assert.rejects(dispatchRequest(
      fixture.root, fixture.base, "../README.md", encodeDispatchRequest(fixture.level3), validateConfig, start,
    ), /normalized repository-relative/);
    assert.equal(starts, 0);
  } finally { rmSync(fixture.root, { recursive: true, force: true }); }
});
