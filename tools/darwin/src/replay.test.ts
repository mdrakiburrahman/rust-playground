import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { assertMatchingReplayInputs, cleanEnv, hasPinnedSubmodules, localReplayOptions } from "./replay.js";

test("rejects paired results with different business corpora or binaries", () => {
  const baseline = {
    binaryHash: "controller",
    images: ["client:image", "collector:image"],
    clientEvidence: {
      profile: "realistic" as const,
      identityEligibleForReconciliation: true,
      payloadCorpusSha256: "a".repeat(64),
      cpu: { coreEquivalents: null, sampleStartUnixMs: null, sampleEndUnixMs: null, unavailableReason: "unavailable" },
    },
  };
  assert.doesNotThrow(() => assertMatchingReplayInputs(baseline, structuredClone(baseline)));
  assert.throws(() => assertMatchingReplayInputs(baseline, {
    ...baseline, clientEvidence: { ...baseline.clientEvidence, payloadCorpusSha256: "b".repeat(64) },
  }), /business corpus/);
  assert.throws(() => assertMatchingReplayInputs(baseline, { ...baseline, binaryHash: "other" }), /binaries/);
  assert.throws(() => assertMatchingReplayInputs(baseline, { ...baseline, images: ["client:other"] }), /binaries/);
});

test("keeps the frozen collector hard cap when wiring replay endpoints", () => {
  const local = localReplayOptions({ collector_memory_limit_bytes: 4294967296 }, "127.0.0.1", "127.0.0.1", "/snapshot");
  assert.equal(local.collector_memory_limit_bytes, 4294967296);
  assert.throws(() => localReplayOptions({}, "127.0.0.1", "127.0.0.1", "/snapshot"), /memory limit/);
});

test("the isolated runtime can execute npx without inheriting credentials", async () => {
  const home = await mkdtemp(join(tmpdir(), "darwin-home-"));
  try {
    const env = cleanEnv(home, 33554432);
    assert.match(execFileSync("npx", ["--version"], { env, encoding: "utf8" }), /^\d+\./);
    assert.equal(env.COPILOT_GITHUB_TOKEN, undefined);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

test("accepts a pinned submodule and rejects an uninitialized one", async () => {
  const root = await mkdtemp(join(tmpdir(), "darwin-runtime-"));
  try {
    const source = join(root, "source");
    const repo = join(root, "repo");
    execFileSync("git", ["init", "-q", source]);
    await writeFile(join(source, "file"), "pinned");
    execFileSync("git", ["-C", source, "add", "file"]);
    execFileSync("git", ["-C", source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "pinned"]);
    execFileSync("git", ["init", "-q", repo]);
    execFileSync("git", ["-C", repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", source, "sub"]);
    assert.equal(hasPinnedSubmodules(repo, process.env), true);
    execFileSync("git", ["-C", repo, "submodule", "deinit", "-f", "sub"], { stdio: "ignore" });
    assert.equal(hasPinnedSubmodules(repo, process.env), false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
